// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package configsync_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/configsync"
	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

const editedFile = "edited.json"

const streamFile = `{
  "stream": {
    "source": {"name": "generator", "type": "generator", "spec": {"interval": "1h"}},
    "sinks": [{"name": "debug", "type": "debug", "from": "generator"}]
  }
}`

func newCore(t *testing.T) *core.Core {
	t.Helper()

	logger := slog.New(slog.DiscardHandler)
	c := core.New(t.Context(), logger, source.NewFactory(logger), stage.NewFactory(logger), sink.NewFactory(logger))
	t.Cleanup(func() { assert.NoError(t, c.Close(context.Background())) })
	return c
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
}

func run(t *testing.T, cs *configsync.ConfigSync) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- cs.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
		assert.NoError(t, cs.Close())
	})
}

func TestStreamNameCannotEscapeDirectory(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dir := filepath.Join(parent, "config")
	require.NoError(t, os.Mkdir(dir, 0o700))

	c := newCore(t)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	run(t, cs)

	_, err = c.UpsertStream(t.Context(), domain.StreamSpec{
		Name:   "../escape",
		Source: domain.SourceSpec{Name: "generator", Type: "generator", Spec: map[string]any{"interval": "1h"}},
		Sinks:  []domain.SinkSpec{{Name: "debug", Type: "debug", From: "generator"}},
	})
	require.ErrorIs(t, err, domain.ErrInvalidStreamName)

	_, err = os.Stat(filepath.Join(parent, "escape.json"))
	assert.ErrorIs(t, err, os.ErrNotExist, "file written outside the config directory")
}

func TestOtherFilesDoNotCancelSync(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, editedFile, streamFile)

	c := newCore(t)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	run(t, cs)

	writeFile(t, dir, editedFile, strings.Replace(streamFile, "1h", "2h", 1))
	writeFile(t, dir, ".edited.json.swp", "swap")

	require.Eventually(t, func() bool {
		stream, getErr := c.GetStream(t.Context(), "edited")
		return getErr == nil && stream.Source.Spec["interval"] == "2h"
	}, 5*time.Second, 10*time.Millisecond, "the change to the stream file was not applied")
}

func TestUnreadableFileKeepsStream(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, editedFile, streamFile)
	writeFile(t, dir, "removed.json", streamFile)

	c := newCore(t)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	run(t, cs)

	_, err = c.GetStream(t.Context(), "edited")
	require.NoError(t, err)

	// A half-saved file, as an editor can leave it, is broken before the
	// other file is removed, so the sync that deletes the removed stream sees
	// it.
	const broken = `{"stream": {"source": `
	writeFile(t, dir, editedFile, broken)
	require.NoError(t, os.Remove(filepath.Join(dir, "removed.json")))

	require.Eventually(t, func() bool {
		_, getErr := c.GetStream(t.Context(), "removed")
		return getErr != nil
	}, 5*time.Second, 10*time.Millisecond)

	_, err = c.GetStream(t.Context(), "edited")
	require.NoError(t, err, "stream with an unreadable file was deleted")

	content, err := os.ReadFile(filepath.Join(dir, editedFile))
	require.NoError(t, err, "unreadable stream file was removed")
	assert.Equal(t, broken, string(content)) //nolint:testifylint // Invalid JSON on purpose; compared byte for byte.

	writeFile(t, dir, editedFile, strings.Replace(streamFile, "1h", "2h", 1))
	require.Eventually(t, func() bool {
		stream, getErr := c.GetStream(t.Context(), "edited")
		return getErr == nil && stream.Source.Spec["interval"] == "2h"
	}, 5*time.Second, 10*time.Millisecond)

	_, err = c.GetStream(t.Context(), "removed")
	assert.ErrorIs(t, err, domain.ErrStreamNotFound)
}

// failingCore fails every upsert while fail is set, like a core whose
// components cannot connect to NATS yet, and counts the attempts.
type failingCore struct {
	*core.Core

	fail     atomic.Bool
	attempts atomic.Int64
}

func (c *failingCore) UpsertStream(ctx context.Context, stream domain.StreamSpec) (*domain.StreamSpec, error) {
	c.attempts.Add(1)
	if c.fail.Load() {
		return nil, errors.New("failed to connect to NATS: nats: no servers available for connection")
	}
	return c.Core.UpsertStream(ctx, stream)
}

func startFailing(t *testing.T, dir string) *failingCore {
	t.Helper()
	c := &failingCore{Core: newCore(t)}
	c.fail.Store(true)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	configsync.SetRetryDelays(cs, 10*time.Millisecond, 80*time.Millisecond)
	run(t, cs)
	return c
}

// A stream that cannot be applied when Tram starts, e.g. because NATS is not
// up yet, is applied once it can be, without its file changing.
func TestStreamThatFailedIsAppliedLater(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "late.json", streamFile)
	c := startFailing(t, dir)

	require.Eventually(t, func() bool { return c.attempts.Load() >= 3 }, 5*time.Second, 5*time.Millisecond,
		"the stream was not tried again")
	_, err := c.GetStream(t.Context(), "late")
	require.ErrorIs(t, err, domain.ErrStreamNotFound)

	c.fail.Store(false)
	require.Eventually(t, func() bool {
		_, getErr := c.GetStream(t.Context(), "late")
		return getErr == nil
	}, 5*time.Second, 5*time.Millisecond, "the stream was not applied")

	// Once every stream is applied, nothing is tried again.
	settled := c.attempts.Load()
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, settled, c.attempts.Load())
}

// The delay doubles with every failed sync up to its maximum, so a stream
// that keeps failing is not tried in a busy loop.
func TestRetriesBackOff(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "failing.json", streamFile)
	c := startFailing(t, dir)

	// 10, 20, 40, then every 80 ms: about 8 attempts in 600 ms, while
	// retrying every 10 ms would make about 60.
	time.Sleep(600 * time.Millisecond)
	assert.GreaterOrEqual(t, c.attempts.Load(), int64(5))
	assert.LessOrEqual(t, c.attempts.Load(), int64(15))
}

// A stream whose file is removed is no longer tried.
func TestRemovedFileIsNotTriedAgain(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "gone.json", streamFile)
	c := startFailing(t, dir)

	require.Eventually(t, func() bool { return c.attempts.Load() >= 2 }, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, os.Remove(filepath.Join(dir, "gone.json")))

	// The sync of the removal may still run once.
	time.Sleep(300 * time.Millisecond)
	stopped := c.attempts.Load()
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, stopped, c.attempts.Load())
}

// Retries end with Run: none runs after it returned.
func TestRetriesStopWithRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "failing.json", streamFile)
	c := &failingCore{Core: newCore(t)}
	c.fail.Store(true)
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	configsync.SetRetryDelays(cs, 10*time.Millisecond, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- cs.Run(ctx) }()
	require.Eventually(t, func() bool { return c.attempts.Load() >= 3 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.NoError(t, cs.Close())

	// A retry that had already started may still finish.
	time.Sleep(50 * time.Millisecond)
	stopped := c.attempts.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, stopped, c.attempts.Load())
}
