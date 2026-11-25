// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/broker/brokertest"
	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/adapters/buffer/sqlite"
)

// brokerDriver adapts the sqlite driver to the broker contract for the
// conformance suite: the queue is the destination of each message, so the
// configs carry no queue and no limits.
type brokerDriver struct{ d *sqlite.Driver }

// queueOfOpts takes the queue the conformance suite names only as the publish
// destination out of the options and returns it with the options cleaned.
func queueOfOpts(opts map[string]any) (string, map[string]any) {
	queue := ""
	if q, ok := opts["queue"].(string); ok {
		queue = q
		cleaned := make(map[string]any, len(opts)-1)
		for k, v := range opts {
			if k != "queue" {
				cleaned[k] = v
			}
		}
		return queue, cleaned
	}
	return queue, opts
}

func (a brokerDriver) NewPublisher(
	opts map[string]any,
	cfg broker.PublisherConfig,
	logger *slog.Logger,
) (broker.Publisher, error) {
	queue, opts := queueOfOpts(opts)
	return a.d.NewPublisher(opts, buffer.PublisherConfig{PublisherConfig: cfg, Queue: queue}, logger)
}

func (brokerDriver) CheckPublisher(map[string]any) error  { return nil }
func (brokerDriver) CheckSubscriber(map[string]any) error { return nil }

func (a brokerDriver) NewSubscriber(
	opts map[string]any,
	cfg broker.SubscriberConfig,
	logger *slog.Logger,
) (broker.Subscriber, error) {
	queue, opts := queueOfOpts(opts)
	return a.d.NewSubscriber(opts, buffer.SubscriberConfig{SubscriberConfig: cfg, Queue: queue}, logger)
}

// TestConformance runs the broker contract suite against the sqlite driver:
// a queue file is a broker whose messages survive closing.
func TestConformance(t *testing.T) {
	t.Parallel()
	brokertest.Run(t, brokertest.Harness{
		Driver: brokerDriver{sqlite.New(validator.New(), t.TempDir())},
		Setup: func(t *testing.T) brokertest.Setup {
			t.Helper()
			queue := "q-" + t.Name()[len("TestConformance/"):]
			return brokertest.Setup{
				Destination: queue,
				Publisher:   map[string]any{"queue": queue},
				Subscriber:  map[string]any{"queue": queue},
			}
		},
		InvalidDestinations: []string{"", "../escape", "a/b", "a b"},
	})
}

func TestNoDataDir(t *testing.T) {
	t.Parallel()
	drv := sqlite.New(validator.New(), "")
	_, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 1,
		Queue: "q",
	}, testLogger())
	require.ErrorIs(t, err, buffer.ErrNoDataDir)

	_, err = drv.NewSubscriber(map[string]any{}, buffer.SubscriberConfig{
		Name: "s", Capacity: 1,
		Queue: "q",
	}, testLogger())
	require.ErrorIs(t, err, buffer.ErrNoDataDir)

	files, err := drv.Files(t.Context())
	require.NoError(t, err)
	assert.Empty(t, files)
}

func TestQueueNameValidation(t *testing.T) {
	t.Parallel()
	drv := sqlite.New(validator.New(), t.TempDir())
	p, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 1,
		Queue: "ok-queue_1.db",
	}, testLogger())
	require.NoError(t, err)
	defer p.Close()

	require.NoError(t, p.CheckDestination("ok"))
	for _, bad := range []string{"", ".", "..", "../x", "a/b", "a b", ".hidden"} {
		assert.ErrorIs(t, p.CheckDestination(bad), broker.ErrInvalidDestination, "%q", bad)
	}
}

// A message above max_bytes is refused, and a queue at max_bytes refuses more
// with buffer.ErrFull, which sources retry.
func TestMaxBytes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	drv := sqlite.New(validator.New(), dir)
	p, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 4,
		Queue: "q", MaxBytes: 10,
	}, testLogger())
	require.NoError(t, err)
	defer p.Close()

	assert.Equal(t, 10, p.MaxMessageSize())
	_, err = p.Publish(t.Context(), broker.Message{Destination: "q", Body: make([]byte, 11), Signal: "logs"})
	require.ErrorIs(t, err, broker.ErrMessageTooLarge)

	ack, err := p.Publish(t.Context(), broker.Message{Destination: "q", Body: make([]byte, 6), Signal: "logs"})
	require.NoError(t, err)
	require.NoError(t, ack.Wait(t.Context()))

	_, err = p.Publish(t.Context(), broker.Message{Destination: "q", Body: make([]byte, 5), Signal: "logs"})
	require.ErrorIs(t, err, buffer.ErrFull)
}

// The queue file is shared within the process: a subscriber reads what a
// publisher of the same process wrote, and the file is closed once both are
// gone.
func TestPublisherAndSubscriberShareTheFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	drv := sqlite.New(validator.New(), dir)

	p, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 4,
		Queue: "q",
	}, testLogger())
	require.NoError(t, err)
	s, err := drv.NewSubscriber(map[string]any{}, buffer.SubscriberConfig{
		Name: "s", Capacity: 2,
		Queue: "q",
	}, testLogger())
	require.NoError(t, err)

	ack, err := p.Publish(t.Context(), broker.Message{Destination: "q", Body: []byte("hello"), Signal: "logs"})
	require.NoError(t, err)
	require.NoError(t, ack.Wait(t.Context()))

	require.NoError(t, s.Open(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d, err := s.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(d.Body()))
	assert.Equal(t, "q", d.Destination())
	assert.Equal(t, "logs", d.(broker.Signaled).Signal())
	assert.Zero(t, d.(broker.Counted).Attempts())
	require.NoError(t, d.Ack())

	// The file exists and is listed as in use.
	files, err := drv.Files(t.Context())
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, filepath.Join(dir, "q.db"), files[0].Path)
	assert.True(t, files[0].InUse)
	assert.Zero(t, files[0].Info.Entries, "the acknowledged entry is gone")

	require.NoError(t, s.Close())
	require.NoError(t, p.Close())
	files, err = drv.Files(t.Context())
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.False(t, files[0].InUse, "the file is closed once its last user is gone")
}

// lockChildEnv marks the child process of TestFileLockedAcrossProcesses.
const lockChildEnv = "TRAM_SQLITE_LOCK_CHILD"

// TestFileLockedChild is not a test: run as a child process, it tries to open
// a queue file another process holds and exits non-zero unless it is refused
// with buffer.ErrInUse.
func TestFileLockedChild(t *testing.T) {
	t.Parallel()
	dir := os.Getenv(lockChildEnv)
	if dir == "" {
		t.Skip("only runs as the child of TestFileLockedAcrossProcesses")
	}
	other := sqlite.New(validator.New(), dir)
	_, err := other.NewSubscriber(map[string]any{}, buffer.SubscriberConfig{
		Name: "s", Capacity: 1,
		Queue: "q",
	}, testLogger())
	if !errors.Is(err, buffer.ErrInUse) {
		fmt.Fprintf(os.Stderr, "expected buffer.ErrInUse, got %v\n", err)
		os.Exit(1)
	}
}

// A second process cannot use the file while the first holds it. Within one
// process the queue is shared instead, so this runs a child process.
func TestFileLockedAcrossProcesses(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	drv := sqlite.New(validator.New(), dir)
	p, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 1,
		Queue: "q",
	}, testLogger())
	require.NoError(t, err)
	defer p.Close()

	cmd := exec.Command(os.Args[0], "-test.run", "^TestFileLockedChild$", "-test.v")
	cmd.Env = append(os.Environ(), lockChildEnv+"="+dir)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child output: %s", out)
}

// Entries survive closing and opening again: a restart redelivers what was
// not acknowledged.
func TestEntriesSurviveRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	drv := sqlite.New(validator.New(), dir)

	p, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 4,
		Queue: "q", MaxAge: time.Hour,
	}, testLogger())
	require.NoError(t, err)
	ack, err := p.Publish(t.Context(), broker.Message{Destination: "q", Body: []byte("kept"), Signal: "traces"})
	require.NoError(t, err)
	require.NoError(t, ack.Wait(t.Context()))
	require.NoError(t, p.Close())

	files, err := drv.Files(t.Context())
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, 1, files[0].Info.Entries)
	assert.Equal(t, time.Hour, files[0].Info.MaxAge)

	s, err := drv.NewSubscriber(map[string]any{}, buffer.SubscriberConfig{
		Name: "s", Capacity: 1,
		Queue: "q",
	}, testLogger())
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Open(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d, err := s.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, "kept", string(d.Body()))
	assert.Equal(t, "traces", d.(broker.Signaled).Signal())
	require.NoError(t, d.Ack())
}

func TestPendingFilesHelperIsInDriversPackage(t *testing.T) {
	t.Parallel()
	// Compile-time check that the drivers package lists files.
	files, err := sqlite.New(validator.New(), t.TempDir()).Files(t.Context())
	require.NoError(t, err)
	assert.Empty(t, files)
}

// Publishers and subscribers of one queue that open and close it at the same
// time share it: none fails on the file lock its own process holds, and once
// the last is closed, the file can be opened again.
func TestConcurrentOpenAndCloseShareTheQueue(t *testing.T) {
	t.Parallel()
	drv := sqlite.New(validator.New(), t.TempDir())

	const rounds, workers = 20, 8
	errs := make(chan error, rounds*workers)
	for range rounds {
		done := make(chan struct{})
		for i := range workers {
			go func() {
				defer func() { done <- struct{}{} }()
				var closer interface{ Close() error }
				var err error
				if i%2 == 0 {
					closer, err = drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
						Name: "p", MaxPending: 1, Queue: "shared",
					}, testLogger())
				} else {
					closer, err = drv.NewSubscriber(map[string]any{}, buffer.SubscriberConfig{
						Name: "s", Capacity: 1, Queue: "shared",
					}, testLogger())
				}
				if err != nil {
					errs <- err
					return
				}
				errs <- closer.Close()
			}()
		}
		for range workers {
			<-done
		}
	}
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	p, err := drv.NewPublisher(map[string]any{}, buffer.PublisherConfig{
		Name: "p", MaxPending: 1, Queue: "shared",
	}, testLogger())
	require.NoError(t, err, "the queue can be opened again after its last user closed it")
	require.NoError(t, p.Close())
}
