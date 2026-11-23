// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package configsync_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/configsync"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

// upsertingCore creates a stream through the API, as it were, while a sync
// lists the running streams: at the moment the stream is running, but its
// file is not written yet.
type upsertingCore struct {
	*core.Core

	t     *testing.T
	armed atomic.Bool
}

func (c *upsertingCore) GetStreams(ctx context.Context) ([]domain.StreamSpec, error) {
	if c.armed.CompareAndSwap(true, false) {
		_, err := c.UpsertStream(ctx, domain.StreamSpec{
			Name:   "created",
			Source: domain.SourceSpec{Name: "generator", Type: "generator", Spec: map[string]any{"interval": "1h"}},
			Sinks:  []domain.SinkSpec{{Name: "debug", Type: "debug", From: "generator"}},
		})
		require.NoError(c.t, err)
	}
	return c.Core.GetStreams(ctx)
}

// A stream created while the directory is synced is kept, and gets its file:
// the sync must not take it for a stream whose file was deleted.
func TestSyncKeepsStreamCreatedMeanwhile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := &upsertingCore{Core: newCore(t), t: t}
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	run(t, cs)
	// Syncs run once Run receives the changes of the core, as after a change
	// of a file or a retry.
	require.Eventually(t, func() bool { return configsync.Running(cs) }, 2*time.Second, time.Millisecond)

	c.armed.Store(true)
	_, err = configsync.Sync(t.Context(), cs)
	require.NoError(t, err)

	_, err = c.GetStream(t.Context(), "created")
	require.NoError(t, err, "the stream created during the sync was deleted")
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, statErr := os.Stat(filepath.Join(dir, "created.json"))
		assert.NoError(collect, statErr)
	}, 2*time.Second, 10*time.Millisecond)
}
