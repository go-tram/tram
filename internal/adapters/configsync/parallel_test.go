// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package configsync_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/configsync"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

// slowCore takes buildTime for every upsert and delete, like streams whose
// sinks connect to a broker, and records how many ran at once.
type slowCore struct {
	*core.Core

	buildTime time.Duration
	mu        sync.Mutex
	inFlight  int
	most      int
}

func (c *slowCore) enter() func() {
	c.mu.Lock()
	c.inFlight++
	c.most = max(c.most, c.inFlight)
	c.mu.Unlock()
	time.Sleep(c.buildTime)
	return func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}
}

func (c *slowCore) UpsertStream(ctx context.Context, spec domain.StreamSpec) (*domain.StreamSpec, error) {
	defer c.enter()()
	return c.Core.UpsertStream(ctx, spec)
}

func (c *slowCore) DeleteStream(ctx context.Context, name string) error {
	defer c.enter()()
	return c.Core.DeleteStream(ctx, name)
}

func (c *slowCore) mostAtOnce() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.most
}

// The streams of a directory are applied in parallel, but not more than a
// few at once, so an instance with many streams starts quickly without
// opening all their connections at the same moment.
func TestSyncAppliesAndDeletesStreamsInParallel(t *testing.T) {
	t.Parallel()

	const streams = 32
	const buildTime = 50 * time.Millisecond
	dir := t.TempDir()
	for i := range streams {
		writeFile(t, dir, fmt.Sprintf("s%d.json", i), streamFile)
	}

	c := &slowCore{Core: newCore(t), buildTime: buildTime}
	start := time.Now()
	cs, err := configsync.New(c, dir, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	loaded := time.Since(start)

	running, err := c.GetStreams(t.Context())
	require.NoError(t, err)
	assert.Len(t, running, streams)
	assert.Greater(t, c.mostAtOnce(), 1, "streams were applied one by one")
	assert.LessOrEqual(t, c.mostAtOnce(), 8, "more streams applied at once than the sync allows")
	assert.Less(t, loaded, streams*buildTime/2, "loading took about as long as one stream after the other")

	// Removing the files deletes the streams, in parallel too.
	run(t, cs)
	require.Eventually(t, func() bool { return configsync.Running(cs) }, 2*time.Second, time.Millisecond)
	for i := range streams {
		require.NoError(t, os.Remove(filepath.Join(dir, fmt.Sprintf("s%d.json", i))))
	}
	require.Eventually(t, func() bool {
		left, getErr := c.GetStreams(t.Context())
		return getErr == nil && len(left) == 0
	}, 5*time.Second, 10*time.Millisecond, "the streams of removed files were not deleted")
}
