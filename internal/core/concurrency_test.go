// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

// slowType is a source whose New waits until the test opens the gate, like
// one that connects to a broker that is slow to answer.
const slowType = "slow"

// gatedSources creates sources and counts, per stream, those created and not
// stopped yet. More than one per stream means one was orphaned: it runs but
// the core does not know it anymore.
type gatedSources struct {
	mu        sync.Mutex
	live      map[string]int
	sources   map[string]*gatedSource
	gate      chan struct{}
	stopDelay time.Duration
}

func newGatedSources() *gatedSources {
	return &gatedSources{live: map[string]int{}, sources: map[string]*gatedSource{}, gate: make(chan struct{})}
}

func (g *gatedSources) New(spec domain.SourceSpec, stream string) (domain.Source, error) {
	if spec.Type == slowType {
		<-g.gate
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.live[stream]++
	src := &gatedSource{
		g: g, name: spec.Name, stream: stream,
		shutdown: make(chan struct{}), crash: make(chan struct{}),
	}
	g.sources[stream] = src
	return src, nil
}

func (g *gatedSources) liveOf(stream string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.live[stream]
}

func (g *gatedSources) latest(stream string) *gatedSource {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sources[stream]
}

func (g *gatedSources) open() { close(g.gate) }

type gatedSource struct {
	g        *gatedSources
	name     string
	stream   string
	shutdown chan struct{}
	crash    chan struct{}
	once     sync.Once
	subs     []domain.Subscriber
}

func (s *gatedSource) Name() string { return s.name }
func (s *gatedSource) Subscribe(sub domain.Subscriber) error {
	s.subs = append(s.subs, sub)
	return nil
}
func (s *gatedSource) SetSubscribers(subs []domain.Subscriber) { s.subs = subs }
func (s *gatedSource) Run(ctx context.Context) error {
	select {
	case <-s.shutdown:
	case <-ctx.Done():
	case <-s.crash:
		return errors.New("source crashed")
	}
	return nil
}
func (s *gatedSource) Stop() error {
	s.once.Do(func() {
		time.Sleep(s.g.stopDelay)
		close(s.shutdown)
		s.g.mu.Lock()
		s.g.live[s.stream]--
		s.g.mu.Unlock()
	})
	return nil
}

func newGatedCore(t *testing.T) (*core.Core, *gatedSources) {
	t.Helper()
	sources := newGatedSources()
	reg := &registry{}
	c := core.New(t.Context(), slog.New(slog.DiscardHandler), sources, stageFactory{reg}, sinkFactory{reg})
	c.SetRestartDelays(time.Millisecond, 10*time.Millisecond)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c, sources
}

func gatedSpec(name, sourceType string, version int) domain.StreamSpec {
	return domain.StreamSpec{
		Name: name,
		Source: domain.SourceSpec{
			Name: "source", Type: sourceType, Spec: map[string]any{"version": float64(version)},
		},
		Sinks: []domain.SinkSpec{sink("sink", "fake")},
	}
}

// waitFor fails the test if fn does not return within a second.
func waitFor(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s is blocked", what)
	}
}

// A stream that takes long to build, e.g. while it connects to a broker, does
// not hold up reading, creating or deleting other streams.
func TestSlowBuildDoesNotBlockOtherStreams(t *testing.T) {
	t.Parallel()
	c, sources := newGatedCore(t)

	slow := make(chan error, 1)
	go func() {
		_, err := c.UpsertStream(context.Background(), gatedSpec("slow", slowType, 0))
		slow <- err
	}()

	waitFor(t, "upserting another stream", func() {
		_, err := c.UpsertStream(t.Context(), gatedSpec("fast", "fake", 0))
		require.NoError(t, err)
	})
	waitFor(t, "listing the streams", func() {
		streams, err := c.GetStreams(t.Context())
		require.NoError(t, err)
		assert.Len(t, streams, 1, "the slow stream is not built yet")
	})
	waitFor(t, "deleting another stream", func() {
		assert.NoError(t, c.DeleteStream(t.Context(), "fast"))
	})

	sources.open()
	require.NoError(t, <-slow)
	_, err := c.GetStream(t.Context(), "slow")
	assert.NoError(t, err)
}

// Concurrent upserts of one name, new or existing, leave exactly one
// running stream: none of them may orphan the stream of another.
func TestConcurrentUpsertsOfOneNameRunOneStream(t *testing.T) {
	t.Parallel()
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", existing), func(t *testing.T) {
			t.Parallel()
			c, sources := newGatedCore(t)
			if existing {
				_, err := c.UpsertStream(t.Context(), gatedSpec("one", "fake", -1))
				require.NoError(t, err)
			}

			var wg sync.WaitGroup
			for version := range 20 {
				wg.Go(func() {
					_, err := c.UpsertStream(t.Context(), gatedSpec("one", "fake", version))
					assert.NoError(t, err)
				})
			}
			wg.Wait()

			assert.Equal(t, 1, sources.liveOf("one"), "sources running for the stream")
			streams, err := c.GetStreams(t.Context())
			require.NoError(t, err)
			assert.Len(t, streams, 1)
		})
	}
}

// Upserts and deletes of one name racing each other leave either the stream
// with one source or no stream and no source.
func TestConcurrentUpsertAndDeleteLeaveNoOrphan(t *testing.T) {
	t.Parallel()
	c, sources := newGatedCore(t)

	var wg sync.WaitGroup
	for version := range 50 {
		wg.Go(func() {
			_, err := c.UpsertStream(t.Context(), gatedSpec("racy", "fake", version))
			assert.NoError(t, err)
		})
		wg.Go(func() {
			err := c.DeleteStream(t.Context(), "racy")
			if err != nil {
				assert.ErrorIs(t, err, domain.ErrStreamNotFound)
			}
		})
	}
	wg.Wait()

	_, err := c.GetStream(t.Context(), "racy")
	if err == nil {
		assert.Equal(t, 1, sources.liveOf("racy"))
	} else {
		require.ErrorIs(t, err, domain.ErrStreamNotFound)
		assert.Equal(t, 0, sources.liveOf("racy"))
	}
}

// A restart after a crashed source and an upsert of the same stream do not
// both build it.
func TestRestartAndUpsertLeaveOneStream(t *testing.T) {
	t.Parallel()
	c, sources := newGatedCore(t)
	_, err := c.UpsertStream(t.Context(), gatedSpec("crashy", "fake", 0))
	require.NoError(t, err)

	for version := 1; version <= 20; version++ {
		close(sources.latest("crashy").crash)
		_, err = c.UpsertStream(t.Context(), gatedSpec("crashy", "fake", version))
		require.NoError(t, err)
	}
	// Let restarts that were scheduled meanwhile run.
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 1, sources.liveOf("crashy"))
}

// Close stops the streams in parallel, so it takes about as long as the
// slowest one, not as long as all together.
func TestCloseStopsStreamsInParallel(t *testing.T) {
	t.Parallel()
	c, sources := newGatedCore(t)
	sources.stopDelay = 200 * time.Millisecond
	const streams = 10
	for i := range streams {
		_, err := c.UpsertStream(t.Context(), gatedSpec(fmt.Sprintf("s%d", i), "fake", 0))
		require.NoError(t, err)
	}

	start := time.Now()
	require.NoError(t, c.Close(t.Context()))
	assert.Less(t, time.Since(start), streams*sources.stopDelay/2)
	for i := range streams {
		assert.Equal(t, 0, sources.liveOf(fmt.Sprintf("s%d", i)))
	}
}

// Close waits for an upsert in progress; the stream it was building is
// stopped, not left running.
func TestCloseDuringUpsertStopsEverything(t *testing.T) {
	t.Parallel()
	c, sources := newGatedCore(t)

	upserted := make(chan error, 1)
	go func() {
		_, err := c.UpsertStream(context.Background(), gatedSpec("late", slowType, 0))
		upserted <- err
	}()
	// Let the upsert start building.
	time.Sleep(20 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- c.Close(context.Background()) }()
	sources.open()

	require.NoError(t, <-closed)
	require.ErrorIs(t, <-upserted, domain.ErrCoreClosed)
	assert.Equal(t, 0, sources.liveOf("late"))

	_, err := c.UpsertStream(t.Context(), gatedSpec("after", "fake", 0))
	assert.ErrorIs(t, err, domain.ErrCoreClosed)
}

// Close gives up waiting when its context ends, e.g. at the end of the
// termination grace period, instead of blocking the shutdown.
func TestCloseReturnsWhenItsContextEnds(t *testing.T) {
	t.Parallel()
	c, sources := newGatedCore(t)
	sources.stopDelay = time.Second
	_, err := c.UpsertStream(t.Context(), gatedSpec("stuck", "fake", 0))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.ErrorIs(t, c.Close(ctx), context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

// A new stream is listed only once every watcher received its event. The
// config sync relies on it: it deletes listed streams that have no file, and
// writes the file when it receives the event.
func TestNewStreamIsListedOnlyAfterWatchersKnowIt(t *testing.T) {
	t.Parallel()
	c, _ := newGatedCore(t)
	events := make(chan domain.StreamSpecWatchEvent)
	require.NoError(t, c.Watch(t.Context(), events))

	upserted := make(chan error, 1)
	go func() {
		_, err := c.UpsertStream(t.Context(), gatedSpec("announced", "fake", 0))
		upserted <- err
	}()

	// The upsert waits in notify, since nobody received the event yet.
	time.Sleep(20 * time.Millisecond)
	streams, err := c.GetStreams(t.Context())
	require.NoError(t, err)
	assert.Empty(t, streams, "listed before the watcher received the event")
	_, err = c.GetStream(t.Context(), "announced")
	require.ErrorIs(t, err, domain.ErrStreamNotFound)

	event := <-events
	assert.Equal(t, "announced", event.StreamSpec.Name)
	require.NoError(t, <-upserted)
	streams, err = c.GetStreams(t.Context())
	require.NoError(t, err)
	assert.Len(t, streams, 1)
}
