// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

const (
	minRestartDelay = time.Second
	maxRestartDelay = time.Minute
)

// Core runs the streams. Its mutex guards only its maps and closed, and is
// never held while a stream is built or stopped, nor while watchers are
// notified, which can take long: a source may connect to a broker or wait
// for deliveries in flight. Operations on one stream name run one at a time,
// under the lock of that name (lockName); those on different names run in
// parallel.
type Core struct {
	ctx    context.Context
	logger *slog.Logger
	// mutex guards streams, names, watchers and closed.
	mutex   sync.RWMutex
	streams map[string]*stream
	names   map[string]*nameLock
	closed  bool
	// ops counts the operations in progress, those that may change streams
	// and those that start a tap; Close waits for them before it ends the
	// taps and stops the streams.
	ops sync.WaitGroup
	// wg counts the goroutines that run sources.
	wg            sync.WaitGroup
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source]
	stageFactory  ports.Factory[domain.StageSpec, domain.Stage]
	sinkFactory   ports.Factory[domain.SinkSpec, domain.Sink]
	watchers      map[*watcher]struct{}
	metrics       *metrics
	// taps is nil in a test run, which nothing taps.
	taps *taps

	minRestartDelay time.Duration
	maxRestartDelay time.Duration
}

// stream is replaced, not changed, when its spec changes; only the operation
// holding the lock of its name changes stopped and hidden.
type stream struct {
	// tree is built from the resolved spec, which may hold secret values.
	tree *domain.Tree
	// spec is what the core shows: through the API, to watchers like the
	// config sync, and to the caller of an upsert.
	spec    domain.StreamSpec
	done    chan struct{}
	stopped bool
	// hidden keeps a new stream out of GetStream and GetStreams until the
	// watchers were notified of it. A watcher may rely on that, like the
	// config sync, which deletes listed streams whose file it has not seen.
	hidden bool
}

// nameLock serializes the operations on one stream name. It lives in
// Core.names while an operation holds or waits for it.
type nameLock struct {
	mu sync.Mutex
	// refs counts the operations holding or waiting for the lock; guarded
	// by Core.mutex.
	refs int
}

func (s *stream) running() bool {
	// A suspended stream has no source running, so no done channel.
	if s.done == nil {
		return false
	}
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

type watcher struct {
	ctx context.Context
	ch  chan<- domain.StreamSpecWatchEvent
}

func New(
	ctx context.Context,
	logger *slog.Logger,
	sourceFactory ports.Factory[domain.SourceSpec, domain.Source],
	stageFactory ports.Factory[domain.StageSpec, domain.Stage],
	sinkFactory ports.Factory[domain.SinkSpec, domain.Sink],
) *Core {
	m, err := newMetrics(otel.GetMeterProvider())
	if err != nil {
		logger.ErrorContext(ctx, "Could not create metrics", slog.Any("error", err))
	}

	return &Core{
		ctx:           ctx,
		logger:        logger,
		streams:       map[string]*stream{},
		names:         map[string]*nameLock{},
		sourceFactory: sourceFactory,
		stageFactory:  stageFactory,
		sinkFactory:   sinkFactory,
		watchers:      map[*watcher]struct{}{},
		metrics:       m,
		taps:          newTaps(),

		minRestartDelay: minRestartDelay,
		maxRestartDelay: maxRestartDelay,
	}
}

func (c *Core) UpsertStream(ctx context.Context, spec domain.StreamSpec) (*domain.StreamSpec, error) {
	return c.UpsertResolvedStream(ctx, spec, spec)
}

// UpsertResolvedStream runs resolved, a copy of spec with references like
// secrets replaced by their values, and shows only spec. The stream is
// replaced if either changed, so a changed secret restarts it.
func (c *Core) UpsertResolvedStream(
	ctx context.Context,
	spec, resolved domain.StreamSpec,
) (*domain.StreamSpec, error) {
	tree, err := newResolvedTree(spec, resolved)
	if err != nil {
		return nil, err
	}

	if !c.begin() {
		return nil, domain.ErrCoreClosed
	}
	defer c.ops.Done()
	unlock := c.lockName(spec.Name)
	defer unlock()

	old, exists := c.lookup(spec.Name)
	running := exists && old.running()

	// An unchanged stream is kept as it is, running or suspended; a crashed
	// one (done closed) is rebuilt.
	if exists && (running || resolved.Suspended) && old.tree.Spec.Equal(resolved) && old.spec.Equal(spec) {
		return &old.spec, nil
	}

	// A change of suspended stops or starts the source, so it is no hot swap.
	if running && old.tree.Spec.Source.Equal(resolved.Source) && old.tree.Spec.Suspended == resolved.Suspended {
		s, swapErr := c.replaceReceivers(old, tree, spec)
		if swapErr != nil {
			return nil, errors.Join(domain.ErrStreamBuildFailed, swapErr)
		}
		c.replace(spec.Name, s)
		c.logger.InfoContext(ctx, "Replaced stages and sinks of stream", slog.String("stream", spec.Name))
		c.taps.retain(spec.Name, tree)
		c.notify(domain.StreamSpecWatchEventTypeUpsert, spec)
		return &s.spec, nil
	}

	return c.rebuild(ctx, tree, spec, old, exists)
}

// rebuild stops old, if it exists, and builds and runs tree in its place.
// The caller holds the lock of the stream's name.
func (c *Core) rebuild(
	ctx context.Context,
	tree *domain.Tree,
	spec domain.StreamSpec,
	old *stream,
	exists bool,
) (*domain.StreamSpec, error) {
	// The old stream is stopped first because it may hold resources the new one
	// needs, like a listening port, but stays registered until it is replaced.
	if exists {
		if err := c.stopStream(old); err != nil {
			c.logger.ErrorContext(ctx, "Could not cleanly stop stream",
				slog.String("stream", spec.Name), slog.Any("error", err))
		}
	}

	if err := c.buildStream(tree); err != nil {
		if exists {
			if restored := c.restoreStream(old.tree.Spec, old.spec); restored != nil {
				c.replace(spec.Name, restored)
			}
		}
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	var s *stream
	if tree.Spec.Suspended {
		c.logger.InfoContext(ctx, "Registered suspended stream; its source does not run",
			slog.String("stream", spec.Name))
		s = &stream{tree: tree, spec: spec}
	} else {
		s = c.runStream(tree, spec, 0)
	}

	if exists {
		c.replace(spec.Name, s)
		c.taps.retain(spec.Name, tree)
		c.notify(domain.StreamSpecWatchEventTypeUpsert, spec)
		return &s.spec, nil
	}

	// A new stream is listed only once the watchers know it.
	if !c.add(spec.Name, s) {
		if err := c.stopStream(s); err != nil {
			c.logger.ErrorContext(ctx, "Could not cleanly stop stream created while closing",
				slog.String("stream", spec.Name), slog.Any("error", err))
		}
		return nil, domain.ErrCoreClosed
	}
	c.notify(domain.StreamSpecWatchEventTypeUpsert, spec)
	c.reveal(s)
	return &s.spec, nil
}

// newResolvedTree validates that resolved belongs to spec and builds its tree.
func newResolvedTree(spec, resolved domain.StreamSpec) (*domain.Tree, error) {
	if err := domain.ValidateStreamName(spec.Name); err != nil {
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}
	if resolved.Name != spec.Name {
		return nil, fmt.Errorf("%w: resolved stream %s is not stream %s",
			domain.ErrStreamBuildFailed, resolved.Name, spec.Name)
	}
	tree, err := newTree(resolved)
	if err != nil {
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}
	return tree, nil
}

func (c *Core) DeleteStream(ctx context.Context, name string) error {
	if !c.begin() {
		return domain.ErrCoreClosed
	}
	defer c.ops.Done()
	unlock := c.lockName(name)
	defer unlock()

	s, exists := c.lookup(name)
	if !exists {
		return domain.ErrStreamNotFound
	}

	c.logger.InfoContext(ctx, "Stopping stream", slog.String("stream", name))
	if err := c.stopStream(s); err != nil {
		c.logger.ErrorContext(ctx, "Could not cleanly stop stream",
			slog.String("stream", name), slog.Any("error", err))
	}

	c.mutex.Lock()
	delete(c.streams, name)
	c.mutex.Unlock()
	c.taps.end(name, fmt.Errorf("%w: %s was deleted", domain.ErrStreamNotFound, name))
	c.notify(domain.StreamSpecWatchEventTypeDelete, s.spec)
	c.logger.InfoContext(ctx, "Deleted stream", slog.String("stream", name))

	return nil
}

func (c *Core) GetStream(_ context.Context, name string) (*domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	s, exists := c.streams[name]
	if !exists || s.hidden {
		return nil, domain.ErrStreamNotFound
	}
	return &s.spec, nil
}

func (c *Core) GetStreams(_ context.Context) ([]domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	streams := make([]domain.StreamSpec, 0, len(c.streams))
	for _, s := range c.streams {
		if !s.hidden {
			streams = append(streams, s.spec)
		}
	}
	return streams, nil
}

// Close refuses new operations, waits for those in progress, and stops all
// streams in parallel. It returns ctx's error if that takes longer than ctx
// allows; the streams keep stopping in the background then.
func (c *Core) Close(ctx context.Context) error {
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return nil
	}
	c.closed = true
	c.mutex.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Operations in progress finish first; their streams are then in
		// the map, or stopped by themselves if they were new. A tap that
		// started meanwhile is registered by then, so all of them end.
		c.ops.Wait()
		c.taps.endAll(domain.ErrCoreClosed)

		c.mutex.Lock()
		streams := maps.Clone(c.streams)
		clear(c.streams)
		c.mutex.Unlock()

		var stopping sync.WaitGroup
		for name, s := range streams {
			stopping.Go(func() {
				c.logger.InfoContext(c.ctx, "Stopping stream", slog.String("stream", name))
				if err := c.stopStream(s); err != nil {
					c.logger.ErrorContext(c.ctx, "Could not stop stream",
						slog.String("stream", name), slog.Any("error", err))
				}
			})
		}
		stopping.Wait()
		c.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("streams still stopping: %w", ctx.Err())
	}
}

// begin registers an operation that may change streams, unless the core is
// closed. The caller calls c.ops.Done when it finished.
func (c *Core) begin() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return false
	}
	c.ops.Add(1)
	return true
}

// lockName takes the lock of a stream name and returns the function that
// releases it.
func (c *Core) lockName(name string) func() {
	c.mutex.Lock()
	l, ok := c.names[name]
	if !ok {
		l = &nameLock{}
		c.names[name] = l
	}
	l.refs++
	c.mutex.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		c.mutex.Lock()
		l.refs--
		if l.refs == 0 {
			delete(c.names, name)
		}
		c.mutex.Unlock()
	}
}

// lookup returns the stream of name. The caller holds the lock of name, so
// the stream cannot be hidden.
func (c *Core) lookup(name string) (*stream, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	s, exists := c.streams[name]
	return s, exists
}

// replace puts s in place of the stream of name, which is listed already.
func (c *Core) replace(name string, s *stream) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.streams[name] = s
}

// add registers the new stream s hidden, unless the core is closed.
func (c *Core) add(name string, s *stream) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return false
	}
	s.hidden = true
	c.streams[name] = s
	return true
}

// reveal lists s once its watchers were notified.
func (c *Core) reveal(s *stream) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	s.hidden = false
}

func (c *Core) Watch(ctx context.Context, ch chan domain.StreamSpecWatchEvent) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return domain.ErrCoreClosed
	}

	w := &watcher{ctx: ctx, ch: ch}
	c.watchers[w] = struct{}{}
	context.AfterFunc(ctx, func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		delete(c.watchers, w)
	})

	return nil
}

// notify sends an event to every watcher and returns once each received it.
// It is called without c.mutex, but with the lock of the stream's name, so
// the events of one stream arrive in order.
func (c *Core) notify(eventType domain.StreamSpecWatchEventType, spec domain.StreamSpec) {
	event := domain.StreamSpecWatchEvent{
		EventType:  eventType,
		StreamSpec: spec,
	}
	c.mutex.RLock()
	watchers := slices.Collect(maps.Keys(c.watchers))
	c.mutex.RUnlock()
	for _, w := range watchers {
		select {
		case w.ch <- event:
		case <-w.ctx.Done():
		}
	}
}
