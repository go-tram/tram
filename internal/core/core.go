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

type Core struct {
	ctx           context.Context
	logger        *slog.Logger
	mutex         sync.RWMutex
	wg            sync.WaitGroup
	closed        bool
	streams       map[string]*stream
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

type stream struct {
	// tree is built from the resolved spec, which may hold secret values.
	tree *domain.Tree
	// spec is what the core shows: through the API, to watchers like the
	// config sync, and to the caller of an upsert.
	spec    domain.StreamSpec
	done    chan struct{}
	stopped bool
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
		mutex:         sync.RWMutex{},
		streams:       map[string]*stream{},
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return nil, domain.ErrCoreClosed
	}

	tree, err := newResolvedTree(spec, resolved)
	if err != nil {
		return nil, err
	}

	old, exists := c.streams[spec.Name]
	running := exists && old.running()

	// An unchanged stream is kept as it is, running or suspended; a crashed
	// one (done closed) is rebuilt.
	if exists && (running || resolved.Suspended) && old.tree.Spec.Equal(resolved) && old.spec.Equal(spec) {
		return &old.spec, nil
	}

	// A change of suspended stops or starts the source, so it is no hot swap.
	if running && old.tree.Spec.Source.Equal(resolved.Source) && old.tree.Spec.Suspended == resolved.Suspended {
		if err = c.replaceReceivers(old, tree, spec); err != nil {
			return nil, errors.Join(domain.ErrStreamBuildFailed, err)
		}
		c.logger.InfoContext(ctx, "Replaced stages and sinks of stream", slog.String("stream", spec.Name))
		c.taps.retain(spec.Name, tree)
		c.notify(domain.StreamSpecWatchEventTypeUpsert, spec)
		return &c.streams[spec.Name].spec, nil
	}

	// The old stream is stopped first because it may hold resources the new one
	// needs, like a listening port, but stays registered until it is replaced.
	if exists {
		if err = c.stopStream(old); err != nil {
			c.logger.ErrorContext(ctx, "Could not cleanly stop stream",
				slog.String("stream", spec.Name), slog.Any("error", err))
		}
	}

	if err = c.buildStream(tree); err != nil {
		if exists {
			c.restoreStream(old.tree.Spec, old.spec)
		}
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	if resolved.Suspended {
		c.logger.InfoContext(ctx, "Registered suspended stream; its source does not run",
			slog.String("stream", spec.Name))
		c.streams[spec.Name] = &stream{tree: tree, spec: spec}
	} else {
		c.runStream(tree, spec, 0)
	}
	c.taps.retain(spec.Name, tree)
	c.notify(domain.StreamSpecWatchEventTypeUpsert, spec)

	return &c.streams[spec.Name].spec, nil
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return domain.ErrCoreClosed
	}

	stream, exists := c.streams[name]
	if !exists {
		return domain.ErrStreamNotFound
	}

	c.logger.InfoContext(ctx, "Stopping stream", slog.String("stream", name))
	if err := c.stopStream(stream); err != nil {
		c.logger.ErrorContext(ctx, "Could not cleanly stop stream",
			slog.String("stream", name), slog.Any("error", err))
	}

	delete(c.streams, name)
	c.taps.end(name, fmt.Errorf("%w: %s was deleted", domain.ErrStreamNotFound, name))
	c.notify(domain.StreamSpecWatchEventTypeDelete, stream.spec)
	c.logger.InfoContext(ctx, "Deleted stream", slog.String("stream", name))

	return nil
}

func (c *Core) GetStream(_ context.Context, name string) (*domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	stream, exists := c.streams[name]
	if !exists {
		return nil, domain.ErrStreamNotFound
	}
	return &stream.spec, nil
}

func (c *Core) GetStreams(_ context.Context) ([]domain.StreamSpec, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	streams := make([]domain.StreamSpec, 0, len(c.streams))
	for _, stream := range c.streams {
		streams = append(streams, stream.spec)
	}
	return streams, nil
}

func (c *Core) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.closed = true
	c.taps.endAll(domain.ErrCoreClosed)

	for name, stream := range c.streams {
		c.logger.InfoContext(c.ctx, "Stopping stream", slog.String("stream", name))
		if err := c.stopStream(stream); err != nil {
			c.logger.ErrorContext(c.ctx, "Could not stop stream", slog.String("stream", name), slog.Any("error", err))
		}
	}
	clear(c.streams)

	c.wg.Wait()

	return nil
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

func (c *Core) notify(eventType domain.StreamSpecWatchEventType, spec domain.StreamSpec) {
	event := domain.StreamSpecWatchEvent{
		EventType:  eventType,
		StreamSpec: spec,
	}
	for w := range c.watchers {
		select {
		case w.ch <- event:
		case <-w.ctx.Done():
		}
	}
}
