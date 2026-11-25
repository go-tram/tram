// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"golang.org/x/time/rate"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

// tapBuffer is how many events a tap holds that were not read yet. Once it
// is full, the tap drops events instead of holding up the stream.
const tapBuffer = 64

// Tap taps the running stream name: it hands out the events passing the component opts selects, or all components, until ctx ends,
// the stream is deleted or the component is removed. The stream is not
// changed and never waits for the tap; a tap that falls behind drops events.
// Taps are kept by stream name, so they keep running when the stream is
// replaced or restarted.
func (c *Core) Tap(ctx context.Context, name string, opts domain.TapOptions) (ports.Tap, error) {
	if opts.Rate < 0 {
		return nil, fmt.Errorf("%w: rate %v is negative", domain.ErrInvalidTapOptions, opts.Rate)
	}

	// The tap starts under the lock of the stream's name, so no upsert
	// replaces the stream between the check of its component and the
	// registration; an upsert after it ends the tap if the component is
	// gone. As an operation, it is done before Close ends all taps.
	if !c.begin() {
		return nil, domain.ErrCoreClosed
	}
	defer c.ops.Done()
	unlock := c.lockName(name)
	defer unlock()

	// A stream is hidden until the watchers know it, and the lock of its
	// name is held meanwhile, so it is visible here or does not exist.
	s, exists := c.lookup(name)
	if !exists || s.hidden {
		return nil, domain.ErrStreamNotFound
	}
	if opts.Component != "" && s.tree.GetNode(opts.Component) == nil {
		return nil, fmt.Errorf("%w: %s in stream %s", domain.ErrComponentNotFound, opts.Component, name)
	}

	l := newTapListener(opts)
	c.taps.add(name, l)
	context.AfterFunc(ctx, func() { c.taps.remove(name, l, ctx.Err()) })
	return l, nil
}

// taps holds the taps of the running streams by stream name.
type taps struct {
	// active counts the taps of all streams, so fanouts skip the registry
	// while nothing is tapped.
	active  atomic.Int64
	mu      sync.RWMutex
	streams map[string][]*tapListener
}

func newTaps() *taps {
	return &taps{streams: map[string][]*tapListener{}}
}

// listening reports whether any stream is tapped. t may be nil, e.g. in a
// test run, which nothing taps.
func (t *taps) listening() bool {
	return t != nil && t.active.Load() > 0
}

// wanting returns the taps of stream that take an event of component now.
func (t *taps) wanting(stream, component string) []*tapListener {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var wanting []*tapListener
	for _, l := range t.streams[stream] {
		if l.wants(component) {
			wanting = append(wanting, l)
		}
	}
	return wanting
}

func (t *taps) add(stream string, l *tapListener) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.streams[stream] = append(t.streams[stream], l)
	t.active.Add(1)
}

// remove ends the tap l with err.
func (t *taps) remove(stream string, l *tapListener, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	listeners := t.streams[stream]
	if i := slices.Index(listeners, l); i >= 0 {
		t.set(stream, slices.Delete(slices.Clone(listeners), i, i+1))
		t.active.Add(-1)
	}
	l.end(err)
}

// end ends all taps of stream with err.
func (t *taps) end(stream string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, l := range t.streams[stream] {
		l.end(err)
	}
	t.active.Add(-int64(len(t.streams[stream])))
	delete(t.streams, stream)
}

// retain ends the taps of stream whose component is not in tree.
func (t *taps) retain(stream string, tree *domain.Tree) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var kept []*tapListener
	for _, l := range t.streams[stream] {
		if l.component == "" || tree.GetNode(l.component) != nil {
			kept = append(kept, l)
			continue
		}
		l.end(fmt.Errorf("%w: %s in stream %s", domain.ErrComponentNotFound, l.component, stream))
		t.active.Add(-1)
	}
	t.set(stream, kept)
}

// endAll ends the taps of all streams with err.
func (t *taps) endAll(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for stream := range t.streams {
		for _, l := range t.streams[stream] {
			l.end(err)
		}
		t.active.Add(-int64(len(t.streams[stream])))
	}
	clear(t.streams)
}

// set sets the taps of stream to listeners.
func (t *taps) set(stream string, listeners []*tapListener) {
	if len(listeners) == 0 {
		delete(t.streams, stream)
		return
	}
	t.streams[stream] = listeners
}

// tapListener is one tap. It implements ports.Tap.
type tapListener struct {
	component string
	// limiter is nil if the tap has no rate.
	limiter *rate.Limiter
	events  chan domain.TapEvent
	dropped atomic.Uint64

	endOnce sync.Once
	done    chan struct{}
	err     error
}

func newTapListener(opts domain.TapOptions) *tapListener {
	l := &tapListener{
		component: opts.Component,
		events:    make(chan domain.TapEvent, tapBuffer),
		done:      make(chan struct{}),
	}
	if opts.Rate > 0 {
		l.limiter = rate.NewLimiter(rate.Limit(opts.Rate), max(1, int(opts.Rate)))
	}
	return l
}

// wants reports whether the tap takes an event of component now; an event
// it would have to drop is counted.
func (l *tapListener) wants(component string) bool {
	if l.component != "" && l.component != component {
		return false
	}
	select {
	case <-l.done:
		return false
	default:
	}
	if len(l.events) == cap(l.events) || (l.limiter != nil && !l.limiter.Allow()) {
		l.dropped.Add(1)
		return false
	}
	return true
}

func (l *tapListener) send(event domain.TapEvent) {
	select {
	case l.events <- event:
	default:
		l.dropped.Add(1)
	}
}

func (l *tapListener) end(err error) {
	l.endOnce.Do(func() {
		l.err = err
		close(l.done)
	})
}

func (l *tapListener) Next(ctx context.Context) (domain.TapEvent, error) {
	select {
	case event := <-l.events:
		return l.counted(event), nil
	case <-ctx.Done():
		return domain.TapEvent{}, ctx.Err()
	case <-l.done:
		// Events taken before the tap ended are handed out first.
		select {
		case event := <-l.events:
			return l.counted(event), nil
		default:
			return domain.TapEvent{}, l.err
		}
	}
}

func (l *tapListener) counted(event domain.TapEvent) domain.TapEvent {
	event.Dropped = l.dropped.Swap(0)
	return event
}

// publish hands event to the taps. Events are read-only (ADR 0005), so the
// taps share the event the stream passes on and read it later, without a
// copy.
func publish(listeners []*tapListener, event domain.TapEvent) {
	for _, l := range listeners {
		l.send(event)
	}
}
