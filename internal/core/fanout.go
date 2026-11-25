// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/go-tram/tram/internal/core/domain"
)

type fanout struct {
	name     string
	children []domain.Subscriber
	out      recordCounter

	// stream, kind and sinks are what the taps of the stream need: the kind
	// of the node, and the names of the children that are sinks, "" for
	// stages, by index of children. taps is nil if nothing can tap it.
	stream string
	kind   domain.NodeKind
	sinks  []string
	taps   *taps
}

func (f *fanout) Name() string { return f.name }
func (f *fanout) From() string { return "" }

func (f *fanout) Stop() error { return nil }

func (f *fanout) Notify(ctx context.Context, event *domain.Event) error {
	f.out.count(ctx, event)

	if f.taps.listening() {
		return f.notifyTapped(ctx, event)
	}
	if len(f.children) == 1 {
		return f.children[0].Notify(ctx, event)
	}
	eg := errgroup.Group{}
	for _, child := range f.children {
		eg.Go(func() error { return child.Notify(ctx, event) })
	}
	return eg.Wait()
}

// notifyTapped is Notify while a stream is tapped: the node's output, and
// what each sink among the children received and how it handled it, go to
// the taps that want them.
func (f *fanout) notifyTapped(ctx context.Context, event *domain.Event) error {
	if listeners := f.taps.wanting(f.stream, f.name); len(listeners) > 0 {
		publish(listeners, domain.TapEvent{Component: f.name, Kind: f.kind, Time: time.Now(), Event: event})
	}
	eg := errgroup.Group{}
	for i, child := range f.children {
		eg.Go(func() error { return f.notifyChild(ctx, f.sinks[i], child, event) })
	}
	return eg.Wait()
}

func (f *fanout) notifyChild(ctx context.Context, sink string, child domain.Subscriber, event *domain.Event) error {
	if sink == "" {
		return child.Notify(ctx, event)
	}
	listeners := f.taps.wanting(f.stream, sink)
	if len(listeners) == 0 {
		return child.Notify(ctx, event)
	}
	start := time.Now()
	err := child.Notify(ctx, event)
	publish(listeners, domain.TapEvent{
		Component:    sink,
		Kind:         domain.KindSink,
		Time:         start,
		Event:        event,
		SinkErr:      err,
		SinkDuration: time.Since(start),
	})
	return err
}
