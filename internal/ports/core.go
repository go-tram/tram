// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package ports

import (
	"context"

	"github.com/go-tram/tram/internal/core/domain"
)

type Core interface {
	UpsertStream(ctx context.Context, stream domain.StreamSpec) (*domain.StreamSpec, error)
	// UpsertResolvedStream runs resolved, which holds values like secrets that
	// stream only refers to, and shows only stream.
	UpsertResolvedStream(ctx context.Context, stream, resolved domain.StreamSpec) (*domain.StreamSpec, error)
	// PlanStream validates stream and resolved like an upsert and reports what
	// applying them would do, without changing the running stream.
	PlanStream(ctx context.Context, stream, resolved domain.StreamSpec) (*domain.StreamPlan, error)
	DeleteStream(ctx context.Context, name string) error
	GetStream(ctx context.Context, name string) (*domain.StreamSpec, error)
	GetStreams(ctx context.Context) ([]domain.StreamSpec, error)
	Watch(ctx context.Context, ch chan domain.StreamSpecWatchEvent) error
}

// Tapper taps running streams: it hands out copies of the events passing
// their components, without changing or slowing down the streams.
type Tapper interface {
	// Tap taps the running stream name until ctx ends, the stream is deleted,
	// or the tapped component is removed from it.
	Tap(ctx context.Context, name string, opts domain.TapOptions) (Tap, error)
}

// Tap is a tap of a running stream.
type Tap interface {
	// Next returns the next event of the tap. It returns
	// domain.ErrStreamNotFound once the stream is deleted,
	// domain.ErrComponentNotFound once the tapped component is removed,
	// domain.ErrCoreClosed once the core is closed, and the error of ctx or
	// of the context of the tap once one of them ends.
	Next(ctx context.Context) (domain.TapEvent, error)
}
