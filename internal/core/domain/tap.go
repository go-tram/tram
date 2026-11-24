// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

import "time"

// TapOptions select what a tap of a running stream receives.
type TapOptions struct {
	// Component is the source, stage or sink to tap. Empty taps all of them.
	Component string
	// Rate is the most events per second the tap receives; more are dropped.
	// Zero means no limit, though a tap that is read too slowly still drops.
	Rate float64
}

// TapEvent is an event at a component of a running stream: what a source or
// stage passed on, or what a sink received and how it handled it. Event is
// shared with the stream and, like every event, read-only.
type TapEvent struct {
	Component string
	Kind      NodeKind
	Time      time.Time
	Event     *Event
	// SinkErr and SinkDuration are set for sinks: the error the sink returned,
	// nil if it took the event, and how long it took.
	SinkErr      error
	SinkDuration time.Duration
	// Dropped is the number of events the tap dropped since the previous
	// event, because of its rate or because it was read too slowly.
	Dropped uint64
}
