// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

// tappedSpec is source -> stage -> sinks ok and failing.
func tappedSpec() domain.StreamSpec {
	spec := streamSpec(1,
		domain.SinkSpec{Name: "ok", Type: "fake", From: "stage"},
		domain.SinkSpec{Name: "failing", Type: failingType, From: "stage"},
	)
	spec.Stages = []domain.StageSpec{{Name: "stage", Type: forwardingType, From: "source"}}
	return spec
}

func bodyEvent(body string) *domain.Event {
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
		}}}},
	}}}}
}

func bodyOf(event *domain.Event) string {
	return event.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0].GetBody().GetStringValue()
}

// emit delivers event from the source of the running stream.
func emit(t *testing.T, reg *registry, event *domain.Event) {
	t.Helper()
	source := reg.source(reg.sourceCount() - 1)
	for _, sub := range source.subs {
		require.NoError(t, sub.Notify(t.Context(), event))
	}
}

func startTapped(t *testing.T, spec domain.StreamSpec) (*core.Core, *registry) {
	t.Helper()
	c, reg := newCore(t)
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)
	<-reg.source(0).started
	return c, reg
}

func tap(t *testing.T, c *core.Core, opts domain.TapOptions) ports.Tap {
	t.Helper()
	tp, err := c.Tap(t.Context(), "stream", opts)
	require.NoError(t, err)
	return tp
}

func next(t *testing.T, tp ports.Tap) (domain.TapEvent, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return tp.Next(ctx)
}

func TestTapSeesEveryComponent(t *testing.T) {
	t.Parallel()

	c, reg := startTapped(t, tappedSpec())
	tp := tap(t, c, domain.TapOptions{})

	err := reg.source(0).subs[0].Notify(t.Context(), bodyEvent("hello"))
	require.Error(t, err, "the failing sink fails the delivery")

	seen := map[string]domain.TapEvent{}
	for range 4 {
		event, nextErr := next(t, tp)
		require.NoError(t, nextErr)
		seen[event.Component] = event
	}
	require.Len(t, seen, 4)

	assert.Equal(t, domain.KindSource, seen["source"].Kind)
	assert.Equal(t, domain.KindStage, seen["stage"].Kind)
	assert.Equal(t, domain.KindSink, seen["ok"].Kind)
	require.NoError(t, seen["ok"].SinkErr)
	assert.Equal(t, domain.KindSink, seen["failing"].Kind)
	require.Error(t, seen["failing"].SinkErr)
	for _, event := range seen {
		assert.Equal(t, "hello", bodyOf(event.Event))
		assert.Zero(t, event.Dropped)
	}
}

func TestTapOfOneComponent(t *testing.T) {
	t.Parallel()

	c, reg := startTapped(t, tappedSpec())
	tp := tap(t, c, domain.TapOptions{Component: "ok"})

	_ = reg.source(0).subs[0].Notify(t.Context(), bodyEvent("first"))
	_ = reg.source(0).subs[0].Notify(t.Context(), bodyEvent("second"))

	for _, want := range []string{"first", "second"} {
		event, err := next(t, tp)
		require.NoError(t, err)
		assert.Equal(t, "ok", event.Component)
		assert.Equal(t, want, bodyOf(event.Event))
	}
}

func TestTapDoesNotHoldUpTheStream(t *testing.T) {
	t.Parallel()

	c, reg := startTapped(t, streamSpec(1, sink("sink", "fake")))
	tp := tap(t, c, domain.TapOptions{Component: "source"})

	const events = 200
	for range events {
		emit(t, reg, bodyEvent("x"))
	}
	assert.Equal(t, events, reg.receivedBy("sink"), "the stream delivers while nobody reads the tap")

	first, err := next(t, tp)
	require.NoError(t, err)
	read := 1
	dropped := first.Dropped
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		event, nextErr := tp.Next(ctx)
		cancel()
		if nextErr != nil {
			break
		}
		read++
		dropped += event.Dropped
	}
	assert.Less(t, read, events, "a tap that is not read drops events")
	assert.Equal(t, uint64(events-read), dropped, "every dropped event is counted")
}

func TestTapRate(t *testing.T) {
	t.Parallel()

	c, reg := startTapped(t, streamSpec(1, sink("sink", "fake")))
	tp := tap(t, c, domain.TapOptions{Component: "source", Rate: 1})

	for range 10 {
		emit(t, reg, bodyEvent("x"))
	}

	event, err := next(t, tp)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), event.Dropped)
}

func TestTapKeepsRunningWhenStagesAndSinksAreReplaced(t *testing.T) {
	t.Parallel()

	c, reg := startTapped(t, streamSpec(1, sink("old", "fake")))
	tp := tap(t, c, domain.TapOptions{Component: "source"})

	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
	require.NoError(t, err)
	require.Equal(t, 1, reg.sourceCount(), "the source keeps running")

	emit(t, reg, bodyEvent("after replace"))
	event, err := next(t, tp)
	require.NoError(t, err)
	assert.Equal(t, "after replace", bodyOf(event.Event))
}

func TestTapKeepsRunningWhenTheStreamIsRebuilt(t *testing.T) {
	t.Parallel()

	c, reg := startTapped(t, streamSpec(1, sink("sink", "fake")))
	tp := tap(t, c, domain.TapOptions{Component: "sink"})

	_, err := c.UpsertStream(t.Context(), streamSpec(2, sink("sink", "fake")))
	require.NoError(t, err)
	require.Equal(t, 2, reg.sourceCount(), "the source is replaced")
	<-reg.source(1).started

	emit(t, reg, bodyEvent("after rebuild"))
	event, err := next(t, tp)
	require.NoError(t, err)
	assert.Equal(t, "after rebuild", bodyOf(event.Event))
}

func TestTapEnds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		component string
		end       func(t *testing.T, c *core.Core, cancel context.CancelFunc)
		want      error
	}{
		{
			name:      "when its component is removed",
			component: "old",
			end: func(t *testing.T, c *core.Core, _ context.CancelFunc) {
				_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("new", "fake")))
				require.NoError(t, err)
			},
			want: domain.ErrComponentNotFound,
		},
		{
			name: "when the stream is deleted",
			end: func(t *testing.T, c *core.Core, _ context.CancelFunc) {
				require.NoError(t, c.DeleteStream(t.Context(), "stream"))
			},
			want: domain.ErrStreamNotFound,
		},
		{
			name: "when the core is closed",
			end: func(t *testing.T, c *core.Core, _ context.CancelFunc) {
				require.NoError(t, c.Close())
			},
			want: domain.ErrCoreClosed,
		},
		{
			name: "when its context ends",
			end: func(_ *testing.T, _ *core.Core, cancel context.CancelFunc) {
				cancel()
			},
			want: context.Canceled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := startTapped(t, streamSpec(1, sink("old", "fake")))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tp, err := c.Tap(ctx, "stream", domain.TapOptions{Component: tc.component})
			require.NoError(t, err)

			tc.end(t, c, cancel)

			_, err = next(t, tp)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestTapHandsOutEventsTakenBeforeItEnded(t *testing.T) {
	t.Parallel()

	c, reg := startTapped(t, streamSpec(1, sink("sink", "fake")))
	tp := tap(t, c, domain.TapOptions{Component: "source"})

	emit(t, reg, bodyEvent("last"))
	require.NoError(t, c.DeleteStream(t.Context(), "stream"))

	event, err := next(t, tp)
	require.NoError(t, err)
	assert.Equal(t, "last", bodyOf(event.Event))
	_, err = next(t, tp)
	require.ErrorIs(t, err, domain.ErrStreamNotFound)
}

func TestTapRejects(t *testing.T) {
	t.Parallel()

	c, _ := startTapped(t, streamSpec(1, sink("sink", "fake")))

	_, err := c.Tap(t.Context(), "unknown", domain.TapOptions{})
	require.ErrorIs(t, err, domain.ErrStreamNotFound)

	_, err = c.Tap(t.Context(), "stream", domain.TapOptions{Component: "unknown"})
	require.ErrorIs(t, err, domain.ErrComponentNotFound)

	_, err = c.Tap(t.Context(), "stream", domain.TapOptions{Rate: -1})
	require.ErrorIs(t, err, domain.ErrInvalidTapOptions)

	require.NoError(t, c.Close())
	_, err = c.Tap(t.Context(), "stream", domain.TapOptions{})
	require.ErrorIs(t, err, domain.ErrCoreClosed)
}
