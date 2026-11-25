// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	brokenType     = "broken"
	rejectingType  = "rejecting"
	failingType    = "failing"
	blockingType   = "blocking"
	forwardingType = "forwarding"
)

type registry struct {
	mu sync.Mutex
	// liveSources counts sources that are not stopped yet. Like a listening
	// port, only one may exist at a time.
	liveSources int
	failSources int
	created     []string
	stopped     []string
	sources     []*fakeSource
	// sinkSpecs holds the spec of every sink created, by name.
	sinkSpecs map[string][]map[string]any
	// received counts the events each stage and sink accepted, by name.
	received map[string]int
}

func (r *registry) receive(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.received == nil {
		r.received = map[string]int{}
	}
	r.received[name]++
}

func (r *registry) receivedBy(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.received[name]
}

func (r *registry) source(i int) *fakeSource {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sources[i]
}

func (r *registry) sourceCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sources)
}

func (r *registry) create(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.created = append(r.created, name)
}

func (r *registry) createdNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.created...)
}

func (r *registry) stop(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = append(r.stopped, name)
}

func (r *registry) stoppedNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stopped...)
}

type fakeSource struct {
	reg      *registry
	name     string
	shutdown chan struct{}
	crash    chan struct{}
	// started is closed when Run starts, after the source was subscribed to.
	started chan struct{}
	subs    []domain.Subscriber
}

func (s *fakeSource) Name() string { return s.name }
func (s *fakeSource) Subscribe(sub domain.Subscriber) error {
	s.subs = append(s.subs, sub)
	return nil
}
func (s *fakeSource) SetSubscribers(subs []domain.Subscriber) {
	s.subs = subs
}
func (s *fakeSource) Run(ctx context.Context) error {
	close(s.started)
	select {
	case <-s.shutdown:
	case <-ctx.Done():
	case <-s.crash:
		return errors.New("source crashed")
	}
	return nil
}
func (s *fakeSource) Stop() error {
	close(s.shutdown)
	s.reg.mu.Lock()
	s.reg.liveSources--
	s.reg.mu.Unlock()
	s.reg.stop(s.name)
	return nil
}

type fakeSink struct {
	reg   *registry
	name  string
	from  string
	err   error
	block bool
}

func (s *fakeSink) Name() string { return s.name }
func (s *fakeSink) From() string { return s.from }
func (s *fakeSink) Notify(ctx context.Context, _ *domain.Event) error {
	if s.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if s.err == nil {
		s.reg.receive(s.name)
	}
	return s.err
}
func (s *fakeSink) Stop() error { s.reg.stop(s.name); return nil }

type sourceFactory struct{ reg *registry }

func (f sourceFactory) New(spec domain.SourceSpec, _ string) (domain.Source, error) {
	f.reg.mu.Lock()
	defer f.reg.mu.Unlock()
	if spec.Type == brokenType {
		return nil, errors.New("broken source")
	}
	if f.reg.failSources > 0 {
		f.reg.failSources--
		return nil, errors.New("source unavailable")
	}
	if f.reg.liveSources > 0 {
		return nil, domain.ErrPortUnavailable
	}
	f.reg.liveSources++
	f.reg.created = append(f.reg.created, spec.Name)
	src := &fakeSource{reg: f.reg, name: spec.Name, shutdown: make(chan struct{}), crash: make(chan struct{}),
		started: make(chan struct{}),
	}
	f.reg.sources = append(f.reg.sources, src)
	if limit, ok := spec.Spec["delivery_timeout"].(string); ok {
		timeout, err := time.ParseDuration(limit)
		if err != nil {
			return nil, err
		}
		return &limitedSource{fakeSource: src, timeout: timeout}, nil
	}
	return src, nil
}

type limitedSource struct {
	*fakeSource

	timeout time.Duration
}

func (s *limitedSource) DeliveryTimeout() time.Duration { return s.timeout }

type fakeStage struct {
	fakeSink

	forward bool
	hold    time.Duration
	subs    []domain.Subscriber
}

func (s *fakeStage) Notify(ctx context.Context, event *domain.Event) error {
	if !s.forward {
		return s.fakeSink.Notify(ctx, event)
	}
	for _, sub := range s.subs {
		if err := sub.Notify(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeStage) Subscribe(sub domain.Subscriber) error {
	s.subs = append(s.subs, sub)
	return nil
}

type stageFactory struct{ reg *registry }

func (f stageFactory) New(spec domain.StageSpec, _ string) (domain.Stage, error) {
	var hold time.Duration
	if raw, ok := spec.Spec["hold"].(string); ok {
		var err error
		if hold, err = time.ParseDuration(raw); err != nil {
			return nil, err
		}
	}
	f.reg.create(spec.Name)
	return &fakeStage{
		reg: f.reg, name: spec.Name, from: spec.From,
		forward: spec.Type == forwardingType,
		hold:    hold,
	}, nil
}

func (s *fakeStage) MaxHold() time.Duration { return s.hold }

type sinkFactory struct{ reg *registry }

func (f sinkFactory) New(spec domain.SinkSpec, _ string) (domain.Sink, error) {
	if spec.Type == brokenType {
		return nil, errors.New("broken sink")
	}
	f.reg.create(spec.Name)
	f.reg.mu.Lock()
	if f.reg.sinkSpecs == nil {
		f.reg.sinkSpecs = map[string][]map[string]any{}
	}
	f.reg.sinkSpecs[spec.Name] = append(f.reg.sinkSpecs[spec.Name], spec.Spec)
	f.reg.mu.Unlock()
	sink := &fakeSink{reg: f.reg, name: spec.Name, from: spec.From}
	switch spec.Type {
	case rejectingType:
		sink.err = domain.ErrEventRejected
	case failingType:
		sink.err = errors.New("sink unavailable")
	case blockingType:
		sink.block = true
	}
	return sink, nil
}

func newCore(t *testing.T) (*core.Core, *registry) {
	t.Helper()
	reg := &registry{}
	c := core.New(t.Context(), slog.New(slog.DiscardHandler), sourceFactory{reg}, stageFactory{reg}, sinkFactory{reg})
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c, reg
}

func streamSpec(port int, sinks ...domain.SinkSpec) domain.StreamSpec {
	return domain.StreamSpec{
		Name:   "stream",
		Source: domain.SourceSpec{Name: "source", Type: "fake", Spec: map[string]any{"port": float64(port)}},
		Sinks:  sinks,
	}
}

func names[T interface{ Name() string }](items []T) []string {
	var out []string
	for _, item := range items {
		out = append(out, item.Name())
	}
	return out
}

func sink(name, typ string) domain.SinkSpec {
	return domain.SinkSpec{Name: name, Type: typ, From: "source"}
}

func TestUpsertStream(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		update        domain.StreamSpec
		expectErr     bool
		expectSource  string
		expectSinks   []string
		expectStopped []string
	}{
		{
			name:          "replaces stream with a changed source that holds an exclusive resource",
			update:        streamSpec(2, sink("new", "fake")),
			expectSinks:   []string{"new"},
			expectStopped: []string{"source", "old"},
		},
		{
			name:          "restores old stream when building the update fails",
			update:        streamSpec(2, sink("new", brokenType)),
			expectErr:     true,
			expectSinks:   []string{"old"},
			expectStopped: []string{"source", "old", "source"},
		},
		{
			name:        "keeps old stream running when the update is not a tree",
			update:      streamSpec(2, domain.SinkSpec{Name: "new", Type: "fake", From: "missing"}),
			expectErr:   true,
			expectSinks: []string{"old"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, reg := newCore(t)

			_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
			require.NoError(t, err)

			_, err = c.UpsertStream(t.Context(), tc.update)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			got, err := c.GetStream(t.Context(), "stream")
			require.NoError(t, err)
			var sinks []string
			for _, s := range got.Sinks {
				sinks = append(sinks, s.Name)
			}
			assert.Equal(t, tc.expectSinks, sinks)
			assert.ElementsMatch(t, tc.expectStopped, reg.stoppedNames())

			reg.mu.Lock()
			assert.Equal(t, 1, reg.liveSources)
			reg.mu.Unlock()
		})
	}
}

func TestUpsertRejectsInvalidStreamName(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	spec := streamSpec(1, sink("out", "fake"))
	spec.Name = "../escape"
	_, err := c.UpsertStream(t.Context(), spec)
	require.ErrorIs(t, err, domain.ErrInvalidStreamName)
	require.ErrorIs(t, err, domain.ErrStreamBuildFailed)

	assert.Empty(t, reg.createdNames(), "no component is created")
	streams, err := c.GetStreams(t.Context())
	require.NoError(t, err)
	assert.Empty(t, streams)
}

func TestCrashedSourceIsRestarted(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	c.SetRestartDelays(10*time.Millisecond, 50*time.Millisecond)

	spec := streamSpec(1, sink("out", "fake"))
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	reg.mu.Lock()
	reg.failSources = 2
	reg.mu.Unlock()
	close(reg.source(0).crash)

	require.Eventually(t, func() bool { return reg.sourceCount() == 2 }, 5*time.Second, 5*time.Millisecond)

	// The crashed stream is stopped once, although restarting failed twice;
	// the fake source panics when stopped twice.
	assert.ElementsMatch(t, []string{"source", "out"}, reg.stoppedNames())
	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, got.Equal(spec))

	require.NoError(t, c.DeleteStream(t.Context(), "stream"))
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, 2, reg.sourceCount())
}

func TestFailedUpdateAndRestoreKeepStream(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	c.SetRestartDelays(10*time.Millisecond, 50*time.Millisecond)

	old := streamSpec(1, sink("old", "fake"))
	_, err := c.UpsertStream(t.Context(), old)
	require.NoError(t, err)

	events := make(chan domain.StreamSpecWatchEvent, 10)
	require.NoError(t, c.Watch(t.Context(), events))

	reg.mu.Lock()
	reg.failSources = 2
	reg.mu.Unlock()
	_, err = c.UpsertStream(t.Context(), streamSpec(2, sink("new", "fake")))
	require.Error(t, err)

	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err, "the stream disappeared")
	assert.True(t, got.Equal(old))
	assert.Empty(t, events, "watchers are not notified")

	require.Eventually(t, func() bool { return reg.sourceCount() == 2 }, 5*time.Second, 5*time.Millisecond)
	assert.ElementsMatch(t, []string{"source", "old"}, reg.stoppedNames(), "stopped once")
}

func TestUpsertRestartsCrashedStream(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)
	c.SetRestartDelays(time.Hour, time.Hour)

	spec := streamSpec(1, sink("out", "fake"))
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	close(reg.source(0).crash)

	require.Eventually(t, func() bool {
		_, upsertErr := c.UpsertStream(t.Context(), spec)
		return upsertErr == nil && reg.sourceCount() == 2
	}, 5*time.Second, 5*time.Millisecond)
	assert.ElementsMatch(t, []string{"source", "out"}, reg.stoppedNames())
}

func TestUpsertUnchangedStreamDoesNothing(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	spec := streamSpec(1, sink("out", "fake"))
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)

	events := make(chan domain.StreamSpecWatchEvent, 1)
	require.NoError(t, c.Watch(t.Context(), events))

	same := streamSpec(1, domain.SinkSpec{Name: "out", Type: "fake", From: "source", Spec: map[string]any{}})
	same.Stages = []domain.StageSpec{}
	_, err = c.UpsertStream(t.Context(), same)
	require.NoError(t, err)

	assert.Empty(t, reg.stoppedNames(), "nothing is restarted")
	assert.Equal(t, []string{"source", "out"}, reg.createdNames(), "nothing is created again")
	assert.Empty(t, events, "watchers are not notified")
}

func TestUpsertWithUnchangedSourceReplacesOnlyStagesAndSinks(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)

	events := make(chan domain.StreamSpecWatchEvent, 1)
	require.NoError(t, c.Watch(t.Context(), events))

	update := streamSpec(1, domain.SinkSpec{Name: "new", Type: "fake", From: "filter"})
	update.Stages = []domain.StageSpec{{Name: "filter", Type: "fake", From: "source"}}
	_, err = c.UpsertStream(t.Context(), update)
	require.NoError(t, err)

	assert.Equal(t, []string{"old"}, reg.stoppedNames(), "the source keeps running")
	assert.Equal(t, []string{"source", "old", "filter", "new"}, reg.createdNames(), "the source is not created again")

	require.Len(t, reg.sources, 1)
	source := reg.sources[0]
	children := core.Children(source.subs)
	assert.Equal(t, []string{"filter"}, names(children), "the running source delivers to the new stage")
	assert.Equal(t, []string{"new"}, names(core.Children(children[0].(*fakeStage).subs)))

	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, update.Equal(*got))
	require.Len(t, events, 1)
	assert.Equal(t, domain.StreamSpecWatchEventTypeUpsert, (<-events).EventType)

	require.NoError(t, c.DeleteStream(t.Context(), "stream"))
	assert.Equal(t, []string{"old", "source", "filter", "new"}, reg.stoppedNames())
}

func TestUpsertWithUnchangedSourceLeavesStreamUntouchedOnFailure(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("old", "fake")))
	require.NoError(t, err)

	_, err = c.UpsertStream(t.Context(), streamSpec(1, sink("a", "fake"), sink("b", brokenType)))
	require.ErrorIs(t, err, domain.ErrStreamBuildFailed)

	assert.Equal(t, []string{"a"}, reg.stoppedNames(), "only the sink created for the failed update is stopped")
	assert.Equal(
		t,
		[]string{"old"},
		names(core.Children(reg.sources[0].subs)),
		"the source still delivers to the old sink",
	)

	got, err := c.GetStream(t.Context(), "stream")
	require.NoError(t, err)
	assert.True(t, streamSpec(1, sink("old", "fake")).Equal(*got))
}

// Stages like a batch flush on Stop, so they have to stop before their children.
func TestDeleteStreamStopsTopDown(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	spec := domain.StreamSpec{
		Name:   "stream",
		Source: domain.SourceSpec{Name: "source", Type: "fake"},
		Stages: []domain.StageSpec{
			{Name: "first", Type: "fake", From: "source"},
			{Name: "second", Type: "fake", From: "first"},
		},
		Sinks: []domain.SinkSpec{{Name: "out", Type: "fake", From: "second"}},
	}
	_, err := c.UpsertStream(t.Context(), spec)
	require.NoError(t, err)
	require.NoError(t, c.DeleteStream(t.Context(), "stream"))

	assert.Equal(t, []string{"source", "first", "second", "out"}, reg.stoppedNames())
}

func TestDeleteStreamStopsAllComponents(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("a", "fake"), sink("b", "fake")))
	require.NoError(t, err)
	require.NoError(t, c.DeleteStream(t.Context(), "stream"))

	assert.ElementsMatch(t, []string{"source", "a", "b"}, reg.stoppedNames())
	_, err = c.GetStream(t.Context(), "stream")
	assert.ErrorIs(t, err, domain.ErrStreamNotFound)
}

func TestFailedBuildStopsCreatedComponents(t *testing.T) {
	t.Parallel()

	c, reg := newCore(t)

	_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("a", "fake"), sink("b", brokenType)))
	require.ErrorIs(t, err, domain.ErrStreamBuildFailed)

	assert.ElementsMatch(t, []string{"source", "a"}, reg.stoppedNames())
}

func TestWatch(t *testing.T) {
	t.Parallel()

	c, _ := newCore(t)

	ctx, cancel := context.WithCancel(t.Context())
	ch := make(chan domain.StreamSpecWatchEvent)
	require.NoError(t, c.Watch(ctx, ch))

	done := make(chan error, 1)
	go func() {
		_, err := c.UpsertStream(t.Context(), streamSpec(1, sink("a", "fake")))
		done <- err
	}()

	select {
	case event := <-ch:
		assert.Equal(t, domain.StreamSpecWatchEventTypeUpsert, event.EventType)
		assert.Equal(t, "stream", event.StreamSpec.Name)
	case <-time.After(time.Second):
		t.Fatal("no watch event received")
	}
	require.NoError(t, <-done)

	cancel()
	deleted := make(chan error, 1)
	go func() { deleted <- c.DeleteStream(t.Context(), "stream") }()

	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DeleteStream blocked on a cancelled watcher")
	}
}
