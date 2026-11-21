// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/adapters/k8scrd/internal/controller"
	"github.com/go-tram/tram/internal/core/domain"
)

const defaultStream = "default.s"

type fakeCore struct {
	streams  map[string]domain.StreamSpec
	resolved map[string]domain.StreamSpec
	upserts  int
}

func (c *fakeCore) UpsertStream(ctx context.Context, s domain.StreamSpec) (*domain.StreamSpec, error) {
	return c.UpsertResolvedStream(ctx, s, s)
}

func (c *fakeCore) UpsertResolvedStream(_ context.Context, s, resolved domain.StreamSpec) (*domain.StreamSpec, error) {
	c.upserts++
	c.streams[s.Name] = s
	c.resolved[s.Name] = resolved
	return &s, nil
}

func (c *fakeCore) PlanStream(_ context.Context, s, _ domain.StreamSpec) (*domain.StreamPlan, error) {
	return &domain.StreamPlan{Stream: s.Name, Action: domain.StreamPlanActionRebuild}, nil
}

func (c *fakeCore) DeleteStream(_ context.Context, name string) error {
	if _, ok := c.streams[name]; !ok {
		return domain.ErrStreamNotFound
	}
	delete(c.streams, name)
	return nil
}

func (c *fakeCore) GetStream(_ context.Context, name string) (*domain.StreamSpec, error) {
	s, ok := c.streams[name]
	if !ok {
		return nil, domain.ErrStreamNotFound
	}
	return &s, nil
}

func (c *fakeCore) GetStreams(context.Context) ([]domain.StreamSpec, error) { return nil, nil }

func (c *fakeCore) Watch(context.Context, chan domain.StreamSpecWatchEvent) error { return nil }

func newStream(namespace string) *tramv1.Stream {
	return &tramv1.Stream{
		Namespace: namespace, Name: "s",
		Spec: tramv1.StreamSpec{
			Source: tramv1.Spec{
				Name: "in",
				Type: "otlp",
				Spec: &runtime.RawExtension{Raw: []byte(`{"http_port":4318}`)},
			},
			Sinks: []tramv1.ReceiverSpec{{
				Name: "out",
				Type: "nats",
				From: "in",
				Spec: &runtime.RawExtension{Raw: []byte(`{"subject":{"register":["a.>"]}}`)},
			}},
		},
	}
}

func newReconciler(t *testing.T, objs ...*tramv1.Stream) (*controller.StreamReconciler, *fakeCore) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tramv1.AddToScheme(scheme))

	builder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tramv1.Stream{})
	for _, obj := range objs {
		builder = builder.WithObjects(obj)
	}

	core := &fakeCore{streams: map[string]domain.StreamSpec{}, resolved: map[string]domain.StreamSpec{}}
	return &controller.StreamReconciler{Client: builder.Build(), Scheme: scheme, Core: core}, core
}

func request(namespace string) ctrl.Request {
	return ctrl.Request{Namespace: namespace, Name: "s"}
}

func TestReconcileUpsertsOnlyOnChange(t *testing.T) {
	t.Parallel()

	r, core := newReconciler(t, newStream("default"))

	for range 3 {
		_, err := r.Reconcile(t.Context(), request("default"))
		require.NoError(t, err)
	}
	assert.Equal(t, 1, core.upserts)

	var stream tramv1.Stream
	require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "s"}, &stream))
	assert.True(t, stream.Status.Mounted)

	stream.Spec.Source.Spec = &runtime.RawExtension{Raw: []byte(`{"http_port":4319}`)}
	require.NoError(t, r.Update(t.Context(), &stream))
	_, err := r.Reconcile(t.Context(), request("default"))
	require.NoError(t, err)
	assert.Equal(t, 2, core.upserts)
}

func TestReconcileKeepsNamespacesApart(t *testing.T) {
	t.Parallel()

	r, core := newReconciler(t, newStream("a"), newStream("b"))

	for _, ns := range []string{"a", "b"} {
		_, err := r.Reconcile(t.Context(), request(ns))
		require.NoError(t, err)
	}
	require.Len(t, core.streams, 2)

	var stream tramv1.Stream
	require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: "a", Name: "s"}, &stream))
	require.NoError(t, r.Delete(t.Context(), &stream))
	_, err := r.Reconcile(t.Context(), request("a"))
	require.NoError(t, err)

	assert.NotContains(t, core.streams, "a.s")
	assert.Contains(t, core.streams, "b.s")

	_, err = r.Reconcile(t.Context(), request("a"))
	assert.NoError(t, err)
}

func TestToDomainKeepsSpecTypes(t *testing.T) {
	t.Parallel()

	spec, err := controller.ToDomain(defaultStream, *newStream("default"))
	require.NoError(t, err)

	assert.Equal(t, defaultStream, spec.Name)
	assert.Equal(t, map[string]any{"http_port": float64(4318)}, spec.Source.Spec)
	assert.Equal(t, map[string]any{"subject": map[string]any{"register": []any{"a.>"}}}, spec.Sinks[0].Spec)
	assert.Equal(t, domain.MetricsSpec{}, spec.Metrics, "a stream without metrics keeps the defaults")
}

func TestToDomainMapsMetrics(t *testing.T) {
	t.Parallel()

	stream := newStream("default")
	stream.Spec.Metrics = &tramv1.MetricsSpec{
		Records:     &tramv1.NodeMetricsSpec{Stages: new(true)},
		Duration:    &tramv1.NodeMetricsSpec{Source: new(false), Sinks: new(true)},
		RequestSize: new(false),
	}

	spec, err := controller.ToDomain(defaultStream, *stream)
	require.NoError(t, err)

	assert.Equal(t, domain.MetricsSpec{
		Records:     domain.NodeMetrics{Stages: new(true)},
		Duration:    domain.NodeMetrics{Source: new(false), Sinks: new(true)},
		RequestSize: new(false),
	}, spec.Metrics)
}

func TestReconcileRunsOnlyStreamsThatReferToTheEngine(t *testing.T) {
	t.Parallel()

	// The engine label set by hand, without a reference: the cache would hold
	// the Stream, but it runs nothing.
	labeled := newStream("default")
	labeled.Labels = map[string]string{tramv1.EngineLabel("mapper"): ""}
	r, core := newReconciler(t, labeled)
	r.Engine = "mapper"

	_, err := r.Reconcile(t.Context(), request("default"))
	require.NoError(t, err)
	assert.Empty(t, core.streams)

	var stream tramv1.Stream
	key := types.NamespacedName{Namespace: "default", Name: "s"}
	require.NoError(t, r.Get(t.Context(), key, &stream))
	assert.False(t, stream.Status.Mounted, "the status is left to the engines that run the Stream")

	stream.Spec.EngineRefs = []tramv1.EngineReference{{Name: "mapper"}}
	require.NoError(t, r.Update(t.Context(), &stream))
	_, err = r.Reconcile(t.Context(), request("default"))
	require.NoError(t, err)
	assert.Contains(t, core.streams, defaultStream, "a Stream that refers to the engine runs")

	require.NoError(t, r.Get(t.Context(), key, &stream))
	stream.Spec.EngineRefs = []tramv1.EngineReference{{Name: "routers"}}
	require.NoError(t, r.Update(t.Context(), &stream))
	_, err = r.Reconcile(t.Context(), request("default"))
	require.NoError(t, err)
	assert.Empty(t, core.streams, "a Stream that no longer refers to the engine stops, even with a stale label")
}
