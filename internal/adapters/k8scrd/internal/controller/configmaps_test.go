// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/adapters/k8scrd/internal/controller"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	routingA   = "routing-a"
	routingB   = "routing-b"
	tableLabel = "tram.io/table"
)

const routingSelector = `{"valueFrom":{"configMapSelector":{"matchLabels":{"tram.io/table":"routing"}}}}`

func configMap(namespace, name string, labels map[string]string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{Namespace: namespace, Name: name, Labels: labels, Data: data}
}

func routing() map[string]string { return map[string]string{tableLabel: "routing"} }

// selectingStream is in namespace ns and has a value_map stage whose table is
// given by table.
func selectingStream(table string) *tramv1.Stream {
	s := newStream("ns")
	s.Spec.Stages = []tramv1.ReceiverSpec{{
		Name: "map",
		Type: "value_map",
		From: "in",
		Spec: &runtime.RawExtension{Raw: []byte(
			`{"source_attr":"tenant","target_attr":"router","default":"none","table":` + table + `}`)},
	}}
	s.Spec.Sinks[0].From = "map"
	return s
}

func configMapReconciler(t *testing.T, objs ...client.Object) (*controller.StreamReconciler, *fakeCore, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, tramv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&tramv1.Stream{}).
		WithIndex(&tramv1.Stream{}, controller.ConfigMapIndex, func(obj client.Object) []string {
			stream, _ := obj.(*tramv1.Stream)
			return controller.ConfigMapIndexValues(stream)
		}).
		WithObjects(objs...).
		Build()
	core := &fakeCore{streams: map[string]domain.StreamSpec{}, resolved: map[string]domain.StreamSpec{}}
	return &controller.StreamReconciler{Client: c, Scheme: scheme, Core: core}, core, c
}

func table(t *testing.T, spec domain.StreamSpec) any {
	t.Helper()
	require.Len(t, spec.Stages, 1)
	return spec.Stages[0].Spec["table"]
}

func TestReplaceRefsParsesConfigMapSelectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selector any
		want     string
		wantErr  bool
	}{
		{name: "match labels", selector: map[string]any{"matchLabels": map[string]any{"a": "b"}}, want: "a=b"},
		{name: "match expressions", selector: map[string]any{"matchExpressions": []any{
			map[string]any{"key": "a", "operator": "In", "values": []any{"x", "y"}},
		}}, want: "a in (x,y)"},
		{name: "not an object", selector: "routing", wantErr: true},
		{name: "empty selector", selector: map[string]any{}, wantErr: true},
		{name: "empty match labels", selector: map[string]any{"matchLabels": map[string]any{}}, wantErr: true},
		{name: "unknown field", selector: map[string]any{"name": "routing"}, wantErr: true},
		{name: "invalid operator", selector: map[string]any{"matchExpressions": []any{
			map[string]any{"key": "a", "operator": "Like", "values": []any{"x"}},
		}}, wantErr: true},
		{
			name:     "invalid label value",
			selector: map[string]any{"matchLabels": map[string]any{"a": "not valid!"}},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			in := map[string]any{"table": map[string]any{"valueFrom": map[string]any{"configMapSelector": tt.selector}}}
			out, found, err := controller.ReplaceRefs(in, func(string, string) any { return nil })
			if tt.wantErr {
				require.ErrorIs(t, err, controller.ErrInvalidConfigMapSelector)
				return
			}
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, map[string]any{"table": "configmaps:" + tt.want}, out)
		})
	}
}

func TestConfigMapSelectors(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"tram.io/table=routing"}, controller.ConfigMapSelectors(selectingStream(routingSelector)))
	assert.Empty(t, controller.ConfigMapSelectors(newStream("ns")))
	assert.Empty(t, controller.ConfigMapSelectors(referringStream(tokenRef)), "secrets are no config map selectors")
}

func TestReconcileMergesSelectedConfigMaps(t *testing.T) {
	t.Parallel()

	r, core, _ := configMapReconciler(t,
		selectingStream(routingSelector),
		configMap("ns", routingA, routing(), map[string]string{"t1": "r1", "t2": "r1"}),
		configMap("ns", routingB, routing(), map[string]string{"t3": "r2", "t2": "r1"}),
		// Not selected: other labels, or another namespace.
		configMap("ns", "other", map[string]string{tableLabel: "other"}, map[string]string{"t9": "x"}),
		configMap("other", "routing-c", routing(), map[string]string{"t8": "x"}),
	)
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.NoError(t, err)

	assert.Equal(t, map[string]any{"t1": "r1", "t2": "r1", "t3": "r2"}, table(t, core.resolved["ns.s"]),
		"the stream runs with the merged table; a key with the same value in two config maps is fine")
	assert.Equal(t, map[string]any{"valueFrom": map[string]any{"configMapSelector": map[string]any{
		"matchLabels": map[string]any{tableLabel: "routing"},
	}}}, table(t, core.streams["ns.s"]), "the core shows the selector")
}

func TestReconcileAppliesChangedConfigMap(t *testing.T) {
	t.Parallel()

	r, core, c := configMapReconciler(t,
		selectingStream(routingSelector),
		configMap("ns", routingA, routing(), map[string]string{"t1": "r1"}),
	)
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.NoError(t, err)

	require.NoError(t, c.Create(t.Context(), configMap("ns", routingB, routing(), map[string]string{"t2": "r2"})))
	_, err = r.Reconcile(t.Context(), request("ns"))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"t1": "r1", "t2": "r2"}, table(t, core.resolved["ns.s"]))

	upserts := core.upserts
	_, err = r.Reconcile(t.Context(), request("ns"))
	require.NoError(t, err)
	assert.Equal(t, upserts+1, core.upserts, "the core decides whether the values changed")
}

func TestReconcileRetriesUnresolvableConfigMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		objs []client.Object
		want error
	}{
		{
			name: "no config map matches",
			objs: []client.Object{selectingStream(routingSelector)},
			want: controller.ErrConfigMapNotFound,
		},
		{
			name: "conflicting values",
			objs: []client.Object{
				selectingStream(routingSelector),
				configMap("ns", routingA, routing(), map[string]string{"t1": "r1"}),
				configMap("ns", routingB, routing(), map[string]string{"t1": "r2"}),
			},
			want: controller.ErrConfigMapConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, core, c := configMapReconciler(t, tt.objs...)
			_, err := r.Reconcile(t.Context(), request("ns"))
			require.ErrorIs(t, err, tt.want)
			require.NotErrorIs(t, err, reconcile.TerminalError(nil), "changing the config maps fixes it")
			assert.Empty(t, core.streams)

			var stream tramv1.Stream
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "s"}, &stream))
			assert.False(t, stream.Status.Mounted)
		})
	}
}

func TestReconcileConflictNamesBothConfigMaps(t *testing.T) {
	t.Parallel()

	r, _, _ := configMapReconciler(t,
		selectingStream(routingSelector),
		configMap("ns", routingB, routing(), map[string]string{"t1": "r2"}),
		configMap("ns", routingA, routing(), map[string]string{"t1": "r1"}),
	)
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.ErrorIs(t, err, controller.ErrConfigMapConflict)
	assert.Contains(t, err.Error(), `key t1 is "r1" in routing-a and "r2" in routing-b`)
}

func TestReconcileRejectsInvalidConfigMapSelector(t *testing.T) {
	t.Parallel()

	r, _, _ := configMapReconciler(t, selectingStream(`{"valueFrom":{"configMapSelector":{}}}`))
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.ErrorIs(t, err, controller.ErrInvalidConfigMapSelector)
	assert.ErrorIs(t, err, reconcile.TerminalError(nil))
}

func TestStreamsSelecting(t *testing.T) {
	t.Parallel()

	r, _, _ := configMapReconciler(t, selectingStream(routingSelector), referringStreamNamed("secrets"))

	tests := []struct {
		name      string
		namespace string
		labelSets []map[string]string
		want      []string
	}{
		{name: "matching labels", namespace: "ns", labelSets: []map[string]string{routing()}, want: []string{"s"}},
		{name: "other labels", namespace: "ns", labelSets: []map[string]string{{tableLabel: "x"}}},
		{name: "other namespace", namespace: "other", labelSets: []map[string]string{routing()}},
		{
			name:      "label removed: the old labels match",
			namespace: "ns",
			labelSets: []map[string]string{routing(), {}},
			want:      []string{"s"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, r.StreamsSelecting(t.Context(), tt.namespace, tt.labelSets...))
		})
	}
}

func referringStreamNamed(name string) *tramv1.Stream {
	s := referringStream(tokenRef)
	s.Name = name
	return s
}
