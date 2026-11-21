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

const otlpAuth = "otlp-auth"

const tokenRef = `{"valueFrom":{"secretKeyRef":{"name":"otlp-auth","key":"token"}}}`

func ref(name, key string) map[string]any {
	return map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": name, "key": key}}}
}

func secret(namespace string, data map[string]string) *corev1.Secret {
	s := &corev1.Secret{Namespace: namespace, Name: otlpAuth, Data: map[string][]byte{}}
	for k, v := range data {
		s.Data[k] = []byte(v)
	}
	return s
}

// referringStream is in namespace ns and has an OTLP sink whose Authorization header refers to the
// secret otlp-auth.
func referringStream(header string) *tramv1.Stream {
	s := newStream("ns")
	s.Spec.Sinks = []tramv1.ReceiverSpec{{
		Name: "out",
		Type: "otlp",
		From: "in",
		Spec: &runtime.RawExtension{Raw: []byte(`{"url":"http://o:4318","headers":{"Authorization":` + header + `}}`)},
	}}
	return s
}

func secretReconciler(t *testing.T, objs ...client.Object) (*controller.StreamReconciler, *fakeCore, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, tramv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tramv1.Stream{}).WithObjects(objs...).Build()
	core := &fakeCore{streams: map[string]domain.StreamSpec{}, resolved: map[string]domain.StreamSpec{}}
	return &controller.StreamReconciler{Client: c, Scheme: scheme, Core: core}, core, c
}

func header(t *testing.T, spec domain.StreamSpec) any {
	t.Helper()
	headers, ok := spec.Sinks[0].Spec["headers"].(map[string]any)
	require.True(t, ok)
	return headers["Authorization"]
}

func TestReplaceRefs(t *testing.T) {
	t.Parallel()

	in := map[string]any{
		"url":     "http://o",
		"headers": map[string]any{"Authorization": ref("a", "token")},
		"list":    []any{"x", ref("b", "key"), map[string]any{"nested": ref("a", "other")}},
		// Not references: more keys, or another kind of valueFrom.
		"two":   map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{}}, "x": 1},
		"other": map[string]any{"valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": "c"}}},
	}
	out, found, err := controller.ReplaceRefs(in, func(name, key string) any { return name + "/" + key })
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, map[string]any{
		"url":     "http://o",
		"headers": map[string]any{"Authorization": "a/token"},
		"list":    []any{"x", "b/key", map[string]any{"nested": "a/other"}},
		"two":     in["two"],
		"other":   in["other"],
	}, out)
	assert.Equal(t, ref("a", "token"), in["headers"].(map[string]any)["Authorization"], "the input is not modified")

	_, found, err = controller.ReplaceRefs(map[string]any{"url": "x"}, func(string, string) any { return nil })
	require.NoError(t, err)
	assert.False(t, found)
}

func TestReplaceRefsRejectsInvalidReferences(t *testing.T) {
	t.Parallel()

	for _, selector := range []any{
		otlpAuth,
		map[string]any{"name": otlpAuth},
		map[string]any{"key": "token"},
		map[string]any{"name": otlpAuth, "key": "token", "optional": true},
	} {
		in := map[string]any{"token": map[string]any{"valueFrom": map[string]any{"secretKeyRef": selector}}}
		_, _, err := controller.ReplaceRefs(in, func(string, string) any { return "" })
		require.ErrorIs(t, err, controller.ErrInvalidSecretRef, "%v", selector)
	}
}

func TestSecretNames(t *testing.T) {
	t.Parallel()

	s := referringStream(tokenRef)
	s.Spec.Source.Spec = &runtime.RawExtension{Raw: []byte(
		`{"url":{"valueFrom":{"secretKeyRef":{"name":"nats","key":"url"}}},` +
			`"again":{"valueFrom":{"secretKeyRef":{"name":"otlp-auth","key":"x"}}}}`)}
	assert.ElementsMatch(t, []string{"nats", otlpAuth}, controller.SecretNames(s))
	assert.Empty(t, controller.SecretNames(newStream("ns")))
}

func TestReconcileResolvesSecrets(t *testing.T) {
	t.Parallel()

	r, core, _ := secretReconciler(
		t,
		referringStream(tokenRef),
		secret("ns", map[string]string{"token": "Bearer s3cr3t"}),
	)
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.NoError(t, err)

	assert.Equal(t, "Bearer s3cr3t", header(t, core.resolved["ns.s"]), "the stream runs with the value")
	assert.Equal(t, ref(otlpAuth, "token"), header(t, core.streams["ns.s"]), "the core shows the reference")
}

func TestReconcileAppliesChangedSecret(t *testing.T) {
	t.Parallel()

	r, core, c := secretReconciler(t, referringStream(tokenRef), secret("ns", map[string]string{"token": "old"}))
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.NoError(t, err)

	require.NoError(t, c.Update(t.Context(), secret("ns", map[string]string{"token": "new"})))
	_, err = r.Reconcile(t.Context(), request("ns"))
	require.NoError(t, err)
	assert.Equal(t, "new", header(t, core.resolved["ns.s"]))
}

func TestReconcileReadsSecretsOfTheStreamsNamespace(t *testing.T) {
	t.Parallel()

	r, _, _ := secretReconciler(t, referringStream(tokenRef), secret("other", map[string]string{"token": "v"}))
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.ErrorIs(t, err, controller.ErrSecretNotFound)
}

func TestReconcileRetriesMissingSecret(t *testing.T) {
	t.Parallel()

	cases := map[string][]client.Object{
		"missing secret": {referringStream(tokenRef)},
		"missing key":    {referringStream(tokenRef), secret("ns", map[string]string{"other": "v"})},
	}
	for name, objs := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r, core, c := secretReconciler(t, objs...)
			_, err := r.Reconcile(t.Context(), request("ns"))
			require.ErrorIs(t, err, controller.ErrSecretNotFound)
			require.NotErrorIs(t, err, reconcile.TerminalError(nil), "the secret may be created later")
			assert.Empty(t, core.streams)

			var stream tramv1.Stream
			require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "s"}, &stream))
			assert.False(t, stream.Status.Mounted)
		})
	}
}

func TestReconcileRejectsInvalidSecretRef(t *testing.T) {
	t.Parallel()

	r, _, _ := secretReconciler(t, referringStream(`{"valueFrom":{"secretKeyRef":{"name":"otlp-auth"}}}`))
	_, err := r.Reconcile(t.Context(), request("ns"))
	require.ErrorIs(t, err, controller.ErrInvalidSecretRef)
	assert.ErrorIs(t, err, reconcile.TerminalError(nil))
}
