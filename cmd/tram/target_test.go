// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

func TestTargetPrecedence(t *testing.T) {
	cases := []struct {
		name    string
		flags   targetFlags
		env     map[string]string
		want    target
		wantErr string
	}{
		{name: "localhost by default", want: target{address: defaultAdminEndpoint}},
		{
			name: "TRAM_ADMIN_ENDPOINT",
			env:  map[string]string{adminEndpointEnv: "env:1"},
			want: target{address: "env:1"},
		},
		{
			name: "TRAM_KUBE",
			env:  map[string]string{kubeEnv: "true"},
			want: target{kube: &kubeTarget{}},
		},
		{
			name:    "TRAM_KUBE and TRAM_ADMIN_ENDPOINT",
			env:     map[string]string{kubeEnv: "1", adminEndpointEnv: "env:1"},
			wantErr: "not both",
		},
		{name: "TRAM_KUBE no boolean", env: map[string]string{kubeEnv: "yes please"}, wantErr: "no boolean"},
		{
			name:  "--admin-endpoint over the environment",
			flags: targetFlags{adminEndpoint: "flag:1"},
			env:   map[string]string{kubeEnv: "1"},
			want:  target{address: "flag:1"},
		},
		{
			name:  "--kube over TRAM_ADMIN_ENDPOINT, with the Kubernetes flags",
			flags: targetFlags{kubeOn: true, kube: kubeTarget{Namespace: "obs", Context: "prod"}},
			env:   map[string]string{adminEndpointEnv: "env:1"},
			want:  target{kube: &kubeTarget{Namespace: "obs", Context: "prod"}},
		},
		{
			name:  "--engine means Kubernetes",
			flags: targetFlags{kube: kubeTarget{Engine: "telemetry"}},
			want:  target{kube: &kubeTarget{Engine: "telemetry"}},
		},
		{
			name:    "--engine and --admin-endpoint",
			flags:   targetFlags{adminEndpoint: "flag:1", kube: kubeTarget{Engine: "telemetry"}},
			wantErr: "not both",
		},
		{
			name:    "--engine and --selector",
			flags:   targetFlags{kube: kubeTarget{Engine: "a", Selector: "b=c"}},
			wantErr: "not both",
		},
		{
			name:    "Kubernetes flags without Kubernetes",
			flags:   targetFlags{kube: kubeTarget{Namespace: "obs"}},
			wantErr: "need a target in Kubernetes",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(adminEndpointEnv, "")
			t.Setenv(kubeEnv, "")
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			got, err := tc.flags.resolve()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func engineObject(namespace, name string, adminPort int64) *unstructured.Unstructured {
	e := &unstructured.Unstructured{}
	e.SetAPIVersion("io.tram.core/v1alpha1")
	e.SetKind("Engine")
	e.SetNamespace(namespace)
	e.SetName(name)
	if adminPort != 0 {
		_ = unstructured.SetNestedField(e.Object, adminPort, "spec", "runtime", "api", "adminPort")
	}
	return e
}

func streamObject(namespace, name string, engines ...string) *unstructured.Unstructured {
	s := &unstructured.Unstructured{}
	s.SetAPIVersion("io.tram.core/v1")
	s.SetKind("Stream")
	s.SetNamespace(namespace)
	s.SetName(name)
	refs := make([]any, 0, len(engines))
	for _, e := range engines {
		refs = append(refs, map[string]any{"name": e})
	}
	_ = unstructured.SetNestedSlice(s.Object, refs, "spec", "engineRefs")
	return s
}

func fakeConnector(namespace string, objects ...runtime.Object) *kubeConnector {
	lists := map[schema.GroupVersionResource]string{
		engineResource(): "EngineList",
		streamResource(): "StreamList",
	}
	return &kubeConnector{
		dynamic:     dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), lists, objects...),
		namespace:   namespace,
		enginePorts: map[string]int{},
	}
}

func TestKubeFindsTheEngines(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		objects    []runtime.Object
		engine     string
		stream     string
		isResource bool
		want       []string
		wantErr    string
	}{
		{name: "the Engine named", engine: "a", want: []string{"a"}},
		{
			name:       "the Engines of the Stream resource",
			objects:    []runtime.Object{streamObject("obs", "logs", "a", "b")},
			stream:     "logs",
			isResource: true,
			want:       []string{"a", "b"},
		},
		{
			name:       "a stream without Stream resource",
			stream:     "logs",
			isResource: true,
			wantErr:    "name the pods with --engine or --selector",
		},
		{
			name:    "the only Engine of the namespace",
			objects: []runtime.Object{engineObject("obs", "a", 0), engineObject("other", "b", 0)},
			want:    []string{"a"},
		},
		{
			name:    "several Engines",
			objects: []runtime.Object{engineObject("obs", "b", 0), engineObject("obs", "a", 0)},
			wantErr: "several Engines in namespace obs (a, b): choose one with --engine",
		},
		{name: "no Engine", wantErr: "no Engine in namespace obs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			k := fakeConnector("obs", tc.objects...)
			got, err := k.engines(t.Context(), tc.engine, tc.stream, tc.isResource)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func readyPodOf(name, engine string) *corev1.Pod {
	return &corev1.Pod{
		Name: name, Namespace: "obs", Labels: map[string]string{
			tramv1alpha1.EngineLabel: engine,
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func TestKubeTakesTheAdminPortOfEachEngine(t *testing.T) {
	t.Parallel()

	k := fakeConnector("obs", engineObject("obs", "a", 6000), engineObject("obs", "b", 0))
	require.NoError(t, k.selectEngines(t.Context(), []string{"a", "b"}))
	assert.Equal(t, "tram.io/engine in (a,b)", k.selector.String())

	k.clientset = fake.NewClientset(readyPodOf("b-1", "b"), readyPodOf("a-1", "a"), readyPodOf("c-1", "c"))
	pods, err := k.pods(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []readyPod{{name: "a-1", port: 6000}, {name: "b-1", port: tramv1alpha1.DefaultAdminPort}}, pods)
}

func TestReadyPods(t *testing.T) {
	t.Parallel()

	pod := func(phase corev1.PodPhase, ready corev1.ConditionStatus, deleting bool) *corev1.Pod {
		p := &corev1.Pod{Status: corev1.PodStatus{
			Phase:      phase,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}},
		}}
		if deleting {
			p.DeletionTimestamp = &metav1.Time{}
		}
		return p
	}
	assert.True(t, ready(pod(corev1.PodRunning, corev1.ConditionTrue, false)))
	assert.False(t, ready(pod(corev1.PodRunning, corev1.ConditionFalse, false)), "not ready yet")
	assert.False(t, ready(pod(corev1.PodPending, corev1.ConditionTrue, false)))
	assert.False(t, ready(pod(corev1.PodRunning, corev1.ConditionTrue, true)), "terminating")
}
