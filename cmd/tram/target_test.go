// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// useConfig points the CLI at a config of its own with the contexts local,
// at local:1, and prod, the Engine telemetry, the current one.
func useConfig(t *testing.T) {
	t.Helper()
	t.Setenv(configEnv, filepath.Join(t.TempDir(), "config.yaml"))
	t.Setenv(adminEndpointEnv, "")
	t.Setenv(contextEnv, "")
	_, err := runCmd(t, "context", "add", "local", "--admin-endpoint", "local:1")
	require.NoError(t, err)
	_, err = runCmd(t, "context", "add", "prod", "--engine", "telemetry", "-n", "obs", "--kube-context", "prod")
	require.NoError(t, err)
	_, err = runCmd(t, "context", "use", "prod")
	require.NoError(t, err)
}

func TestTargetPrecedence(t *testing.T) {
	useConfig(t)
	prod := &kubeTarget{Engine: "telemetry", Namespace: "obs", Context: "prod"}

	cases := []struct {
		name  string
		flags targetFlags
		env   map[string]string
		want  target
	}{
		{name: "the current context", want: target{kube: prod}},
		{
			name: "TRAM_CONTEXT over the current context",
			env:  map[string]string{contextEnv: "local"},
			want: target{address: "local:1"},
		},
		{
			name: "TRAM_ADMIN_ENDPOINT over TRAM_CONTEXT",
			env:  map[string]string{contextEnv: "local", adminEndpointEnv: "env:1"},
			want: target{address: "env:1"},
		},
		{
			name:  "--context over the environment",
			flags: targetFlags{context: "local"},
			env:   map[string]string{adminEndpointEnv: "env:1"},
			want:  target{address: "local:1"},
		},
		{
			name:  "--admin-endpoint over --context",
			flags: targetFlags{adminEndpoint: "flag:1", context: "prod"},
			want:  target{address: "flag:1"},
		},
		{
			name:  "--engine over everything",
			flags: targetFlags{adminEndpoint: "flag:1", kube: kubeTarget{Engine: "other"}},
			want:  target{kube: &kubeTarget{Engine: "other"}},
		},
		{
			name:  "Kubernetes flags refine the context",
			flags: targetFlags{kube: kubeTarget{Namespace: "staging", AdminPort: 6000}},
			want: target{
				kube: &kubeTarget{Engine: "telemetry", Namespace: "staging", Context: "prod", AdminPort: 6000},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			got, err := tc.flags.resolve()
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTargetDefaultsToLocalhost(t *testing.T) {
	t.Setenv(configEnv, filepath.Join(t.TempDir(), "none.yaml"))
	t.Setenv(adminEndpointEnv, "")
	t.Setenv(contextEnv, "")

	got, err := (&targetFlags{}).resolve()
	require.NoError(t, err)
	assert.Equal(t, target{address: defaultAdminEndpoint}, got)
}

//nolint:paralleltest // Sets the environment of the process.
func TestTargetErrors(t *testing.T) {
	useConfig(t)

	_, err := (&targetFlags{context: "missing"}).resolve()
	require.ErrorContains(t, err, `no context "missing"`)

	_, err = (&targetFlags{context: "local", kube: kubeTarget{Namespace: "x"}}).resolve()
	require.ErrorContains(t, err, "need a target in Kubernetes")

	_, err = (&targetFlags{kube: kubeTarget{Engine: "a", Selector: "b=c"}}).resolve()
	require.ErrorContains(t, err, "not both")
}

//nolint:paralleltest // Sets the environment of the process.
func TestContextCommands(t *testing.T) {
	useConfig(t)

	out, err := runCmd(t, "context", "list")
	require.NoError(t, err)
	assert.Contains(t, out, "         local  local:1")
	assert.Contains(t, out, "*        prod   engine telemetry in namespace obs of kube context prod")

	_, err = runCmd(t, "context", "remove", "prod")
	require.NoError(t, err)
	got, err := (&targetFlags{}).resolve()
	require.NoError(t, err)
	assert.Equal(t, target{address: defaultAdminEndpoint}, got, "no current context left")

	_, err = runCmd(t, "context", "use", "prod")
	require.ErrorContains(t, err, `no context "prod"`)
	_, err = runCmd(t, "context", "add", "x")
	require.ErrorContains(t, err, "set --admin-endpoint, --engine or --selector")
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
