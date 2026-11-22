// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/go-tram/tram/internal/adapters/k8scrd"
	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

const (
	testImage     = "tram:test"
	testStreamUID = "fc9e4198-cbe4-686e-95f7-abfa5a16a23e"
	otlpHTTP      = "otlp-http"
	managerName   = "tram-manager"
)

// startEngineCluster starts a cluster whose admin client knows Engines.
func startEngineCluster(t *testing.T) *cluster {
	t.Helper()
	c := startCluster(t)
	require.NoError(t, tramv1alpha1.AddToScheme(c.admin.Scheme()))
	return c
}

func (c *cluster) createEngine(t *testing.T, namespace, name string, spec tramv1alpha1.EngineSpec) {
	t.Helper()
	engine := &tramv1alpha1.Engine{Namespace: namespace, Name: name, Spec: spec}
	require.NoError(t, c.admin.Create(t.Context(), engine))
}

func (c *cluster) updateEngine(t *testing.T, namespace, name string, mutate func(*tramv1alpha1.EngineSpec)) {
	t.Helper()
	var engine tramv1alpha1.Engine
	require.NoError(t, c.admin.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, &engine))
	base := engine.DeepCopy()
	mutate(&engine.Spec)
	require.NoError(t, c.admin.Patch(t.Context(), &engine, client.MergeFrom(base)))
}

// requireEngine waits until the Engine's status satisfies check.
func (c *cluster) requireEngine(
	t *testing.T,
	namespace, name string,
	check func(*assert.CollectT, *tramv1alpha1.Engine),
) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var engine tramv1alpha1.Engine
		key := types.NamespacedName{Namespace: namespace, Name: name}
		if !assert.NoError(collect, c.admin.Get(t.Context(), key, &engine)) {
			return
		}
		check(collect, &engine)
	}, eventuallyTimeout, pollInterval)
}

// requireOwned waits until the Engine's object of obj's kind exists and the
// Engine controls it, and returns it in obj.
func (c *cluster) requireOwned(t *testing.T, namespace, engine string, obj client.Object) {
	t.Helper()
	key := types.NamespacedName{Namespace: namespace, Name: "tram-engine-" + engine}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		if !assert.NoError(collect, c.admin.Get(t.Context(), key, obj)) {
			return
		}
		refs := obj.GetOwnerReferences()
		if assert.Len(collect, refs, 1) {
			assert.Equal(collect, "Engine", refs[0].Kind)
			assert.Equal(collect, engine, refs[0].Name)
			assert.True(collect, *refs[0].Controller)
		}
	}, eventuallyTimeout, pollInterval)
}

func (c *cluster) requireGone(t *testing.T, namespace, engine string, obj client.Object) {
	t.Helper()
	key := types.NamespacedName{Namespace: namespace, Name: "tram-engine-" + engine}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.True(collect, apierrors.IsNotFound(c.admin.Get(t.Context(), key, obj)))
	}, eventuallyTimeout, pollInterval)
}

func condition(engine *tramv1alpha1.Engine, conditionType string) metav1.Condition {
	if found := meta.FindStatusCondition(engine.Status.Conditions, conditionType); found != nil {
		return *found
	}
	return metav1.Condition{}
}

func TestManagerRunsEngine(t *testing.T) {
	t.Parallel()
	c := startEngineCluster(t)
	ns := c.newNamespace(t)
	runManager(t, k8scrd.ManagerOptions{
		Kubeconfig: c.adminKubeconfig(t), Namespaces: []string{ns}, EngineImage: testImage,
	})

	c.createEngine(t, ns, testStreamUID, tramv1alpha1.EngineSpec{
		Replicas:         new(int32(2)),
		Ports:            []tramv1alpha1.EnginePort{{Name: otlpHTTP, Port: 4318}},
		DisruptionBudget: &tramv1alpha1.DisruptionBudgetSpec{MaxUnavailable: new(intstr.FromInt32(1))},
		PodTemplate: tramv1alpha1.PodTemplate{Spec: tramv1alpha1.PodSpec{
			NodeSelector: map[string]string{"node-pool": "telemetry"},
		}},
	})

	var deployment appsv1.Deployment
	c.requireOwned(t, ns, testStreamUID, &deployment)
	assert.Equal(t, new(int32(2)), deployment.Spec.Replicas)
	container := deployment.Spec.Template.Spec.Containers[0]
	assert.Equal(t, testImage, container.Image)
	assert.Contains(t, container.Args, "--engine=fc9e4198-cbe4-686e-95f7-abfa5a16a23e")
	assert.Contains(t, container.Args, "--watch-namespace="+ns)
	assert.Equal(
		t,
		"tram-engine-fc9e4198-cbe4-686e-95f7-abfa5a16a23e",
		deployment.Spec.Template.Spec.ServiceAccountName,
	)
	assert.Equal(t, map[string]string{"node-pool": "telemetry"}, deployment.Spec.Template.Spec.NodeSelector)

	var service corev1.Service
	c.requireOwned(t, ns, testStreamUID, &service)
	ports := make([]string, 0, len(service.Spec.Ports))
	for _, p := range service.Spec.Ports {
		ports = append(ports, p.Name)
	}
	assert.Equal(t, []string{"http", "grpc", otlpHTTP}, ports)

	var binding rbacv1.RoleBinding
	c.requireOwned(t, ns, testStreamUID, &binding)
	assert.Equal(t, "manager-role", binding.RoleRef.Name)
	c.requireOwned(t, ns, testStreamUID, &corev1.ServiceAccount{})
	c.requireOwned(t, ns, testStreamUID, &policyv1.PodDisruptionBudget{})

	c.requireEngine(
		t,
		ns,
		testStreamUID,
		func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
			assert.Equal(collect, e.Generation, e.Status.ObservedGeneration)
			assert.Equal(collect, "tram.io/engine=fc9e4198-cbe4-686e-95f7-abfa5a16a23e", e.Status.Selector)
			assert.Equal(collect, metav1.ConditionTrue, condition(e, tramv1alpha1.ConditionReconciled).Status)
			// envtest runs no Deployment controller.
			assert.Equal(collect, "DeploymentUnavailable", condition(e, tramv1alpha1.ConditionReady).Reason)
		},
	)

	// A Stream that refers to the Engine is counted and accepted.
	c.createEngineStream(t, ns, "s", nil, testStreamUID)
	c.requireEngine(
		t,
		ns,
		testStreamUID,
		func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
			assert.Equal(collect, int32(1), e.Status.Streams)
		},
	)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var s tramv1.Stream
		if !assert.NoError(collect, c.admin.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: "s"}, &s)) ||
			!assert.Len(collect, s.Status.Engines, 1) {
			return
		}
		accepted := meta.FindStatusCondition(s.Status.Engines[0].Conditions, tramv1.ConditionAccepted)
		if assert.NotNil(collect, accepted) {
			assert.Equal(collect, metav1.ConditionTrue, accepted.Status)
		}
	}, eventuallyTimeout, pollInterval)

	// The Deployment becomes available.
	base := deployment.DeepCopy()
	deployment.Status = appsv1.DeploymentStatus{
		Replicas: 2, ReadyReplicas: 2,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue,
			Reason: "MinimumReplicasAvailable", Message: "Deployment has minimum availability.",
		}},
	}
	require.NoError(t, c.admin.Status().Patch(t.Context(), &deployment, client.MergeFrom(base)))
	c.requireEngine(
		t,
		ns,
		testStreamUID,
		func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
			assert.Equal(collect, int32(2), e.Status.ReadyReplicas)
			assert.Equal(collect, metav1.ConditionTrue, condition(e, tramv1alpha1.ConditionReady).Status)
		},
	)

	// The scale subresource serves the selector, for autoscalers like KEDA.
	var engine tramv1alpha1.Engine
	require.NoError(
		t,
		c.admin.Get(
			t.Context(),
			types.NamespacedName{Namespace: ns, Name: testStreamUID},
			&engine,
		),
	)
	var scale autoscalingv1.Scale
	require.NoError(t, c.admin.SubResource("scale").Get(t.Context(), &engine, &scale))
	assert.Equal(t, int32(2), scale.Spec.Replicas)
	assert.Equal(t, "tram.io/engine=fc9e4198-cbe4-686e-95f7-abfa5a16a23e", scale.Status.Selector)

	// A resource deleted by hand is restored, one the Engine no longer asks
	// for is deleted.
	require.NoError(t, c.admin.Delete(t.Context(), &service))
	c.requireOwned(t, ns, testStreamUID, &corev1.Service{})
	c.updateEngine(
		t,
		ns,
		testStreamUID,
		func(spec *tramv1alpha1.EngineSpec) { spec.DisruptionBudget = nil },
	)
	c.requireGone(t, ns, testStreamUID, &policyv1.PodDisruptionBudget{})
}

// ownsReplicas tells whether manager applied the replicas of deployment.
func ownsReplicas(t *testing.T, deployment *appsv1.Deployment, manager string) bool {
	t.Helper()
	for _, entry := range deployment.ManagedFields {
		if entry.Manager != manager || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]map[string]any
		require.NoError(t, json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields))
		if _, ok := fields["f:spec"]["f:replicas"]; ok {
			return true
		}
	}
	return false
}

func TestEngineHandsReplicasOverToAutoscaler(t *testing.T) {
	t.Parallel()
	c := startEngineCluster(t)
	ns := c.newNamespace(t)
	runManager(t, k8scrd.ManagerOptions{
		Kubeconfig: c.adminKubeconfig(t), Namespaces: []string{ns}, EngineImage: testImage,
	})

	c.createEngine(t, ns, "routers", tramv1alpha1.EngineSpec{Replicas: new(int32(3))})
	key := types.NamespacedName{Namespace: ns, Name: "tram-engine-routers"}
	requireReplicas := func(want int32) {
		t.Helper()
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			var d appsv1.Deployment
			if assert.NoError(collect, c.admin.Get(t.Context(), key, &d)) {
				assert.Equal(collect, new(want), d.Spec.Replicas)
			}
		}, eventuallyTimeout, pollInterval)
	}
	requireReplicas(3)

	// Enabling autoscaling keeps the replicas instead of resetting them to 1,
	// and leaves them to the autoscaler.
	c.updateEngine(t, ns, "routers", func(spec *tramv1alpha1.EngineSpec) {
		spec.Autoscaling = &tramv1alpha1.AutoscalingSpec{
			Enabled: true, MinReplicas: new(int32(2)), MaxReplicas: new(int32(6)),
		}
	})
	c.requireOwned(t, ns, "routers", &autoscalingv2.HorizontalPodAutoscaler{})
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var d appsv1.Deployment
		if assert.NoError(collect, c.admin.Get(t.Context(), key, &d)) {
			assert.False(collect, ownsReplicas(t, &d, managerName), "the manager no longer applies the replicas")
		}
	}, eventuallyTimeout, pollInterval)
	requireReplicas(3)

	// The autoscaler scales; a later change of the Engine keeps its replicas.
	var deployment appsv1.Deployment
	require.NoError(t, c.admin.Get(t.Context(), key, &deployment))
	scale := &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: 5}}
	require.NoError(t, c.admin.SubResource("scale").Update(t.Context(), &deployment,
		client.WithSubResourceBody(scale), client.FieldOwner("horizontal-pod-autoscaler")))
	c.updateEngine(t, ns, "routers", func(spec *tramv1alpha1.EngineSpec) { spec.Runtime.LogLevel = "debug" })
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var d appsv1.Deployment
		if assert.NoError(collect, c.admin.Get(t.Context(), key, &d)) {
			assert.Contains(collect, d.Spec.Template.Spec.Containers[0].Args, "--log-level=debug")
		}
	}, eventuallyTimeout, pollInterval)
	requireReplicas(5)

	// Disabling autoscaling deletes the autoscaler and applies spec.replicas.
	c.updateEngine(t, ns, "routers", func(spec *tramv1alpha1.EngineSpec) {
		spec.Autoscaling.Enabled = false
		spec.Replicas = new(int32(4))
	})
	c.requireGone(t, ns, "routers", &autoscalingv2.HorizontalPodAutoscaler{})
	requireReplicas(4)
}

func TestEngineReportsInvalidSpec(t *testing.T) {
	t.Parallel()
	c := startEngineCluster(t)
	ns := c.newNamespace(t)
	runManager(t, k8scrd.ManagerOptions{Kubeconfig: c.adminKubeconfig(t), Namespaces: []string{ns}})

	// Neither the Engine nor the manager names an image.
	c.createEngine(t, ns, "broken", tramv1alpha1.EngineSpec{})
	c.requireEngine(t, ns, "broken", func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		reconciled := condition(e, tramv1alpha1.ConditionReconciled)
		assert.Equal(collect, metav1.ConditionFalse, reconciled.Status)
		assert.Equal(collect, "InvalidSpec", reconciled.Reason)
		assert.Contains(collect, reconciled.Message, "no image")
	})
	require.Never(t, func() bool {
		var d appsv1.Deployment
		key := types.NamespacedName{Namespace: ns, Name: "tram-engine-broken"}
		return c.admin.Get(t.Context(), key, &d) == nil
	}, settleTime, pollInterval, "an invalid Engine gets no Deployment")

	// A port that collides with the HTTP API.
	c.updateEngine(t, ns, "broken", func(spec *tramv1alpha1.EngineSpec) {
		spec.Image = testImage
		spec.Ports = []tramv1alpha1.EnginePort{{Name: otlpHTTP, Port: 8080}}
	})
	c.requireEngine(t, ns, "broken", func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Contains(collect, condition(e, tramv1alpha1.ConditionReconciled).Message, "both use 8080")
	})

	c.updateEngine(t, ns, "broken", func(spec *tramv1alpha1.EngineSpec) { spec.Ports[0].Port = 4318 })
	c.requireOwned(t, ns, "broken", &appsv1.Deployment{})
	c.requireEngine(t, ns, "broken", func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Equal(collect, metav1.ConditionTrue, condition(e, tramv1alpha1.ConditionReconciled).Status)
	})
}

func TestEngineRejectsInvalidSpecs(t *testing.T) {
	t.Parallel()
	c := startEngineCluster(t)
	ns := c.newNamespace(t)

	tests := []struct {
		name string
		spec tramv1alpha1.EngineSpec
	}{
		{"a-name-longer-than-fifty-one-characters-is-too-long-x", tramv1alpha1.EngineSpec{}},
		{"autoscaling-without-bounds", tramv1alpha1.EngineSpec{
			Autoscaling: &tramv1alpha1.AutoscalingSpec{Enabled: true},
		}},
		{"min-above-max", tramv1alpha1.EngineSpec{
			Autoscaling: &tramv1alpha1.AutoscalingSpec{MinReplicas: new(int32(5)), MaxReplicas: new(int32(2))},
		}},
		{"budget-with-both", tramv1alpha1.EngineSpec{DisruptionBudget: &tramv1alpha1.DisruptionBudgetSpec{
			MinAvailable: new(intstr.FromInt32(1)), MaxUnavailable: new(intstr.FromInt32(1)),
		}}},
		{"empty-budget", tramv1alpha1.EngineSpec{DisruptionBudget: &tramv1alpha1.DisruptionBudgetSpec{}}},
		{"reserved-port-name", tramv1alpha1.EngineSpec{Ports: []tramv1alpha1.EnginePort{{Name: "http", Port: 4318}}}},
		{"managed-pod-label", tramv1alpha1.EngineSpec{PodTemplate: tramv1alpha1.PodTemplate{
			Metadata: tramv1alpha1.PodMetadata{Labels: map[string]string{"tram.io/engine": "other"}},
		}}},
	}
	for _, tt := range tests {
		engine := &tramv1alpha1.Engine{Namespace: ns, Name: tt.name, Spec: tt.spec}
		err := c.admin.Create(t.Context(), engine)
		assert.True(t, apierrors.IsInvalid(err), "%s: %v", tt.name, err)
	}
}

// The manager runs Engines with its own role alone: the bind verb on the
// engines' role lets it bind that role to their service accounts.
func TestManagerRunsEnginesWithItsClusterRole(t *testing.T) {
	t.Parallel()
	c := startEngineCluster(t)

	for _, path := range []string{
		manifest("tram-manager", "rbac", "manager", roleFile),
		manifest("tram-manager", "rbac", "engine", roleFile),
	} {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		var role rbacv1.ClusterRole
		require.NoError(t, yaml.Unmarshal(content, &role))
		require.NoError(t, c.admin.Create(t.Context(), &role))
	}

	const user = managerName
	require.NoError(t, c.admin.Create(t.Context(), &rbacv1.ClusterRoleBinding{
		Name:     managerName,
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: managerName},
		Subjects: []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}))

	ns := c.newNamespace(t)
	c.createEngine(t, ns, "mapper", tramv1alpha1.EngineSpec{
		Image: testImage,
		Autoscaling: &tramv1alpha1.AutoscalingSpec{
			Enabled:     true,
			MinReplicas: new(int32(2)),
			MaxReplicas: new(int32(4)),
		},
		DisruptionBudget: &tramv1alpha1.DisruptionBudgetSpec{MaxUnavailable: new(intstr.FromInt32(1))},
	})
	c.createEngineStream(t, ns, "s", nil, "mapper")

	runManager(t, k8scrd.ManagerOptions{
		Kubeconfig:              c.kubeconfig(t, envtest.User{Name: user}),
		LeaderElection:          true,
		LeaderElectionNamespace: ns,
	})

	for _, obj := range []client.Object{
		&appsv1.Deployment{}, &corev1.Service{}, &corev1.ServiceAccount{}, &rbacv1.RoleBinding{},
		&autoscalingv2.HorizontalPodAutoscaler{}, &policyv1.PodDisruptionBudget{},
	} {
		c.requireOwned(t, ns, "mapper", obj)
	}
	c.requireEngine(t, ns, "mapper", func(collect *assert.CollectT, e *tramv1alpha1.Engine) {
		assert.Equal(collect, metav1.ConditionTrue, condition(e, tramv1alpha1.ConditionReconciled).Status)
		assert.Equal(collect, int32(1), e.Status.Streams)
	})
	c.requireLabels(t, ns, "s", map[string]string{mapperLabel: ""})
}

// examplesDir is examples/k8s/examples, relative to this package.
func examplesDir() string {
	return filepath.Join("..", "..", "..", "examples", "k8s", "examples")
}

// readManifests returns the objects of a YAML file with one or more documents.
func readManifests(t *testing.T, path string) []*unstructured.Unstructured {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	var objs []*unstructured.Unstructured
	for doc := range strings.SplitSeq(string(content), "\n---\n") {
		obj := &unstructured.Unstructured{}
		require.NoError(t, yaml.Unmarshal([]byte(doc), &obj.Object), path)
		if len(obj.Object) > 0 {
			objs = append(objs, obj)
		}
	}
	return objs
}

// The examples in examples/k8s/examples pass the validation of the CRDs, and
// every Stream runs on the example Engine, on a port of its own that the
// Engine declares, so all of them can run on it at once.
func TestExamplesAreValid(t *testing.T) {
	t.Parallel()
	c := startEngineCluster(t)
	ns := c.newNamespace(t)

	paths, err := filepath.Glob(filepath.Join(examplesDir(), "*.yaml"))
	require.NoError(t, err)
	var engines, streams []*unstructured.Unstructured
	for _, path := range paths {
		for _, obj := range readManifests(t, path) {
			obj.SetNamespace(ns)
			if obj.GetKind() == "Engine" {
				engines = append(engines, obj)
			} else {
				streams = append(streams, obj)
			}
		}
	}
	require.Len(t, engines, 1, "the examples run on one Engine")
	require.NotEmpty(t, streams)

	// Created first: the API server's response carries integers as such.
	for _, obj := range append(engines, streams...) {
		require.NoError(t, c.admin.Create(t.Context(), obj), "%s %s", obj.GetKind(), obj.GetName())
	}

	var engine tramv1alpha1.Engine
	key := types.NamespacedName{Namespace: ns, Name: engines[0].GetName()}
	require.NoError(t, c.admin.Get(t.Context(), key, &engine))
	declared := map[int64]bool{}
	for _, p := range engine.Spec.Ports {
		declared[int64(p.Port)] = true
	}

	listening := map[int64]string{}
	for _, stream := range streams {
		name := stream.GetName()
		require.Equal(t, "Stream", stream.GetKind(), name)

		refs, _, refsErr := unstructured.NestedSlice(stream.Object, "spec", "engineRefs")
		require.NoError(t, refsErr, name)
		assert.Equal(t, []any{map[string]any{"name": engine.Name}}, refs, "%s runs on the Engine %s", name, engine.Name)

		if source, _, _ := unstructured.NestedString(stream.Object, "spec", "source", "type"); source != "otlp" {
			continue
		}
		port, found, portErr := unstructured.NestedInt64(stream.Object, "spec", "source", "spec", "http_port")
		require.NoError(t, portErr, name)
		require.True(t, found, name)
		assert.True(t, declared[port], "%s listens on port %d, which the Engine does not declare", name, port)
		if other, ok := listening[port]; ok {
			assert.Failf(t, "port collision", "%s and %s both listen on port %d", other, name, port)
		}
		listening[port] = name
	}
}
