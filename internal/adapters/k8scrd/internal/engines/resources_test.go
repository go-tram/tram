// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
	"github.com/go-tram/tram/internal/adapters/k8scrd/internal/engines"
)

const (
	otlpHTTP    = "otlp-http"
	hubMapper   = "telemetry-hub-mapper"
	managerRole = "manager-role"
)

// typed converts an apply configuration to its typed object, the way the API
// server reads it.
func typed[T any](t *testing.T, config any) *T {
	t.Helper()
	data, err := json.Marshal(config)
	require.NoError(t, err)
	out := new(T)
	require.NoError(t, json.Unmarshal(data, out))
	return out
}

func testEngine() *tramv1alpha1.Engine {
	e := &tramv1alpha1.Engine{}
	e.Name, e.Namespace, e.UID = hubMapper, "tram-nats-13", "engine-uid"
	return e
}

func build(t *testing.T, e *tramv1alpha1.Engine, replicas *int32) engines.Resources {
	t.Helper()
	res, err := engines.BuildResources(e, "tram:default", managerRole, replicas)
	require.NoError(t, err)
	return res
}

func TestBuildResourcesDefaults(t *testing.T) {
	t.Parallel()

	res := build(t, testEngine(), new(int32(1)))
	const name, ns = "tram-engine-telemetry-hub-mapper", "tram-nats-13"

	deployment := typed[appsv1.Deployment](t, res.Deployment())
	assert.Equal(t, name, deployment.Name)
	assert.Equal(t, ns, deployment.Namespace)
	assert.Equal(t, new(int32(1)), deployment.Spec.Replicas)
	assert.Equal(
		t,
		map[string]string{engines.EngineLabel: hubMapper},
		deployment.Spec.Selector.MatchLabels,
	)
	require.Len(t, deployment.OwnerReferences, 1)
	assert.Equal(t, metav1.OwnerReference{
		APIVersion: "io.tram.core/v1alpha1", Kind: "Engine", Name: hubMapper, UID: "engine-uid",
		Controller: new(true), BlockOwnerDeletion: new(true),
	}, deployment.OwnerReferences[0])
	assert.Equal(t, engines.FieldOwner, deployment.Labels[engines.ManagedByLabel])

	pod := deployment.Spec.Template
	assert.Equal(t, hubMapper, pod.Labels[engines.EngineLabel])
	assert.Equal(t, name, pod.Spec.ServiceAccountName)
	require.Len(t, pod.Spec.Containers, 1)
	container := pod.Spec.Containers[0]
	assert.Equal(t, "tram", container.Name)
	assert.Equal(t, "tram:default", container.Image)
	assert.Equal(t, []string{
		"server",
		"--log-level=info",
		"--http-endpoint=0.0.0.0:8080",
		"--grpc-endpoint=0.0.0.0:50051",
		"--admin-endpoint=127.0.0.1:50052",
		"--enable-k8s-operator",
		"--engine=telemetry-hub-mapper",
		"--watch-namespace=tram-nats-13",
	}, container.Args)
	assert.Equal(t, []corev1.ContainerPort{
		{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
		{Name: "grpc", ContainerPort: 50051, Protocol: corev1.ProtocolTCP},
	}, container.Ports)
	assert.Equal(t, intstr.FromString("http"), container.ReadinessProbe.TCPSocket.Port)
	assert.Equal(t, new(false), container.SecurityContext.AllowPrivilegeEscalation)
	assert.Equal(t, new(true), container.SecurityContext.RunAsNonRoot)
	assert.Equal(t, new(true), container.SecurityContext.ReadOnlyRootFilesystem)
	// With a read-only root file system, temporary files go to an empty
	// directory.
	assert.Equal(t, []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}}, container.VolumeMounts)
	assert.Equal(t, []corev1.Volume{{Name: "tmp", EmptyDir: &corev1.EmptyDirVolumeSource{}}}, pod.Spec.Volumes)

	service := typed[corev1.Service](t, res.Service())
	assert.Equal(t, corev1.ServiceTypeClusterIP, service.Spec.Type)
	assert.Equal(t, map[string]string{engines.EngineLabel: hubMapper}, service.Spec.Selector)
	assert.Equal(t, []corev1.ServicePort{
		{Name: "http", Port: 8080, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
		{Name: "grpc", Port: 50051, TargetPort: intstr.FromString("grpc"), Protocol: corev1.ProtocolTCP},
	}, service.Spec.Ports)

	binding := typed[rbacv1.RoleBinding](t, res.RoleBinding())
	assert.Equal(t,
		rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: managerRole}, binding.RoleRef)
	assert.Equal(t, []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: name, Namespace: ns}}, binding.Subjects)

	assert.Equal(t, name, typed[corev1.ServiceAccount](t, res.ServiceAccount()).Name)
	assert.Nil(t, res.HPA(), "no autoscaler without autoscaling")
	assert.Nil(t, res.PDB(), "no disruption budget without one")
}

func TestBuildResourcesFromSpec(t *testing.T) {
	t.Parallel()

	e := testEngine()
	e.Spec = tramv1alpha1.EngineSpec{
		Image:           "registry.example.com/tram:1.14.0",
		ImagePullPolicy: corev1.PullIfNotPresent,
		//nolint:exhaustive // Empty lists, set no resources.
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{}, Limits: corev1.ResourceList{}},
		Autoscaling: &tramv1alpha1.AutoscalingSpec{
			Enabled: true, MinReplicas: new(int32(3)), MaxReplicas: new(int32(24)),
			TargetCPUUtilizationPercentage: new(int32(70)),
		},
		DisruptionBudget: &tramv1alpha1.DisruptionBudgetSpec{MaxUnavailable: new(intstr.FromInt32(1))},
		Ports:            []tramv1alpha1.EnginePort{{Name: otlpHTTP, Port: 4318}},
		Service:          tramv1alpha1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
		Runtime: tramv1alpha1.RuntimeSpec{
			LogLevel: "debug",
			API:      tramv1alpha1.APISpec{HTTPPort: 9080, GRPCPort: 9051},
			Pprof:    &tramv1alpha1.PprofSpec{Port: 6060},
		},
		PodTemplate: tramv1alpha1.PodTemplate{
			Metadata: tramv1alpha1.PodMetadata{
				Labels:      map[string]string{"team": "telemetry"},
				Annotations: map[string]string{"prometheus.io/scrape": "true"},
			},
			Spec: tramv1alpha1.PodSpec{
				TerminationGracePeriodSeconds: new(int64(30)),
				PriorityClassName:             "telemetry-high",
				NodeSelector:                  map[string]string{"node-pool": "telemetry"},
				Tolerations: []corev1.Toleration{{
					Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "telemetry",
					Effect: corev1.TaintEffectNoSchedule,
				}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone",
					WhenUnsatisfiable: corev1.ScheduleAnyway,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{engines.EngineLabel: hubMapper},
					},
				}},
				ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry"}},
			},
		},
	}
	e.Spec.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("4")
	e.Spec.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("12Gi")
	require.True(t, engines.Autoscaled(e))

	res := build(t, e, nil)

	deployment := typed[appsv1.Deployment](t, res.Deployment())
	assert.Nil(t, deployment.Spec.Replicas, "the autoscaler sets the replicas")
	pod := deployment.Spec.Template
	assert.Equal(t, "telemetry", pod.Labels["team"])
	assert.Equal(t, hubMapper, pod.Labels[engines.EngineLabel])
	assert.Equal(t, map[string]string{"prometheus.io/scrape": "true"}, pod.Annotations)
	assert.Equal(t, new(int64(30)), pod.Spec.TerminationGracePeriodSeconds)
	assert.Equal(t, "telemetry-high", pod.Spec.PriorityClassName)
	assert.Equal(t, map[string]string{"node-pool": "telemetry"}, pod.Spec.NodeSelector)
	assert.Equal(t, e.Spec.PodTemplate.Spec.Tolerations, pod.Spec.Tolerations)
	assert.Equal(t, e.Spec.PodTemplate.Spec.TopologySpreadConstraints, pod.Spec.TopologySpreadConstraints)
	assert.Equal(t, e.Spec.PodTemplate.Spec.ImagePullSecrets, pod.Spec.ImagePullSecrets)

	container := pod.Spec.Containers[0]
	assert.Equal(t, "registry.example.com/tram:1.14.0", container.Image)
	assert.Equal(t, corev1.PullIfNotPresent, container.ImagePullPolicy)
	assert.True(t, e.Spec.Resources.Requests.Cpu().Equal(*container.Resources.Requests.Cpu()))
	assert.True(t, e.Spec.Resources.Limits.Memory().Equal(*container.Resources.Limits.Memory()))
	assert.Equal(t, []string{
		"server",
		"--log-level=debug",
		"--http-endpoint=0.0.0.0:9080",
		"--grpc-endpoint=0.0.0.0:9051",
		"--admin-endpoint=127.0.0.1:50052",
		"--pprof-endpoint=127.0.0.1:6060",
		"--enable-k8s-operator",
		"--engine=telemetry-hub-mapper",
		"--watch-namespace=tram-nats-13",
	}, container.Args)
	assert.Equal(t, []corev1.ContainerPort{
		{Name: "http", ContainerPort: 9080, Protocol: corev1.ProtocolTCP},
		{Name: "grpc", ContainerPort: 9051, Protocol: corev1.ProtocolTCP},
		{Name: otlpHTTP, ContainerPort: 4318, Protocol: corev1.ProtocolTCP},
	}, container.Ports, "admin and pprof stay internal")

	service := typed[corev1.Service](t, res.Service())
	assert.Equal(t, corev1.ServiceTypeLoadBalancer, service.Spec.Type)
	names := make([]string, 0, len(service.Spec.Ports))
	for _, p := range service.Spec.Ports {
		names = append(names, p.Name)
	}
	assert.Equal(t, []string{"http", "grpc", otlpHTTP}, names, "admin and pprof are not exposed")

	require.NotNil(t, res.HPA())
	hpa := typed[autoscalingv2.HorizontalPodAutoscaler](t, res.HPA())
	assert.Equal(t, autoscalingv2.CrossVersionObjectReference{
		APIVersion: "apps/v1", Kind: "Deployment", Name: "tram-engine-telemetry-hub-mapper",
	}, hpa.Spec.ScaleTargetRef)
	assert.Equal(t, new(int32(3)), hpa.Spec.MinReplicas)
	assert.Equal(t, int32(24), hpa.Spec.MaxReplicas)
	require.Len(t, hpa.Spec.Metrics, 1)
	assert.Equal(t, corev1.ResourceCPU, hpa.Spec.Metrics[0].Resource.Name)
	assert.Equal(t, new(int32(70)), hpa.Spec.Metrics[0].Resource.Target.AverageUtilization)

	require.NotNil(t, res.PDB())
	pdb := typed[policyv1.PodDisruptionBudget](t, res.PDB())
	assert.Equal(t, new(intstr.FromInt32(1)), pdb.Spec.MaxUnavailable)
	assert.Nil(t, pdb.Spec.MinAvailable)
	assert.Equal(t, map[string]string{engines.EngineLabel: hubMapper}, pdb.Spec.Selector.MatchLabels)
}

func TestBuildResourcesAutoscalingDisabledKeepsNoAutoscaler(t *testing.T) {
	t.Parallel()

	e := testEngine()
	e.Spec.Autoscaling = &tramv1alpha1.AutoscalingSpec{MinReplicas: new(int32(3)), MaxReplicas: new(int32(5))}
	assert.Nil(t, build(t, e, new(int32(2))).HPA())
	assert.False(t, engines.Autoscaled(e))
}

func TestBuildResourcesRejectsInvalidEngines(t *testing.T) {
	t.Parallel()

	noImage := testEngine()
	_, err := engines.BuildResources(noImage, "", managerRole, nil)
	require.ErrorIs(t, err, engines.ErrInvalidEngine)
	require.ErrorContains(t, err, "--engine-image")

	collision := testEngine()
	collision.Spec.Ports = []tramv1alpha1.EnginePort{{Name: otlpHTTP, Port: 8080}}
	_, err = engines.BuildResources(collision, "tram", managerRole, nil)
	require.ErrorIs(t, err, engines.ErrInvalidEngine)
	require.ErrorContains(t, err, "http and otlp-http both use 8080")

	pprof := testEngine()
	pprof.Spec.Runtime.Pprof = &tramv1alpha1.PprofSpec{Port: 50051}
	_, err = engines.BuildResources(pprof, "tram", managerRole, nil)
	require.ErrorIs(t, err, engines.ErrInvalidEngine)

	admin := testEngine()
	admin.Spec.Ports = []tramv1alpha1.EnginePort{{Name: otlpHTTP, Port: 50052}}
	_, err = engines.BuildResources(admin, "tram", managerRole, nil)
	require.ErrorIs(t, err, engines.ErrInvalidEngine)
	require.ErrorContains(t, err, "admin and otlp-http both use 50052")
}

func TestOwnsReplicas(t *testing.T) {
	t.Parallel()

	deployment := &appsv1.Deployment{}
	deployment.ManagedFields = []metav1.ManagedFieldsEntry{
		{
			Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate,
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:replicas":{}}}`),
		},
		{
			Manager: engines.FieldOwner, Operation: metav1.ManagedFieldsOperationApply,
			FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:template":{}}}`),
		},
	}
	assert.False(t, engines.OwnsReplicas(deployment, engines.FieldOwner))

	deployment.ManagedFields[1].FieldsV1 = metav1.NewFieldsV1(`{"f:spec":{"f:replicas":{},"f:template":{}}}`)
	assert.True(t, engines.OwnsReplicas(deployment, engines.FieldOwner))
}
