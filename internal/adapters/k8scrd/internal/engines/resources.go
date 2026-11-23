// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	autoscalingv2ac "k8s.io/client-go/applyconfigurations/autoscaling/v2"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	policyv1ac "k8s.io/client-go/applyconfigurations/policy/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"

	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

const (
	// EngineLabel carries the Engine's name on every resource the manager
	// generates for it, and selects the Engine's pods.
	EngineLabel = "tram.io/engine"

	// ManagedByLabel marks the resources the manager generates; the manager
	// caches only those.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	// FieldOwner is the field manager of the manager's server-side applies,
	// and the value of ManagedByLabel.
	FieldOwner = "tram-manager"

	containerName = "tram"

	defaultHTTPPort         = 8080
	defaultGRPCPort         = 50051
	defaultTargetCPUPercent = 80
	defaultLogLevel         = "info"

	// VolumeLabel marks the volumes of an Engine's instances.
	VolumeLabel = "tram.io/volume"
	// DrainLabel carries the Engine's name on its drain Jobs, which must not
	// carry EngineLabel: the Service and the disruption budget select it.
	DrainLabel = "tram.io/drain"
	// OrdinalLabel carries the ordinal of the volume a drain Job drains.
	OrdinalLabel = "tram.io/ordinal"
	// StorageLabel marks the pods of instances with storage. Once they are
	// ready, the Service of an Engine with storage selects only them, so no
	// request for a Stream with a buffer reaches an instance without one.
	StorageLabel = "tram.io/storage"
	// labelTrue is the value of boolean labels and annotations.
	labelTrue = "true"

	// volumeName is the name of the volume claim template; the claims are
	// called <volumeName>-<StatefulSet>-<ordinal>.
	volumeName       = "data"
	defaultMountPath = "/var/lib/tram"
	// The root file system of the containers is read-only; SQLite, the
	// driver of buffers, writes its temporary files to an empty directory
	// mounted at tmpMountPath instead.
	tmpVolumeName  = "tmp"
	tmpMountPath   = "/tmp"
	drainMountPath = "/data"
	// nonRootGroup is the group of the image's nonroot user; volumes are made
	// writable for it.
	nonRootGroup      = 65532
	drainBackoffLimit = 10
)

// ErrInvalidEngine is the reason of an Engine whose spec cannot be run, e.g.
// one with colliding ports.
var ErrInvalidEngine = errors.New("invalid Engine")

// ResourceName names the resources the manager generates for engine.
func ResourceName(engine string) string {
	return "tram-engine-" + engine
}

// resources are the apply configurations of an Engine's resources. HPA and
// PDB are nil when the Engine does not ask for them.
type resources struct {
	serviceAccount *corev1ac.ServiceAccountApplyConfiguration
	roleBinding    *rbacv1ac.RoleBindingApplyConfiguration
	// Exactly one of deployment and statefulSet is set: a StatefulSet if the
	// Engine has storage.
	deployment  *appsv1ac.DeploymentApplyConfiguration
	statefulSet *appsv1ac.StatefulSetApplyConfiguration
	service     *corev1ac.ServiceApplyConfiguration
	hpa         *autoscalingv2ac.HorizontalPodAutoscalerApplyConfiguration
	pdb         *policyv1ac.PodDisruptionBudgetApplyConfiguration
}

// buildParams are the inputs of buildResources besides the Engine.
type buildParams struct {
	// image runs when the Engine sets none.
	image string
	// clusterRole is bound to the Engine's service account in its namespace.
	clusterRole string
	// replicas of the Deployment; nil leaves them to the autoscaler.
	replicas *int32
	// storageOnly narrows the Service to the instances with storage.
	storageOnly bool
}

// buildResources returns the resources that run engine. It fails with
// ErrInvalidEngine when the Engine has no image or its ports collide.
func buildResources(engine *tramv1alpha1.Engine, params buildParams) (resources, error) {
	image := engine.Spec.Image
	if image == "" {
		image = params.image
	}
	if image == "" {
		return resources{}, fmt.Errorf("%w: no image; set spec.image or the manager's --engine-image", ErrInvalidEngine)
	}
	ports, err := portsOf(engine)
	if err != nil {
		return resources{}, err
	}

	name, ns := ResourceName(engine.Name), engine.Namespace
	labels := managedLabels(engine.Name)
	owner := ownerOf(engine)

	podSpec, err := podSpecOf(engine, image, name, ports)
	if err != nil {
		return resources{}, err
	}
	podLabels := maps.Clone(engine.Spec.PodTemplate.Metadata.Labels)
	if podLabels == nil {
		podLabels = map[string]string{}
	}
	if engine.Spec.Storage != nil {
		podLabels[StorageLabel] = labelTrue
	}
	maps.Copy(podLabels, labels)

	template := corev1ac.PodTemplateSpec().
		WithLabels(podLabels).
		WithAnnotations(engine.Spec.PodTemplate.Metadata.Annotations).
		WithSpec(podSpec)
	selector := metav1ac.LabelSelector().WithMatchLabels(map[string]string{EngineLabel: engine.Name})

	res := resources{
		serviceAccount: corev1ac.ServiceAccount(name, ns).WithLabels(labels).WithOwnerReferences(owner),
		roleBinding: rbacv1ac.RoleBinding(name, ns).WithLabels(labels).WithOwnerReferences(owner).
			WithRoleRef(rbacv1ac.RoleRef().
				WithAPIGroup(rbacv1.GroupName).WithKind("ClusterRole").WithName(params.clusterRole)).
			WithSubjects(rbacv1ac.Subject().WithKind(rbacv1.ServiceAccountKind).WithName(name).WithNamespace(ns)),
		service: serviceOf(engine, name, labels, owner, ports, params.storageOnly),
		hpa:     hpaOf(engine, name, labels, owner),
		pdb:     pdbOf(engine, name, labels, owner),
	}
	if storage := engine.Spec.Storage; storage != nil {
		spec := appsv1ac.StatefulSetSpec().
			WithSelector(selector).
			WithTemplate(template).
			WithServiceName(name).
			WithPodManagementPolicy(appsv1.ParallelPodManagement).
			WithPersistentVolumeClaimRetentionPolicy(appsv1ac.StatefulSetPersistentVolumeClaimRetentionPolicy().
				WithWhenDeleted(appsv1.RetainPersistentVolumeClaimRetentionPolicyType).
				WithWhenScaled(appsv1.RetainPersistentVolumeClaimRetentionPolicyType)).
			WithVolumeClaimTemplates(claimTemplateOf(engine.Name, storage))
		if params.replicas != nil {
			spec.WithReplicas(*params.replicas)
		}
		res.statefulSet = appsv1ac.StatefulSet(name, ns).WithLabels(labels).WithOwnerReferences(owner).WithSpec(spec)
	} else {
		spec := appsv1ac.DeploymentSpec().WithSelector(selector).WithTemplate(template)
		if params.replicas != nil {
			spec.WithReplicas(*params.replicas)
		}
		res.deployment = appsv1ac.Deployment(name, ns).WithLabels(labels).WithOwnerReferences(owner).WithSpec(spec)
	}
	return res, nil
}

// workloadKind is the kind of workload that runs engine.
func workloadKind(engine *tramv1alpha1.Engine) string {
	if engine.Spec.Storage != nil {
		return tramv1alpha1.WorkloadStatefulSet
	}
	return tramv1alpha1.WorkloadDeployment
}

// claimTemplateOf is the volume of every instance. The claims carry the
// managed labels, so the manager's cache holds them.
func claimTemplateOf(
	engine string,
	storage *tramv1alpha1.StorageSpec,
) *corev1ac.PersistentVolumeClaimApplyConfiguration {
	labels := managedLabels(engine)
	labels[VolumeLabel] = volumeName
	requests := make(corev1.ResourceList)
	requests[corev1.ResourceStorage] = storage.Size
	spec := corev1ac.PersistentVolumeClaimSpec().
		WithAccessModes(corev1.ReadWriteOnce).
		WithResources(corev1ac.VolumeResourceRequirements().WithRequests(requests))
	if storage.StorageClassName != nil {
		spec.WithStorageClassName(*storage.StorageClassName)
	}
	return corev1ac.PersistentVolumeClaim(volumeName, "").WithLabels(labels).WithSpec(spec)
}

// mountPath is where the instances of engine mount their volume.
func mountPath(storage *tramv1alpha1.StorageSpec) string {
	if storage.MountPath != "" {
		return storage.MountPath
	}
	return defaultMountPath
}

// gracePeriod is the termination grace period of the instances in seconds,
// and whether one is set at all.
func gracePeriod(engine *tramv1alpha1.Engine) (int64, bool) {
	if p := engine.Spec.PodTemplate.Spec.TerminationGracePeriodSeconds; p != nil {
		return *p, true
	}
	return 0, false
}

// ClaimName is the name of the volume claim of the instance with ordinal.
func ClaimName(engine string, ordinal int) string {
	return volumeName + "-" + ResourceName(engine) + "-" + strconv.Itoa(ordinal)
}

// claimOrdinal returns the ordinal of a volume claim of engine, and false
// for other claims.
func claimOrdinal(engine, claim string) (int, bool) {
	prefix := volumeName + "-" + ResourceName(engine) + "-"
	rest, ok := strings.CutPrefix(claim, prefix)
	if !ok {
		return 0, false
	}
	ordinal, err := strconv.Atoi(rest)
	if err != nil || ordinal < 0 || strconv.Itoa(ordinal) != rest {
		return 0, false
	}
	return ordinal, true
}

// managedLabels are the labels of every generated resource and pod.
func managedLabels(engine string) map[string]string {
	return map[string]string{
		EngineLabel:                   engine,
		"app.kubernetes.io/name":      "tram",
		"app.kubernetes.io/instance":  engine,
		"app.kubernetes.io/component": "engine",
		ManagedByLabel:                FieldOwner,
	}
}

func ownerOf(engine *tramv1alpha1.Engine) *metav1ac.OwnerReferenceApplyConfiguration {
	return metav1ac.OwnerReference().
		WithAPIVersion(tramv1alpha1.GroupVersion.String()).
		WithKind("Engine").
		WithName(engine.Name).
		WithUID(engine.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)
}

// port is a port of the tram container, exposed by the Service unless
// internal.
type port struct {
	name     string
	number   int32
	internal bool
}

// portsOf returns the ports of the tram container: the APIs, pprof and the
// Engine's ports. Two ports with the same number fail.
func portsOf(engine *tramv1alpha1.Engine) ([]port, error) {
	api := engine.Spec.Runtime.API
	ports := []port{
		{name: "http", number: orDefault(api.HTTPPort, defaultHTTPPort)},
		{name: "grpc", number: orDefault(api.GRPCPort, defaultGRPCPort)},
	}
	if pprof := engine.Spec.Runtime.Pprof; pprof != nil {
		ports = append(ports, port{name: "pprof", number: pprof.Port, internal: true})
	}
	for _, p := range engine.Spec.Ports {
		ports = append(ports, port{name: p.Name, number: p.Port})
	}

	seen := make(map[int32]string, len(ports))
	for _, p := range ports {
		if other, ok := seen[p.number]; ok {
			return nil, fmt.Errorf("%w: ports %s and %s both use %d", ErrInvalidEngine, other, p.name, p.number)
		}
		seen[p.number] = p.name
	}
	return ports, nil
}

func orDefault(value, def int32) int32 {
	if value == 0 {
		return def
	}
	return value
}

// args are the arguments of `tram`, the image's entrypoint. The APIs listen on
// all interfaces to be reachable through the Service; pprof only on the pod's
// localhost, reachable with kubectl port-forward.
func args(engine *tramv1alpha1.Engine, ports []port) []string {
	logLevel := engine.Spec.Runtime.LogLevel
	if logLevel == "" {
		logLevel = defaultLogLevel
	}
	out := []string{
		"server",
		"--log-level=" + logLevel,
		"--http-endpoint=0.0.0.0:" + strconv.Itoa(int(ports[0].number)),
		"--grpc-endpoint=0.0.0.0:" + strconv.Itoa(int(ports[1].number)),
	}
	if pprof := engine.Spec.Runtime.Pprof; pprof != nil {
		out = append(out, "--pprof-endpoint=127.0.0.1:"+strconv.Itoa(int(pprof.Port)))
	}
	if storage := engine.Spec.Storage; storage != nil {
		out = append(out, "--data-dir="+mountPath(storage))
	}
	return append(out,
		"--enable-k8s-operator",
		"--engine="+engine.Name,
		"--watch-namespace="+engine.Namespace,
	)
}

func podSpecOf(
	engine *tramv1alpha1.Engine,
	image, serviceAccount string,
	ports []port,
) (*corev1ac.PodSpecApplyConfiguration, error) {
	container, err := containerOf(engine, image, ports)
	if err != nil {
		return nil, err
	}

	pod := engine.Spec.PodTemplate.Spec
	spec := corev1ac.PodSpec().
		WithServiceAccountName(serviceAccount).
		WithContainers(container).
		WithVolumes(corev1ac.Volume().WithName(tmpVolumeName).WithEmptyDir(corev1ac.EmptyDirVolumeSource()))
	if seconds, ok := gracePeriod(engine); ok {
		spec.WithTerminationGracePeriodSeconds(seconds)
	}
	if pod.PriorityClassName != "" {
		spec.WithPriorityClassName(pod.PriorityClassName)
	}
	if len(pod.NodeSelector) > 0 {
		spec.WithNodeSelector(pod.NodeSelector)
	}
	if err = applyPodScheduling(pod, spec); err != nil {
		return nil, err
	}
	if engine.Spec.Storage != nil {
		writableVolumes(spec)
	}
	return spec, nil
}

// writableVolumes makes volumes writable for the image's nonroot user,
// unless the Engine chose an fsGroup itself.
func writableVolumes(spec *corev1ac.PodSpecApplyConfiguration) {
	if spec.SecurityContext == nil {
		spec.SecurityContext = corev1ac.PodSecurityContext()
	}
	if spec.SecurityContext.FSGroup == nil {
		spec.SecurityContext.WithFSGroup(nonRootGroup)
	}
}

func containerOf(
	engine *tramv1alpha1.Engine,
	image string,
	ports []port,
) (*corev1ac.ContainerApplyConfiguration, error) {
	container := corev1ac.Container().
		WithName(containerName).
		WithImage(image).
		WithArgs(args(engine, ports)...).
		WithReadinessProbe(corev1ac.Probe().
			WithTCPSocket(corev1ac.TCPSocketAction().WithPort(intstr.FromString("http")))).
		WithSecurityContext(corev1ac.SecurityContext().
			WithRunAsNonRoot(true).
			WithReadOnlyRootFilesystem(true).
			WithAllowPrivilegeEscalation(false).
			WithCapabilities(corev1ac.Capabilities().WithDrop("ALL")).
			WithSeccompProfile(corev1ac.SeccompProfile().WithType(corev1.SeccompProfileTypeRuntimeDefault)))
	if engine.Spec.ImagePullPolicy != "" {
		container.WithImagePullPolicy(engine.Spec.ImagePullPolicy)
	}
	for _, p := range ports {
		if p.internal {
			continue
		}
		container.WithPorts(corev1ac.ContainerPort().
			WithName(p.name).WithContainerPort(p.number).WithProtocol(corev1.ProtocolTCP))
	}
	if storage := engine.Spec.Storage; storage != nil {
		container.WithVolumeMounts(corev1ac.VolumeMount().WithName(volumeName).WithMountPath(mountPath(storage)))
	}
	container.WithVolumeMounts(corev1ac.VolumeMount().WithName(tmpVolumeName).WithMountPath(tmpMountPath))
	resources, err := convert[corev1ac.ResourceRequirementsApplyConfiguration](engine.Spec.Resources)
	if err != nil {
		return nil, err
	}
	return container.WithResources(resources), nil
}

// applyPodScheduling copies the fields of the pod template that need a
// conversion into apply configurations.
func applyPodScheduling(pod tramv1alpha1.PodSpec, spec *corev1ac.PodSpecApplyConfiguration) error {
	if err := convertInto(pod.Tolerations, &spec.Tolerations); err != nil {
		return err
	}
	if err := convertInto(pod.TopologySpreadConstraints, &spec.TopologySpreadConstraints); err != nil {
		return err
	}
	if err := convertInto(pod.ImagePullSecrets, &spec.ImagePullSecrets); err != nil {
		return err
	}
	var err error
	if pod.Affinity != nil {
		if spec.Affinity, err = convert[corev1ac.AffinityApplyConfiguration](pod.Affinity); err != nil {
			return err
		}
	}
	if pod.SecurityContext != nil {
		if spec.SecurityContext, err = convert[corev1ac.PodSecurityContextApplyConfiguration](
			pod.SecurityContext); err != nil {
			return err
		}
	}
	return nil
}

func serviceOf(
	engine *tramv1alpha1.Engine,
	name string,
	labels map[string]string,
	owner *metav1ac.OwnerReferenceApplyConfiguration,
	ports []port,
	storageOnly bool,
) *corev1ac.ServiceApplyConfiguration {
	serviceType := engine.Spec.Service.Type
	if serviceType == "" {
		serviceType = corev1.ServiceTypeClusterIP
	}
	spec := corev1ac.ServiceSpec().
		WithType(serviceType).
		WithSelector(serviceSelector(engine.Name, storageOnly))
	for _, p := range ports {
		if p.internal {
			continue
		}
		spec.WithPorts(corev1ac.ServicePort().
			WithName(p.name).WithPort(p.number).WithTargetPort(intstr.FromString(p.name)).
			WithProtocol(corev1.ProtocolTCP))
	}
	return corev1ac.Service(name, engine.Namespace).WithLabels(labels).WithOwnerReferences(owner).WithSpec(spec)
}

// serviceSelector selects the Engine's instances, or only those with storage.
func serviceSelector(engine string, storageOnly bool) map[string]string {
	selector := map[string]string{EngineLabel: engine}
	if storageOnly {
		selector[StorageLabel] = labelTrue
	}
	return selector
}

func hpaOf(
	engine *tramv1alpha1.Engine,
	name string,
	labels map[string]string,
	owner *metav1ac.OwnerReferenceApplyConfiguration,
) *autoscalingv2ac.HorizontalPodAutoscalerApplyConfiguration {
	if !autoscaled(engine) {
		return nil
	}
	scaling := engine.Spec.Autoscaling
	target := int32(defaultTargetCPUPercent)
	if scaling.TargetCPUUtilizationPercentage != nil {
		target = *scaling.TargetCPUUtilizationPercentage
	}
	return autoscalingv2ac.HorizontalPodAutoscaler(name, engine.Namespace).
		WithLabels(labels).WithOwnerReferences(owner).
		WithSpec(autoscalingv2ac.HorizontalPodAutoscalerSpec().
			WithScaleTargetRef(autoscalingv2ac.CrossVersionObjectReference().
				WithAPIVersion(appsv1.SchemeGroupVersion.String()).WithKind(workloadKind(engine)).WithName(name)).
			WithMinReplicas(*scaling.MinReplicas).
			WithMaxReplicas(*scaling.MaxReplicas).
			WithMetrics(autoscalingv2ac.MetricSpec().
				WithType(autoscalingv2.ResourceMetricSourceType).
				WithResource(autoscalingv2ac.ResourceMetricSource().
					WithName(corev1.ResourceCPU).
					WithTarget(autoscalingv2ac.MetricTarget().
						WithType(autoscalingv2.UtilizationMetricType).
						WithAverageUtilization(target)))))
}

func pdbOf(
	engine *tramv1alpha1.Engine,
	name string,
	labels map[string]string,
	owner *metav1ac.OwnerReferenceApplyConfiguration,
) *policyv1ac.PodDisruptionBudgetApplyConfiguration {
	budget := engine.Spec.DisruptionBudget
	if budget == nil {
		return nil
	}
	spec := policyv1ac.PodDisruptionBudgetSpec().
		WithSelector(metav1ac.LabelSelector().WithMatchLabels(map[string]string{EngineLabel: engine.Name}))
	if budget.MinAvailable != nil {
		spec.WithMinAvailable(*budget.MinAvailable)
	}
	if budget.MaxUnavailable != nil {
		spec.WithMaxUnavailable(*budget.MaxUnavailable)
	}
	return policyv1ac.PodDisruptionBudget(name, engine.Namespace).
		WithLabels(labels).WithOwnerReferences(owner).WithSpec(spec)
}

// autoscaled tells whether a HorizontalPodAutoscaler sets the replicas of
// engine. The CRD requires the bounds while autoscaling is enabled.
func autoscaled(engine *tramv1alpha1.Engine) bool {
	scaling := engine.Spec.Autoscaling
	return scaling != nil && scaling.Enabled && scaling.MinReplicas != nil && scaling.MaxReplicas != nil
}

// convert turns a typed value from the Engine's spec into its apply
// configuration, which has the same JSON form.
func convert[T any](in any) (*T, error) {
	data, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", in, err)
	}
	out := new(T)
	if err = json.Unmarshal(data, out); err != nil {
		return nil, fmt.Errorf("convert %T: %w", in, err)
	}
	return out, nil
}

// convertInto converts a list from the Engine's spec into the list of apply
// configurations out, leaving it nil when in is empty.
func convertInto[In, Out any](in []In, out *[]Out) error {
	if len(in) == 0 {
		return nil
	}
	converted, err := convert[[]Out](in)
	if err != nil {
		return err
	}
	*out = *converted
	return nil
}
