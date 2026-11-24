// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Condition types of an Engine.
const (
	// ConditionReconciled tells whether the manager applied the latest spec.
	ConditionReconciled = "Reconciled"
	// ConditionReady tells whether the Engine's Deployment or StatefulSet is
	// available.
	ConditionReady = "Ready"
	// ConditionMigrating is true while the instances move from a Deployment to
	// a StatefulSet or back, after spec.storage was added or removed.
	ConditionMigrating = "Migrating"
	// ConditionDraining is true while volumes without an instance hold buffer
	// files that drain Jobs still deliver.
	ConditionDraining = "Draining"
)

// Workloads that run an Engine's instances.
const (
	WorkloadDeployment  = "Deployment"
	WorkloadStatefulSet = "StatefulSet"
)

// DiscardBuffersAnnotation lets the manager delete an Engine that carries it
// with the value "true" at once, discarding what buffers still hold instead of
// waiting for drains or for their max_age.
const DiscardBuffersAnnotation = "tram.io/discard-buffers"

// EngineSpec describes a group of Tram instances. The manager runs them as a
// Deployment; every instance runs every Stream whose spec.engineRefs names the
// Engine.
type EngineSpec struct {
	// Replicas is the number of Tram instances while autoscaling is disabled.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Image is the Tram image of the instances. Defaults to the manager's
	// --engine-image.
	// +optional
	Image string `json:"image,omitempty"`

	// +optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Resources of the tram container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Autoscaling lets a HorizontalPodAutoscaler set the number of instances
	// by their CPU utilization.
	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`

	// DisruptionBudget limits how many instances voluntary disruptions, like
	// node drains, may take down at a time.
	// +optional
	DisruptionBudget *DisruptionBudgetSpec `json:"disruptionBudget,omitempty"`

	// Ports are the ports the HTTP sources of the Engine's Streams may listen
	// on. The Engine's Service exposes them.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Ports []EnginePort `json:"ports,omitempty"`

	// +optional
	Service ServiceSpec `json:"service,omitempty"`

	// Runtime holds the options of the Tram process.
	// +optional
	Runtime RuntimeSpec `json:"runtime,omitempty"`

	// PodTemplate holds pod-level settings of the instances. The tram
	// container is configured by the other fields.
	// +optional
	PodTemplate PodTemplate `json:"podTemplate,omitempty"`

	// Storage gives every instance a volume of its own for the files of the
	// buffer stages of its Streams. With it, the instances run as a
	// StatefulSet; without it, as a Deployment, and buffers pass events
	// through without storing them. Adding or removing it migrates the
	// instances; volumes without an instance are drained before they are
	// deleted.
	// +optional
	Storage *StorageSpec `json:"storage,omitempty"`
}

// StorageSpec is the volume of every instance of an Engine.
// +kubebuilder:validation:XValidation:rule="self.size == oldSelf.size",message="size cannot be changed; resize the volumes by hand"
// +kubebuilder:validation:XValidation:rule="has(self.storageClassName) == has(oldSelf.storageClassName) && (!has(self.storageClassName) || self.storageClassName == oldSelf.storageClassName)",message="storageClassName cannot be changed"
type StorageSpec struct {
	// Size of each instance's volume. It holds the buffer files, whose
	// payloads max_bytes limits, plus their write-ahead logs and free pages.
	Size resource.Quantity `json:"size"`
	// StorageClassName of the volumes; the cluster's default class if unset.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
	// MountPath of the volume in the instances, the data directory of their
	// buffers. Defaults to /var/lib/tram.
	// +optional
	// +kubebuilder:validation:Pattern=`^/[A-Za-z0-9._/-]*$`
	MountPath string `json:"mountPath,omitempty"`
	// DrainResources are the resources of the drain Jobs that deliver what
	// volumes without an instance hold. They run no sources, so they need
	// less than an instance. Defaults to requests of 50m CPU and 128Mi
	// memory, and the limits of the instances.
	// +optional
	DrainResources *corev1.ResourceRequirements `json:"drainResources,omitempty"`
}

// AutoscalingSpec configures the HorizontalPodAutoscaler of an Engine.
// +kubebuilder:validation:XValidation:rule="!self.enabled || (has(self.minReplicas) && has(self.maxReplicas))",message="minReplicas and maxReplicas are required while autoscaling is enabled"
// +kubebuilder:validation:XValidation:rule="!has(self.minReplicas) || !has(self.maxReplicas) || self.minReplicas <= self.maxReplicas",message="minReplicas must not exceed maxReplicas"
type AutoscalingSpec struct {
	// Enabled creates the HorizontalPodAutoscaler; while it is enabled,
	// spec.replicas is not used.
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	MinReplicas *int32 `json:"minReplicas,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`
	// TargetCPUUtilizationPercentage is the average CPU utilization, relative
	// to the CPU requests, the autoscaler aims at. Defaults to 80.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TargetCPUUtilizationPercentage *int32 `json:"targetCPUUtilizationPercentage,omitempty"`
}

// DisruptionBudgetSpec configures the PodDisruptionBudget of an Engine.
// +kubebuilder:validation:XValidation:rule="has(self.minAvailable) != has(self.maxUnavailable)",message="set exactly one of minAvailable and maxUnavailable"
type DisruptionBudgetSpec struct {
	// +optional
	// +kubebuilder:validation:XIntOrString
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

const (
	// EngineLabel labels the pods of an Engine with its name.
	EngineLabel = "tram.io/engine"
	// DefaultAdminPort is the port of the admin API of an Engine's pods
	// without spec.runtime.api.adminPort.
	DefaultAdminPort = 50052
)

// EnginePort is a port the HTTP sources of the Engine's Streams may listen on.
// +kubebuilder:validation:XValidation:rule="!(self.name in ['http', 'grpc', 'admin', 'pprof'])",message="http, grpc, admin and pprof are reserved for the ports of the Tram process"
type EnginePort struct {
	// Name of the port in the container and the Service.
	// +kubebuilder:validation:MaxLength=15
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// ServiceSpec configures the Service of an Engine.
type ServiceSpec struct {
	// Type of the Service. Defaults to ClusterIP.
	// +optional
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	Type corev1.ServiceType `json:"type,omitempty"`
}

// RuntimeSpec maps to flags of `tram server`.
type RuntimeSpec struct {
	// LogLevel is --log-level. Defaults to info.
	// +optional
	// +kubebuilder:validation:Enum=debug;info;warn;error
	LogLevel string `json:"logLevel,omitempty"`
	// +optional
	API APISpec `json:"api,omitempty"`
	// Pprof serves the Go profiles on the port, bound to the pod's localhost:
	// reach it with kubectl port-forward. Disabled without it.
	// +optional
	Pprof *PprofSpec `json:"pprof,omitempty"`
}

// APISpec sets the ports of the Tram APIs.
type APISpec struct {
	// HTTPPort is the port of --http-endpoint, which also serves /metrics.
	// Defaults to 8080.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	HTTPPort int32 `json:"httpPort,omitempty"`
	// GRPCPort is the port of --grpc-endpoint. Defaults to 50051.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	GRPCPort int32 `json:"grpcPort,omitempty"`
	// AdminPort is the port of --admin-endpoint, the admin API, e.g. for
	// tram tap. It is bound to the pod's localhost and not exposed: reach it
	// with kubectl port-forward. Defaults to 50052.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	AdminPort int32 `json:"adminPort,omitempty"`
}

// PprofSpec sets the port of --pprof-endpoint.
type PprofSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// PodTemplate holds the pod-level settings of an Engine's instances, with the
// names of a PodTemplateSpec.
type PodTemplate struct {
	// +optional
	Metadata PodMetadata `json:"metadata,omitempty"`
	// +optional
	Spec PodSpec `json:"spec,omitempty"`
}

// PodMetadata are labels and annotations added to the instances' pods.
// +kubebuilder:validation:XValidation:rule="!has(self.labels) || !self.labels.exists(k, k == 'tram.io/engine' || k.startsWith('app.kubernetes.io/'))",message="tram.io/engine and app.kubernetes.io/ labels are set by the manager"
type PodMetadata struct {
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PodSpec holds the pod-level fields of a PodSpec an Engine may set.
type PodSpec struct {
	// +optional
	// +kubebuilder:validation:Minimum=0
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	// +listType=atomic
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`
	// +optional
	// +listType=atomic
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`
	// +optional
	// +listType=atomic
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

// EngineStatus is the observed state of an Engine.
type EngineStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Replicas is the number of instances of the Deployment.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
	// ReadyReplicas is the number of ready instances.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// Streams is the number of Streams whose spec.engineRefs name the Engine.
	// +optional
	Streams int32 `json:"streams,omitempty"`
	// Selector selects the instances' pods, for the scale subresource.
	// +optional
	Selector string `json:"selector,omitempty"`
	// Workload runs the instances: Deployment or StatefulSet. While the
	// Engine migrates, the one it migrates to.
	// +optional
	Workload string `json:"workload,omitempty"`
	// Volumes is the number of volumes of the Engine, with an instance or
	// draining.
	// +optional
	Volumes int32 `json:"volumes,omitempty"`
	// DrainingVolumes is the number of volumes without an instance whose
	// buffer files are being drained.
	// +optional
	DrainingVolumes int32 `json:"drainingVolumes,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Engine runs Tram instances for the Streams that refer to it.
// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Streams",type=integer,JSONPath=`.status.streams`
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.status.workload`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 51 && self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="the name must be a DNS-1123 label of at most 51 characters"
type Engine struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EngineSpec   `json:"spec,omitempty"`
	Status EngineStatus `json:"status,omitempty"`
}

// EngineList is a list of Engine resources.
// +kubebuilder:object:root=true
type EngineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Engine `json:"items"`
}

//nolint:gochecknoinits // Registers the types with SchemeBuilder, as Kubebuilder expects.
func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Engine{}, &EngineList{})
		return nil
	})
}
