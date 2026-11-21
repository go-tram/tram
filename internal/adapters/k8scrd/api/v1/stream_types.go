// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type Spec struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Spec is the component specific configuration. It is passed through
	// unchanged, so numbers, booleans and nested objects keep their types.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Spec *runtime.RawExtension `json:"spec,omitempty"`
}

type ReceiverSpec struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Spec is the component specific configuration. It is passed through
	// unchanged, so numbers, booleans and nested objects keep their types.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Type=object
	Spec *runtime.RawExtension `json:"spec,omitempty"`
	From string                `json:"from"`
}

// EngineReference refers to an Engine in the namespace of the Stream.
type EngineReference struct {
	// Name of the Engine.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=51
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
}

type StreamSpec struct {
	// EngineRefs are the Engines that run the stream.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	EngineRefs []EngineReference `json:"engineRefs,omitempty"`

	Source Spec           `json:"source"`
	Stages []ReceiverSpec `json:"stages,omitempty"`
	Sinks  []ReceiverSpec `json:"sinks"`
	// Metrics chooses the metrics the stream records; unset fields keep their
	// defaults.
	// +optional
	Metrics *MetricsSpec `json:"metrics,omitempty"`

	// Suspended keeps the stream defined but does not run its source: no
	// ports are opened and no consumers are attached. Used to pause a
	// stream, e.g. the writer of a buffer while its queue is drained.
	// +optional
	Suspended *bool `json:"suspended,omitempty"`
}

type MetricsSpec struct {
	// Records covers tram.node.records and tram.node.records.out.
	// +optional
	Records *NodeMetricsSpec `json:"records,omitempty"`
	// Duration covers tram.node.duration.
	// +optional
	Duration *NodeMetricsSpec `json:"duration,omitempty"`
	// RequestSize covers tram.source.request.size. Defaults to true.
	// +optional
	RequestSize *bool `json:"request_size,omitempty"`
}

// NodeMetricsSpec enables a metric per kind of node.
type NodeMetricsSpec struct {
	// Defaults to true.
	// +optional
	Source *bool `json:"source,omitempty"`
	// Defaults to false.
	// +optional
	Stages *bool `json:"stages,omitempty"`
	// Defaults to true.
	// +optional
	Sinks *bool `json:"sinks,omitempty"`
}

// ConditionAccepted is the condition type of a Stream's engine entry that
// tells whether the referenced Engine exists.
const ConditionAccepted = "Accepted"

// ConditionStorageAvailable is the condition type of a Stream's engine entry
// that tells whether the Engine's instances have the storage the Stream's
// buffer sink or source needs. Without it, the Stream does not run there.
const ConditionStorageAvailable = "StorageAvailable"

type StreamStatus struct {
	// Active tells whether the stream runs.
	// +optional
	Active bool `json:"active"`
	// Engines reports, per entry of spec.engineRefs, whether the Engine
	// accepts the Stream.
	// +optional
	// +listType=map
	// +listMapKey=name
	Engines []StreamEngineStatus `json:"engines,omitempty"`
}

// StreamEngineStatus is the status of a Stream on one of its Engines.
type StreamEngineStatus struct {
	// Name of the Engine.
	Name string `json:"name"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Active",type=boolean,JSONPath=`.status.active`
// +kubebuilder:printcolumn:name="Suspended",type=boolean,JSONPath=`.spec.suspended`,description="The source does not run; no ports are opened and no consumers are attached"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type Stream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StreamSpec   `json:"spec,omitempty"`
	Status StreamStatus `json:"status,omitempty"`
}

// StreamList is a list of Stream resources.
// +kubebuilder:object:root=true
type StreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Stream `json:"items"`
}

//nolint:gochecknoinits // Registers the types with SchemeBuilder, as Kubebuilder expects.
func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Stream{}, &StreamList{})
		return nil
	})
}
