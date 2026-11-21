// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package engines holds the controllers of `tram manager`: it runs every
// Engine as a Deployment of Tram instances, or as a StatefulSet with a volume
// per instance if the Engine has storage for buffers, drains volumes without
// an instance, and assigns Streams to Engines by labels derived from their
// spec.engineRefs, which the instances select. It runs no streams itself.
package engines

import (
	"context"
	"fmt"
	"maps"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

// StreamReconciler keeps the engine.tram.io/<name> labels of every Stream
// equal to its spec.engineRefs, and reports in status.engines whether each
// referenced Engine exists. It owns all labels with that prefix: it adds the
// missing ones and removes those no reference names, including labels set by
// hand.
type StreamReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=io.tram.core,resources=streams,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var stream tramv1.Stream
	if err := r.Get(ctx, req.NamespacedName, &stream); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !stream.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if err := r.syncLabels(ctx, &stream); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.syncStatus(ctx, &stream)
}

func (r *StreamReconciler) syncLabels(ctx context.Context, stream *tramv1.Stream) error {
	labels := tramv1.WithEngineLabels(stream.Labels, stream.Spec.EngineRefs)
	if maps.Equal(labels, stream.Labels) {
		return nil
	}

	// A merge patch of the labels alone: other labels, e.g. set by a user at
	// the same time, are left alone.
	patch := client.MergeFrom(stream.DeepCopy())
	stream.Labels = labels
	if err := r.Patch(ctx, stream, patch); err != nil {
		return fmt.Errorf("patch engine labels: %w", err)
	}

	logf.FromContext(ctx).Info("Updated engine labels of Stream",
		"stream", client.ObjectKeyFromObject(stream), "engines", engineNames(stream.Spec.EngineRefs))
	return nil
}

// syncStatus writes an entry per engine reference to status.engines, with an
// Accepted condition that tells whether the Engine exists. Only the manager
// writes status.engines; the instances write status.mounted.
func (r *StreamReconciler) syncStatus(ctx context.Context, stream *tramv1.Stream) error {
	engines := make([]tramv1.StreamEngineStatus, 0, len(stream.Spec.EngineRefs))
	for _, ref := range stream.Spec.EngineRefs {
		entry := tramv1.StreamEngineStatus{Name: ref.Name}
		for _, old := range stream.Status.Engines {
			if old.Name == ref.Name {
				entry.Conditions = append(entry.Conditions, old.Conditions...)
			}
		}

		accepted, engine, err := r.accepted(ctx, stream, ref.Name)
		if err != nil {
			return err
		}
		meta.SetStatusCondition(&entry.Conditions, accepted)
		if usesBuffer(stream) {
			meta.SetStatusCondition(&entry.Conditions, storageAvailable(stream, engine))
		} else {
			meta.RemoveStatusCondition(&entry.Conditions, tramv1.ConditionStorageAvailable)
		}
		engines = append(engines, entry)
	}

	if equality.Semantic.DeepEqual(engines, stream.Status.Engines) ||
		(len(engines) == 0 && len(stream.Status.Engines) == 0) {
		return nil
	}
	patch := client.MergeFrom(stream.DeepCopy())
	stream.Status.Engines = engines
	if err := r.Status().Patch(ctx, stream, patch); err != nil {
		return fmt.Errorf("patch engine status: %w", err)
	}
	return nil
}

// accepted returns the Accepted condition of a Stream on an Engine, and the
// Engine if it exists.
func (r *StreamReconciler) accepted(
	ctx context.Context,
	stream *tramv1.Stream,
	name string,
) (metav1.Condition, *tramv1alpha1.Engine, error) {
	condition := metav1.Condition{Type: tramv1.ConditionAccepted, ObservedGeneration: stream.Generation}

	var engine tramv1alpha1.Engine
	err := r.Get(ctx, types.NamespacedName{Namespace: stream.Namespace, Name: name}, &engine)
	switch {
	case apierrors.IsNotFound(err):
		condition.Status = metav1.ConditionFalse
		condition.Reason = "EngineNotFound"
		condition.Message = fmt.Sprintf("Engine %s does not exist in namespace %s", name, stream.Namespace)
	case err != nil:
		return metav1.Condition{}, nil, fmt.Errorf("get engine %s: %w", name, err)
	case !engine.DeletionTimestamp.IsZero():
		condition.Status = metav1.ConditionFalse
		condition.Reason = "EngineDeleting"
		condition.Message = fmt.Sprintf("Engine %s is being deleted", name)
	default:
		condition.Status = metav1.ConditionTrue
		condition.Reason = "EngineFound"
		condition.Message = fmt.Sprintf("Engine %s runs the Stream", name)
	}
	if condition.Status != metav1.ConditionTrue {
		return condition, nil, nil
	}
	return condition, &engine, nil
}

// storageAvailable tells whether an Engine can run a Stream with a buffer
// sink or source: its instances need a data directory, which only an Engine
// with storage gives them, once they run as a StatefulSet. Elsewhere such a
// Stream does not run.
func storageAvailable(stream *tramv1.Stream, engine *tramv1alpha1.Engine) metav1.Condition {
	condition := metav1.Condition{
		Type: tramv1.ConditionStorageAvailable, Status: metav1.ConditionFalse, ObservedGeneration: stream.Generation,
	}
	switch {
	case engine == nil:
		condition.Reason, condition.Message = "EngineNotRunning", "The Engine does not run the Stream"
	case engine.Spec.Storage == nil:
		condition.Reason = "EngineHasNoStorage"
		condition.Message = fmt.Sprintf(
			"Engine %s has no storage, so the Stream's buffer cannot run; set spec.storage on the Engine", engine.Name)
	case engine.Status.Workload != tramv1alpha1.WorkloadStatefulSet ||
		meta.IsStatusConditionTrue(engine.Status.Conditions, tramv1alpha1.ConditionMigrating):
		condition.Reason = "StorageMigrating"
		condition.Message = fmt.Sprintf(
			"Engine %s moves to instances with storage; the Stream runs once they are ready",
			engine.Name,
		)
	default:
		condition.Status, condition.Reason = metav1.ConditionTrue, "StorageAvailable"
		condition.Message = fmt.Sprintf("The instances of Engine %s have storage for the Stream's buffer", engine.Name)
	}
	return condition
}

// SetupWithManager reconciles a Stream when its spec changes, e.g. its
// engineRefs, when its labels change, so engine labels edited by hand are
// restored, and when an Engine it refers to is created or deleted.
func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tramv1.Stream{}, builder.WithPredicates(predicate.Or[client.Object](
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
		))).
		Watches(&tramv1alpha1.Engine{}, handler.EnqueueRequestsFromMapFunc(r.streamsOf),
			builder.WithPredicates(engineLifecycle())).
		Named("stream-engines").
		Complete(r)
}

// streamsOf returns the Streams that refer to engine, by their engine label.
func (r *StreamReconciler) streamsOf(ctx context.Context, engine client.Object) []reconcile.Request {
	var streams tramv1.StreamList
	if err := r.List(ctx, &streams,
		client.InNamespace(engine.GetNamespace()),
		client.HasLabels{tramv1.EngineLabel(engine.GetName())}); err != nil {
		logf.FromContext(ctx).Error(err, "Could not list Streams of Engine", "engine", engine.GetName())
		return nil
	}
	requests := make([]reconcile.Request, len(streams.Items))
	for i, s := range streams.Items {
		requests[i] = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&s)}
	}
	return requests
}

// engineLifecycle passes the events that change whether an Engine accepts
// Streams: its creation, deletion and the start of its deletion, and those
// that change whether it stores what buffers hold.
func engineLifecycle() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero() {
				return true
			}
			oldEngine, okOld := e.ObjectOld.(*tramv1alpha1.Engine)
			newEngine, okNew := e.ObjectNew.(*tramv1alpha1.Engine)
			return okOld && okNew && persistence(oldEngine) != persistence(newEngine)
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// persistence sums up what storageAvailable depends on.
func persistence(engine *tramv1alpha1.Engine) string {
	return fmt.Sprintf("%t/%s/%t", engine.Spec.Storage != nil, engine.Status.Workload,
		meta.IsStatusConditionTrue(engine.Status.Conditions, tramv1alpha1.ConditionMigrating))
}

// deletionStarted passes the update that marks an object for deletion.
func deletionStarted() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetDeletionTimestamp().IsZero() && !e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

func engineNames(refs []tramv1.EngineReference) []string {
	names := make([]string, len(refs))
	for i, ref := range refs {
		names[i] = ref.Name
	}
	return names
}
