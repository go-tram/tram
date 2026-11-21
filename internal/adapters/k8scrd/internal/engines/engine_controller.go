// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
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

// handoverFieldOwner co-owns the Deployment's replicas while they are handed
// over to the autoscaler, so the manager can stop applying them without
// resetting them; see replicas.
const handoverFieldOwner = "tram-manager-replicas-handover"

// EngineReconciler runs every Engine as a Deployment of Tram instances started
// with --engine, with a service account bound to ClusterRole in the Engine's
// namespace, a Service and, if the Engine asks for them, a
// HorizontalPodAutoscaler and a PodDisruptionBudget. All of them are applied
// server-side and owned by the Engine, so deleting it deletes them.
type EngineReconciler struct {
	client.Client

	// Image runs the Engines that set no spec.image.
	Image string
	// ClusterRole is bound to the service account of every Engine in its
	// namespace: the role of a Tram instance with the operator enabled.
	ClusterRole string
	// Recorder records events on Engines, e.g. when a volume was drained;
	// nil records none.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=io.tram.core,resources=engines,verbs=get;list;watch
// +kubebuilder:rbac:groups=io.tram.core,resources=engines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=io.tram.core,resources=engines/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles,verbs=bind,resourceNames=manager-role
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=io.tram.core,resources=engines,verbs=update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *EngineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var engine tramv1alpha1.Engine
	if err := r.Get(ctx, req.NamespacedName, &engine); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	state, err := r.load(ctx, &engine)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !engine.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &engine, state)
	}
	if err = r.ensureFinalizer(ctx, &engine, state); err != nil {
		return ctrl.Result{}, err
	}
	before := engine.DeepCopy()

	if writers := state.activeBufferWriters(); engine.Spec.Storage == nil && state.statefulSet != nil &&
		len(writers) > 0 {
		// Removing storage ends the Streams' writes to buffers, so the Engine
		// keeps its StatefulSet until their authors stopped them: suspended
		// or removed the buffer sink. Reading Streams may stay; drain Jobs
		// deliver the volumes through them.
		meta.SetStatusCondition(&engine.Status.Conditions, metav1.Condition{
			Type:   tramv1alpha1.ConditionReconciled,
			Status: metav1.ConditionFalse,
			Reason: "StorageInUse",
			Message: fmt.Sprintf("spec.storage was removed, but Streams still write to buffers on the Engine: %s; "+
				"suspend them or remove their buffer sinks, and the Engine migrates to a Deployment while drain "+
				"Jobs deliver the volumes through the reading Streams", strings.Join(writers, ", ")),
			ObservedGeneration: engine.Generation,
		})
		return ctrl.Result{}, r.patchStatus(ctx, before, &engine)
	}

	applyErr := r.applyAll(ctx, &engine, state)
	reconciled := metav1.Condition{
		Type:               tramv1alpha1.ConditionReconciled,
		Status:             metav1.ConditionTrue,
		Reason:             "Applied",
		Message:            "The resources of the Engine match its spec",
		ObservedGeneration: engine.Generation,
	}
	switch {
	case errors.Is(applyErr, ErrInvalidEngine):
		reconciled.Status, reconciled.Reason, reconciled.Message = metav1.ConditionFalse, "InvalidSpec", applyErr.Error()
		// Retrying cannot fix the spec; a change of it reconciles again.
		applyErr = nil
	case applyErr != nil:
		reconciled.Status, reconciled.Reason, reconciled.Message = metav1.ConditionFalse, "ApplyFailed", applyErr.Error()
	}
	meta.SetStatusCondition(&engine.Status.Conditions, reconciled)

	// The resources just applied, as they are now.
	if state, err = r.load(ctx, &engine); err != nil {
		return ctrl.Result{}, errors.Join(applyErr, err)
	}
	migrateErr := r.migrate(ctx, &engine, state)
	drained, drainErr := r.drain(ctx, &engine, state, false)

	r.observe(&engine, state, drained)
	return ctrl.Result{RequeueAfter: drained.requeue},
		errors.Join(applyErr, migrateErr, drainErr, r.patchStatus(ctx, before, &engine))
}

// applyAll applies the Engine's resources and deletes the autoscaler and the
// disruption budget once the Engine no longer asks for them. The workload is
// a Deployment, or a StatefulSet if the Engine has storage; the other kind is
// left to migrate, which deletes it once the new one is ready.
func (r *EngineReconciler) applyAll(ctx context.Context, engine *tramv1alpha1.Engine, state *engineState) error {
	params := buildParams{image: r.Image, clusterRole: r.ClusterRole, storageOnly: state.storageServes(engine)}
	replicas, apply, err := r.replicas(ctx, engine, state)
	if err != nil {
		return err
	}
	if apply {
		params.replicas = &replicas
	}
	res, err := buildResources(engine, params)
	if err != nil {
		return err
	}

	opts := []client.ApplyOption{client.FieldOwner(FieldOwner), client.ForceOwnership}
	applies := []namedApply{
		{"service account", res.serviceAccount},
		{"role binding", res.roleBinding},
		{"service", res.service},
	}
	if res.statefulSet != nil {
		applies = append(applies, namedApply{"stateful set", res.statefulSet})
	} else {
		applies = append(applies, namedApply{"deployment", res.deployment})
	}
	if res.hpa != nil {
		applies = append(applies, namedApply{"horizontal pod autoscaler", res.hpa})
	}
	if res.pdb != nil {
		applies = append(applies, namedApply{"pod disruption budget", res.pdb})
	}
	for _, a := range applies {
		if err = r.Apply(ctx, a.config, opts...); err != nil {
			return fmt.Errorf("apply %s: %w", a.kind, err)
		}
	}

	if res.hpa == nil {
		if err = r.deleteOwned(ctx, engine, &autoscalingv2.HorizontalPodAutoscaler{}); err != nil {
			return err
		}
	}
	if res.pdb == nil {
		if err = r.deleteOwned(ctx, engine, &policyv1.PodDisruptionBudget{}); err != nil {
			return err
		}
	}
	return nil
}

type namedApply struct {
	kind   string
	config runtime.ApplyConfiguration
}

// replicas returns the replicas to apply to the workload, spec.replicas,
// and whether to apply them at all: not while the autoscaler sets them. A new
// autoscaled workload starts with minReplicas, or with the replicas of the
// workload it replaces if they are more, so a migration keeps the capacity.
//
// Dropping the replicas from an apply would reset them to 1 if the manager is
// their only owner, so before it stops applying them, a second field manager
// applies their current value. Once the autoscaler changes them, it owns them,
// and this apply conflicts, which is fine.
func (r *EngineReconciler) replicas(
	ctx context.Context,
	engine *tramv1alpha1.Engine,
	state *engineState,
) (int32, bool, error) {
	if !autoscaled(engine) {
		if engine.Spec.Replicas != nil {
			return *engine.Spec.Replicas, true, nil
		}
		return 1, true, nil
	}
	target, other := state.workloads(engine)
	if target == nil {
		replicas := *engine.Spec.Autoscaling.MinReplicas
		if other != nil {
			replicas = max(replicas, other.specReplicas())
		}
		return replicas, true, nil
	}
	if target.replicas() == nil || !ownsReplicas(target.object(), FieldOwner) {
		return 0, false, nil
	}

	handover := target.replicasApply()
	if err := r.Apply(ctx, handover, client.FieldOwner(handoverFieldOwner)); err != nil && !apierrors.IsConflict(err) {
		return 0, false, fmt.Errorf("hand replicas over to the autoscaler: %w", err)
	}
	return 0, false, nil
}

// ownsReplicas tells whether manager applied the workload's replicas.
func ownsReplicas(obj client.Object, manager string) bool {
	for _, entry := range obj.GetManagedFields() {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]map[string]any
		if err := json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields); err != nil {
			continue
		}
		if _, ok := fields["f:spec"]["f:replicas"]; ok {
			return true
		}
	}
	return false
}

// deleteOwned deletes the Engine's object of obj's kind if the Engine controls
// it.
func (r *EngineReconciler) deleteOwned(ctx context.Context, engine *tramv1alpha1.Engine, obj client.Object) error {
	key := types.NamespacedName{Namespace: engine.Namespace, Name: ResourceName(engine.Name)}
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, engine) {
		return nil
	}
	if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete %s: %w", strings.ToLower(fmt.Sprintf("%T", obj)), err)
	}
	logf.FromContext(ctx).Info("Deleted resource the Engine no longer asks for",
		"engine", client.ObjectKeyFromObject(engine), "resource", fmt.Sprintf("%T", obj))
	return nil
}

// observe fills the status from the workload, the volumes and the Engine's
// Streams.
func (r *EngineReconciler) observe(engine *tramv1alpha1.Engine, state *engineState, drained drainResult) {
	status := &engine.Status
	status.ObservedGeneration = engine.Generation
	status.Selector = EngineLabel + "=" + engine.Name
	status.Streams = int32(len(state.streams)) //nolint:gosec // the number of streams fits.
	status.Workload = workloadKind(engine)
	status.Volumes = int32(len(state.claims))        //nolint:gosec // the number of volumes fits.
	status.DrainingVolumes = int32(drained.draining) //nolint:gosec // the number of volumes fits.
	status.Replicas, status.ReadyReplicas = 0, 0

	ready := metav1.Condition{
		Type:               tramv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             status.Workload + "Missing",
		Message:            fmt.Sprintf("The %s of the Engine does not exist yet", status.Workload),
		ObservedGeneration: engine.Generation,
	}
	target, other := state.workloads(engine)
	if target != nil {
		status.Replicas, status.ReadyReplicas = target.statusReplicas()
		ready.Reason, ready.Message = status.Workload+"Unavailable", fmt.Sprintf(
			"The %s of the Engine is not ready yet", status.Workload)
		if target.ready() {
			ready.Status, ready.Reason = metav1.ConditionTrue, status.Workload+"Available"
			ready.Message = fmt.Sprintf("The %s of the Engine is ready", status.Workload)
		}
	}
	meta.SetStatusCondition(&status.Conditions, ready)

	migrating := metav1.Condition{
		Type: tramv1alpha1.ConditionMigrating, Status: metav1.ConditionFalse, Reason: "Settled",
		Message: "The instances run as " + status.Workload, ObservedGeneration: engine.Generation,
	}
	if other != nil {
		migrating.Status, migrating.Reason = metav1.ConditionTrue, "To"+status.Workload
		migrating.Message = fmt.Sprintf("The %s keeps running until the %s is ready", other.kind, status.Workload)
	}
	meta.SetStatusCondition(&status.Conditions, migrating)

	draining := metav1.Condition{
		Type: tramv1alpha1.ConditionDraining, Status: metav1.ConditionFalse, Reason: "NothingToDrain",
		Message: "Every volume belongs to an instance", ObservedGeneration: engine.Generation,
	}
	if drained.draining > 0 {
		draining.Status, draining.Reason = metav1.ConditionTrue, "Draining"
		draining.Message = fmt.Sprintf("Drain Jobs deliver what %d volumes without an instance hold", drained.draining)
		if drained.failed > 0 {
			draining.Reason = "DrainFailed"
			draining.Message += fmt.Sprintf(
				"; %d drain Jobs failed: delete them to retry, or wait for the entries to expire",
				drained.failed,
			)
		}
	}
	meta.SetStatusCondition(&status.Conditions, draining)
}

func (r *EngineReconciler) patchStatus(ctx context.Context, before, engine *tramv1alpha1.Engine) error {
	if equality.Semantic.DeepEqual(before.Status, engine.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, engine, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch status: %w", err)
	}
	return nil
}

// SetupWithManager reconciles an Engine when its spec changes, when one of its
// resources changes, e.g. the Deployment's status or a resource deleted by
// hand, and when a Stream joins or leaves it.
func (r *EngineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tramv1alpha1.Engine{}, builder.WithPredicates(predicate.Or[client.Object](
			predicate.GenerationChangedPredicate{},
			// The discard annotation and the start of a deletion.
			predicate.AnnotationChangedPredicate{},
			deletionStarted(),
		))).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&batchv1.Job{}).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(claimEngine)).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(&tramv1.Stream{}, streamEngines()).
		Named("engine").
		Complete(r)
}

// streamEngines enqueues the Engines a Stream is labeled for, before and after
// a change of its labels or spec, so their stream counts and the storage
// they hold follow.
func streamEngines() handler.EventHandler {
	enqueue := func(q workqueue.TypedRateLimitingInterface[reconcile.Request], objs ...client.Object) {
		for _, obj := range objs {
			for key := range obj.GetLabels() {
				if name, ok := strings.CutPrefix(key, tramv1.EngineLabelPrefix); ok {
					q.Add(reconcile.Request{Namespace: obj.GetNamespace(), Name: name})
				}
			}
		}
	}
	return handler.Funcs{
		CreateFunc: func(_ context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(q, e.Object)
		},
		UpdateFunc: func(_ context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			// Labels change the Engines a Stream belongs to; its spec, e.g.
			// suspending a Stream that writes to a buffer, may release the
			// storage an Engine is asked to give up.
			if !equality.Semantic.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) ||
				e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				enqueue(q, e.ObjectOld, e.ObjectNew)
			}
		},
		DeleteFunc: func(_ context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(q, e.Object)
		},
	}
}
