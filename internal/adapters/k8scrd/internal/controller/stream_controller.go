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

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

type StreamReconciler struct {
	client.Client

	Core   ports.Core
	Scheme *runtime.Scheme
	// References reads the secrets and config maps that streams refer to,
	// e.g. the manager's API reader, so their values are not kept in the
	// cache. The Client without it.
	References client.Reader
	// Engine, if set, runs only the Streams whose spec.engineRefs name it.
	// The cache already holds only the Streams with its engine label, but the
	// label is just an index for the API server's filter: a label set by hand
	// runs nothing.
	Engine string
	// HasDataDir tells whether the instance has a data directory for buffers.
	// Without one, Streams with a buffer sink or source cannot run and are
	// skipped, e.g. on an Engine without storage, while drain Jobs deliver
	// its volumes.
	HasDataDir bool
	// Drain, for `tram drain`, runs only the Streams that read from a buffer,
	// so they deliver what the buffer files of a volume hold, and treats all
	// others as suspended: nothing writes to the volume any more.
	Drain bool
}

// +kubebuilder:rbac:groups=io.tram.core,resources=streams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=io.tram.core,resources=streams/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

func (r *StreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	name := StreamName(req.NamespacedName)

	var stream tramv1.Stream
	if err := r.Get(ctx, req.NamespacedName, &stream); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("get stream: %w", err)
		}
		return ctrl.Result{}, r.deleteFromCore(ctx, name)
	}

	if r.Engine != "" && !stream.RefersToEngine(r.Engine) {
		return ctrl.Result{}, r.stopUnreferenced(ctx, name)
	}

	desired, err := toDomain(name, stream)
	if err != nil {
		return ctrl.Result{}, errors.Join(r.setMounted(ctx, &stream, false), reconcile.TerminalError(err))
	}

	resolved, refersToValues, err := resolveRefs(ctx, r.references(), stream.Namespace, *desired)
	if err != nil {
		// A missing secret or config map, or config maps that disagree, are
		// retried, and creating or changing them reconciles the stream as
		// well.
		return ctrl.Result{}, errors.Join(r.setMounted(ctx, &stream, false), resolveError(err))
	}

	if r.Drain {
		if !ReadsBuffer(*desired) {
			return ctrl.Result{}, r.deleteFromCore(ctx, name)
		}
		// Draining runs the readers even when their stream is suspended:
		// delivering what the queues hold is the point of the drain.
		desired.Suspended = false
		resolved.Suspended = false
	}
	if !r.HasDataDir && UsesBuffer(*desired) {
		// Not an error to retry: the Stream can run only once the instance
		// has storage, which comes with a new pod.
		log.Info("Not running Stream with a buffer: the instance has no data directory", "stream", name)
		return ctrl.Result{}, errors.Join(r.deleteFromCore(ctx, name), r.setMounted(ctx, &stream, false))
	}

	current, err := r.Core.GetStream(ctx, name)
	if err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
		return ctrl.Result{}, fmt.Errorf("get from core: %w", err)
	}
	changed := current == nil || !current.Equal(*desired)

	switch {
	case refersToValues:
		// The core only sees the values, so it decides whether they changed;
		// it keeps a stream whose spec and values are the same.
		_, err = r.Core.UpsertResolvedStream(ctx, *desired, resolved)
	case changed:
		_, err = r.Core.UpsertStream(ctx, *desired)
	}
	if err != nil {
		return ctrl.Result{}, errors.Join(r.setMounted(ctx, &stream, false), fmt.Errorf("upsert into core: %w", err))
	}
	if changed {
		log.Info("Upserted Stream into core", "stream", name, "generation", stream.Generation)
	}

	return ctrl.Result{}, r.setMounted(ctx, &stream, true)
}

// BufferType is the type of the buffer source and sink, which keep events in
// the data directory of the instance.
const BufferType = "buffer"

// ReadsBuffer reports whether a stream's source reads from a buffer.
func ReadsBuffer(spec domain.StreamSpec) bool {
	return spec.Source.Type == BufferType
}

// UsesBuffer reports whether a stream reads from or writes to a buffer, so
// it needs a data directory.
func UsesBuffer(spec domain.StreamSpec) bool {
	if ReadsBuffer(spec) {
		return true
	}
	for _, sink := range spec.Sinks {
		if sink.Type == BufferType {
			return true
		}
	}
	return false
}

// deleteFromCore removes a Stream that no longer exists from the core.
func (r *StreamReconciler) deleteFromCore(ctx context.Context, name string) error {
	if err := r.Core.DeleteStream(ctx, name); err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
		return fmt.Errorf("delete from core: %w", err)
	}
	logf.FromContext(ctx).Info("Deleted Stream from core", "stream", name)
	return nil
}

// resolveError wraps an error from resolving references. Invalid references
// are terminal; anything else is retried.
func resolveError(err error) error {
	err = fmt.Errorf("resolve references: %w", err)
	if errors.Is(err, ErrInvalidSecretRef) || errors.Is(err, ErrInvalidConfigMapSelector) {
		return reconcile.TerminalError(err)
	}
	return err
}

// stopUnreferenced stops a Stream whose spec.engineRefs do not name the
// engine. Its status is left to the engines that run it.
func (r *StreamReconciler) stopUnreferenced(ctx context.Context, name string) error {
	err := r.Core.DeleteStream(ctx, name)
	if err == nil {
		logf.FromContext(ctx).Info("Stopped Stream that does not refer to the engine",
			"stream", name, "engine", r.Engine)
		return nil
	}
	if errors.Is(err, domain.ErrStreamNotFound) {
		return nil
	}
	return fmt.Errorf("delete from core: %w", err)
}

func (r *StreamReconciler) references() client.Reader {
	if r.References != nil {
		return r.References
	}
	return r.Client
}

func (r *StreamReconciler) setMounted(ctx context.Context, stream *tramv1.Stream, mounted bool) error {
	if r.Drain {
		// Draining is no instance; status.mounted belongs to the instances.
		return nil
	}
	if stream.Status.Mounted == mounted {
		return nil
	}

	patch := client.MergeFrom(stream.DeepCopy())
	stream.Status.Mounted = mounted

	if err := r.Status().Patch(ctx, stream, patch); err != nil {
		return fmt.Errorf("patch status: %w", err)
	}
	return nil
}

// StreamName returns the core stream name for a Stream resource. Namespaces
// cannot contain dots, so the first dot separates namespace and name.
func StreamName(nn types.NamespacedName) string {
	return nn.Namespace + "." + nn.Name
}

func toDomain(name string, s tramv1.Stream) (*domain.StreamSpec, error) {
	sourceSpec, err := specToMap(s.Spec.Source.Spec)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", s.Spec.Source.Name, err)
	}

	source := domain.SourceSpec{
		Name: s.Spec.Source.Name,
		Type: s.Spec.Source.Type,
		Spec: sourceSpec,
	}

	var stages []domain.StageSpec
	for _, stage := range s.Spec.Stages {
		spec, specErr := specToMap(stage.Spec)
		if specErr != nil {
			return nil, fmt.Errorf("stage %s: %w", stage.Name, specErr)
		}
		stages = append(stages, domain.StageSpec{
			Name: stage.Name,
			Type: stage.Type,
			From: stage.From,
			Spec: spec,
		})
	}

	var sinks []domain.SinkSpec
	for _, sink := range s.Spec.Sinks {
		spec, specErr := specToMap(sink.Spec)
		if specErr != nil {
			return nil, fmt.Errorf("sink %s: %w", sink.Name, specErr)
		}
		sinks = append(sinks, domain.SinkSpec{
			Name: sink.Name,
			Type: sink.Type,
			From: sink.From,
			Spec: spec,
		})
	}

	return &domain.StreamSpec{
		Name:      name,
		Source:    source,
		Stages:    stages,
		Sinks:     sinks,
		Metrics:   metricsToDomain(s.Spec.Metrics),
		Suspended: s.Spec.Suspended != nil && *s.Spec.Suspended,
	}, nil
}

func metricsToDomain(m *tramv1.MetricsSpec) domain.MetricsSpec {
	if m == nil {
		return domain.MetricsSpec{}
	}
	return domain.MetricsSpec{
		Records:     nodeMetricsToDomain(m.Records),
		Duration:    nodeMetricsToDomain(m.Duration),
		RequestSize: m.RequestSize,
	}
}

func nodeMetricsToDomain(n *tramv1.NodeMetricsSpec) domain.NodeMetrics {
	if n == nil {
		return domain.NodeMetrics{}
	}
	return domain.NodeMetrics{Source: n.Source, Stages: n.Stages, Sinks: n.Sinks}
}

func specToMap(raw *runtime.RawExtension) (map[string]any, error) {
	var spec map[string]any
	if raw == nil || len(raw.Raw) == 0 {
		return spec, nil
	}
	if err := json.Unmarshal(raw.Raw, &spec); err != nil {
		return nil, fmt.Errorf("decode spec: %w", err)
	}
	return spec, nil
}

func (r *StreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(context.Background(), &tramv1.Stream{}, secretIndex,
		func(obj client.Object) []string {
			stream, ok := obj.(*tramv1.Stream)
			if !ok {
				return nil
			}
			return secretNames(stream)
		})
	if err != nil {
		return fmt.Errorf("index streams by secret: %w", err)
	}
	err = mgr.GetFieldIndexer().IndexField(context.Background(), &tramv1.Stream{}, configMapIndex,
		func(obj client.Object) []string {
			stream, ok := obj.(*tramv1.Stream)
			if !ok || len(configMapSelectors(stream)) == 0 {
				return nil
			}
			return []string{configMapIndexValue}
		})
	if err != nil {
		return fmt.Errorf("index streams by config map selector: %w", err)
	}

	b := ctrl.NewControllerManagedBy(mgr).
		For(&tramv1.Stream{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Only the metadata of secrets and config maps is watched and cached,
		// to notice changes; their values are read when a stream is
		// reconciled.
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.streamsReferringTo), builder.OnlyMetadata).
		Watches(&corev1.ConfigMap{}, r.configMapHandler(), builder.OnlyMetadata).
		Named("stream")

	// A Stream resource owns its stream: a change through the API or the
	// file sync is undone right away. Draining runs no API or file sync.
	if !r.Drain {
		changes := newCoreChanges()
		if err = mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
			return changes.watch(ctx, r.Core)
		})); err != nil {
			return fmt.Errorf("watch the core: %w", err)
		}
		b = b.WatchesRawSource(source.Func(
			func(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
				go changes.forward(ctx, queue, r.owns)
				return nil
			}))
	}
	return b.Complete(r)
}

// configMapHandler enqueues the streams whose selectors match a config map, on
// an update with its old or new labels.
func (r *StreamReconciler) configMapHandler() handler.EventHandler {
	enqueue := func(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request],
		namespace string, labelSets ...map[string]string) {
		for _, req := range r.streamsSelecting(ctx, namespace, labelSets...) {
			q.Add(req)
		}
	}
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.Object.GetNamespace(), e.Object.GetLabels())
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.ObjectNew.GetNamespace(), e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels())
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.Object.GetNamespace(), e.Object.GetLabels())
		},
		GenericFunc: func(ctx context.Context, e event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.Object.GetNamespace(), e.Object.GetLabels())
		},
	}
}

// streamsReferringTo returns the Stream resources that refer to secret, so a
// changed secret restarts their streams with the new values.
func (r *StreamReconciler) streamsReferringTo(ctx context.Context, secret client.Object) []reconcile.Request {
	var streams tramv1.StreamList
	if err := r.List(ctx, &streams,
		client.InNamespace(secret.GetNamespace()),
		client.MatchingFields{secretIndex: secret.GetName()}); err != nil {
		logf.FromContext(ctx).Error(err, "Could not list streams referring to secret", "secret", secret.GetName())
		return nil
	}
	requests := make([]reconcile.Request, len(streams.Items))
	for i, s := range streams.Items {
		requests[i] = reconcile.Request{Namespace: s.Namespace, Name: s.Name}
	}
	return requests
}
