// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller

import (
	"context"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

// coreChanges collects the streams that changed in the core, through the API,
// the file sync or the operator itself, so the Stream resources they belong
// to are reconciled: a resource owns its stream, and a change from another
// source is undone right away. Receiving never waits for the controller, which
// starts later and may be slow, so the core is never held up by the operator.
type coreChanges struct {
	mu      sync.Mutex
	pending map[types.NamespacedName]struct{}
	// signal tells forward that pending holds names.
	signal chan struct{}
}

func newCoreChanges() *coreChanges {
	return &coreChanges{pending: map[types.NamespacedName]struct{}{}, signal: make(chan struct{}, 1)}
}

// watch receives the changes of the core until ctx ends. It runs as a
// runnable of the manager, which starts it before the controllers, so no
// change between the initial reconciles and the watch is missed.
func (c *coreChanges) watch(ctx context.Context, core ports.Core) error {
	events := make(chan domain.StreamSpecWatchEvent)
	if err := core.Watch(ctx, events); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-events:
			c.add(event.StreamSpec.Name)
		}
	}
}

// add records the stream name if it can be one of a Stream resource,
// <namespace>.<name>; namespaces cannot contain dots.
func (c *coreChanges) add(stream string) {
	namespace, name, ok := strings.Cut(stream, ".")
	if !ok || namespace == "" || name == "" {
		return
	}
	c.mu.Lock()
	c.pending[types.NamespacedName{Namespace: namespace, Name: name}] = struct{}{}
	c.mu.Unlock()
	select {
	case c.signal <- struct{}{}:
	default:
	}
}

func (c *coreChanges) take() []types.NamespacedName {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]types.NamespacedName, 0, len(c.pending))
	for name := range c.pending {
		names = append(names, name)
	}
	clear(c.pending)
	return names
}

// forward queues the reconciles of the changed streams whose Stream resource
// owns them, until ctx ends. A stream without such a resource, e.g. one
// created through the API under a name that only looks like one, is not
// touched.
func (c *coreChanges) forward(
	ctx context.Context,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
	owns func(context.Context, types.NamespacedName) bool,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.signal:
			for _, name := range c.take() {
				if owns(ctx, name) {
					queue.Add(reconcile.Request{NamespacedName: name})
				}
			}
		}
	}
}

// owns reports whether the instance runs the Stream resource name: it is in
// the cache, which holds only the selected namespaces and, with an engine,
// the Streams labeled for it, and with an engine its spec.engineRefs name it.
func (r *StreamReconciler) owns(ctx context.Context, name types.NamespacedName) bool {
	var stream tramv1.Stream
	if err := r.Get(ctx, name, &stream); err != nil {
		// Not found, or a namespace the cache does not hold.
		return false
	}
	return r.Engine == "" || stream.RefersToEngine(r.Engine)
}
