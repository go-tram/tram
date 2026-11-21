// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
)

// readerStream reads the buffer queue "q" in namespace.
func readerStream(namespace string) *tramv1.Stream {
	s := newStream(namespace)
	s.Spec.Source = tramv1.Spec{
		Name: "in", Type: "buffer",
		Spec: &runtime.RawExtension{Raw: []byte(`{"driver":"sqlite","sqlite":{},"queue":"q"}`)},
	}
	return s
}

// writerStream writes into the buffer queue "q" in namespace.
func writerStream(namespace string) *tramv1.Stream {
	s := newStream(namespace)
	s.Spec.Sinks[0] = tramv1.ReceiverSpec{
		Name: "out", Type: "buffer", From: "in",
		Spec: &runtime.RawExtension{Raw: []byte(`{"driver":"sqlite","sqlite":{},"queue":"q"}`)},
	}
	return s
}

// Without a data directory, e.g. on an Engine without storage, Streams with
// a buffer are not run, and not retried as an error: they can run only once
// the instance has storage.
func TestReconcileSkipsBufferStreamsWithoutDataDir(t *testing.T) {
	t.Parallel()

	for name, stream := range map[string]*tramv1.Stream{
		"reader": readerStream("default"),
		"writer": writerStream("default"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r, core := newReconciler(t, stream)

			_, err := r.Reconcile(t.Context(), request("default"))
			require.NoError(t, err, "no retry: the Stream waits for storage")
			assert.Zero(t, core.upserts)

			var got tramv1.Stream
			require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "s"}, &got))
			assert.False(t, got.Status.Mounted)
		})
	}
}

func TestReconcileRunsBufferStreamsWithDataDir(t *testing.T) {
	t.Parallel()
	r, core := newReconciler(t, readerStream("default"))
	r.HasDataDir = true

	_, err := r.Reconcile(t.Context(), request("default"))
	require.NoError(t, err)
	assert.Equal(t, 1, core.upserts)
}

// A drain runs the reading Streams, even suspended ones, and nothing that
// would write to the volume.
func TestReconcileInDrainModeRunsReadersOnly(t *testing.T) {
	t.Parallel()

	reader := readerStream("readers")
	reader.Spec.Suspended = new(true)
	r, core := newReconciler(t, reader, writerStream("writers"))
	r.HasDataDir = true
	r.Drain = true

	for _, ns := range []string{"readers", "writers"} {
		_, err := r.Reconcile(t.Context(), request(ns))
		require.NoError(t, err)
	}
	require.Contains(t, core.streams, "readers.s", "the suspended reader drains the volume")
	assert.False(t, core.streams["readers.s"].Suspended)
	assert.NotContains(t, core.streams, "writers.s", "nothing writes to the volume while it drains")
}
