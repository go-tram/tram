// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

func TestDeletedStreamReleasesItsPort(t *testing.T) {
	t.Parallel()

	p := newPipeline(t, nil, []domain.SinkSpec{captureSinkSpec("first", "in")})
	require.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
	require.NoError(t, p.core.DeleteStream(context.Background(), "e2e"))
	assert.Zero(t, p.post(t, request("shop", rec{"r2", "A"}), time.Second), "nothing listens after the delete")

	// A new stream binds the same port at once.
	p.update(t, nil, []domain.SinkSpec{captureSinkSpec("second", "in")})
	require.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r3", "A"}), 10*time.Second))
	assert.Equal(t, []string{"shop/e2e/r1"}, arrived(p.sinks.sink(t, "first").received()))
	assert.Equal(t, []string{"shop/e2e/r3"}, arrived(p.sinks.sink(t, "second").received()))
}

// Creating, updating and deleting streams must not leave goroutines behind:
// a stopped source, stage or sink whose goroutine keeps running also keeps its
// buffers and connections. The test counts the goroutines of the whole
// process, so it does not run in parallel; Go runs it while the parallel tests
// of the package are paused.
//
//nolint:paralleltest // Counts the goroutines of the whole process.
func TestStreamLifecycleLeaksNoGoroutines(t *testing.T) {
	lifecycle := func() {
		p := newPipeline(t,
			[]domain.StageSpec{batchStage(10, "5ms")},
			[]domain.SinkSpec{captureSinkSpec("before", "batch")})
		require.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))

		p.update(t,
			[]domain.StageSpec{batchStage(10, "5ms"), customerFilter(customerA, "batch", isCustomerA)},
			[]domain.SinkSpec{captureSinkSpec("after", customerA)})
		require.Equal(t, http.StatusOK, p.post(t, request("shop", rec{"r2", "A"}), 10*time.Second))

		require.NoError(t, p.core.DeleteStream(context.Background(), "e2e"))
		require.NoError(t, p.core.Close())
	}

	lifecycle() // Starts what lives for the whole process, like the HTTP transport.
	baseline := settledGoroutines()

	const cycles = 20
	for range cycles {
		lifecycle()
	}

	now := settledGoroutines()
	for deadline := time.Now().Add(5 * time.Second); now > baseline && time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
		now = settledGoroutines()
	}
	assert.LessOrEqual(t, now, baseline, "goroutines before %d stream lifecycles and after", cycles)
}

func settledGoroutines() int {
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	runtime.GC()
	return runtime.NumGoroutine()
}

// Updating the streams of a long-running core over and over must not keep the
// data of earlier versions. Each cycle sends about 64 KiB; keeping one event
// per cycle would grow the heap far beyond the limit, which leaves room for
// the noise of the runtime (about 1 KiB per cycle). The core is created
// without the pipeline helpers, whose cleanups keep every pipeline until the
// test ends. Like the goroutine test, it measures the whole process.
//
//nolint:paralleltest // Measures the heap of the whole process.
func TestStreamUpdatesDoNotKeepOldData(t *testing.T) {
	const (
		cycles            = 60
		requestsPerCycle  = 4
		maxGrowthPerCycle = 16 << 10
	)
	records := make([]rec, 16)
	for i := range records {
		records[i] = rec{id: fmt.Sprintf("%02d-%s", i, strings.Repeat("x", 1<<10)), customer: "A"}
	}
	body, err := proto.Marshal(request("shop", records...))
	require.NoError(t, err)

	c := core.New(context.Background(), discard(),
		source.NewFactory(discard()), stage.NewFactory(discard()), sink.NewFactory(discard()))
	defer func() { require.NoError(t, c.Close()) }()
	ports := []int{0, 0}

	cycle := func(i int) {
		// A new port rebuilds the whole stream, the second update swaps
		// only its stages and sinks.
		// The port is free between the cycles that use it, so another test
		// may take it meanwhile; then the cycle moves to a new one.
		var spec domain.StreamSpec
		port := onFreePort(t, ports[i%2], func(port int) error {
			spec = domain.StreamSpec{
				Name:   "e2e",
				Source: domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(port)}},
				Stages: []domain.StageSpec{batchStage(50, "5ms")},
				Sinks:  []domain.SinkSpec{{Name: "out", Type: "noop", From: "batch"}},
			}
			_, upsertErr := c.UpsertStream(context.Background(), spec)
			return upsertErr
		})
		ports[i%2] = port
		for range requestsPerCycle {
			resp, postErr := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v1/logs", port),
				"application/x-protobuf", bytes.NewReader(body))
			require.NoError(t, postErr)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}
		spec.Stages = append(spec.Stages, customerFilter(customerA, "batch", isCustomerA))
		spec.Sinks = []domain.SinkSpec{{Name: "filtered", Type: "noop", From: customerA}}
		_, upsertErr := c.UpsertStream(context.Background(), spec)
		require.NoError(t, upsertErr)
	}

	for i := range 6 { // Warms up pools and caches that live for the process.
		cycle(i)
	}
	baseline := settledHeap()
	for i := range cycles {
		cycle(i)
	}
	growth := int64(settledHeap()) - int64(baseline)
	assert.Less(t, growth, int64(cycles*maxGrowthPerCycle),
		"the heap grew by %d KiB over %d update cycles", growth>>10, cycles)
}

func settledHeap() uint64 {
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	time.Sleep(20 * time.Millisecond)
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
