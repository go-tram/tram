// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package scenario_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/natstest"
)

const (
	idleWindow     = 10 * time.Second
	settleTime     = 2 * time.Second
	baselineWindow = 3 * time.Second
	kibibyte       = 1024
)

type streamShape struct {
	name    string
	workers int
	build   func(e *streamsEnv, i int) domain.StreamSpec
}

func streamShapes() []streamShape {
	routing := func(workers int) streamShape {
		return streamShape{
			name:    "nats-filter-nats",
			workers: workers,
			build: func(e *streamsEnv, i int) domain.StreamSpec {
				return domain.StreamSpec{
					Name: fmt.Sprintf("s%d", i),
					Source: domain.SourceSpec{Name: "in", Type: "broker", Spec: map[string]any{
						"driver":  "nats",
						"signal":  "logs",
						"workers": float64(workers),
						"nats": map[string]any{
							"url":     e.natsURL,
							"stream":  e.input,
							"subject": fmt.Sprintf("%s.s%d", e.input, i),
							"durable": fmt.Sprintf("%s-s%d", e.input, i),
						},
					}},
					Stages: []domain.StageSpec{{
						Name: "filter", Type: "filter", From: "in",
						Spec: celFilter("record.attributes.exists(a, a.key == 'team')"),
					}},
					Sinks: []domain.SinkSpec{e.sink(i, "filter")},
				}
			},
		}
	}
	return []streamShape{
		routing(1),
		routing(4),
		{
			name: "otlp-batch-nats",
			build: func(e *streamsEnv, i int) domain.StreamSpec {
				return domain.StreamSpec{
					Name: fmt.Sprintf("s%d", i),
					Source: domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{
						"http_port": float64(e.ports[i]),
					}},
					Stages: []domain.StageSpec{{
						Name: "batch", Type: "batch", From: "in",
						Spec: map[string]any{"max_records": float64(batchMaxRecords), "timeout": batchTimeout},
					}},
					Sinks: []domain.SinkSpec{e.sink(i, "batch")},
				}
			},
		},
	}
}

func streamCounts() []int {
	return []int{10, 100, 1000}
}

func BenchmarkStreams(b *testing.B) {
	server := natstest.StartServer(b)

	nc, err := nats.Connect(server.URL)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}

	run := 0
	for _, shape := range streamShapes() {
		for _, n := range streamCounts() {
			b.Run(fmt.Sprintf("%s/workers=%d/streams=%d", shape.name, shape.workers, n), func(b *testing.B) {
				for range b.N {
					run++
					e := newStreamsEnv(b, server, js, fmt.Sprintf("streams%d", run))
					measureStreams(b, e, shape, n)
				}
			})
		}
	}
}

type streamsEnv struct {
	b             *testing.B
	natsURL       string
	monitoringURL string
	js            jetstream.JetStream
	input, output string
	ports         []int
}

// reservePorts finds n distinct free ports. All listeners stay open until
// every port is found, so the system cannot hand out a port twice, as it can
// with freePort called n times.
func (e *streamsEnv) reservePorts(n int) {
	e.b.Helper()
	listeners := make([]net.Listener, 0, n)
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			e.b.Fatal(err)
		}
		listeners = append(listeners, l)
		e.ports = append(e.ports, l.Addr().(*net.TCPAddr).Port)
	}
}

func newStreamsEnv(b *testing.B, server natstest.Server, js jetstream.JetStream, prefix string) *streamsEnv {
	b.Helper()
	e := &streamsEnv{
		b: b, natsURL: server.URL, monitoringURL: server.MonitoringURL, js: js,
		input: prefix + "-in", output: prefix + "-out",
	}
	ctx := context.Background()
	// TRAM creates no streams.
	for _, name := range []string{e.input, e.output} {
		if _, err := js.CreateStream(
			ctx,
			jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}},
		); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), e.input)
		_ = js.DeleteStream(context.Background(), e.output)
	})
	return e
}

func (e *streamsEnv) sink(i int, from string) domain.SinkSpec {
	return domain.SinkSpec{Name: "out", Type: "broker", From: from, Spec: map[string]any{
		"driver":      "nats",
		"nats":        map[string]any{"url": e.natsURL},
		"destination": map[string]any{"logs": fmt.Sprintf("'%s.s%d'", e.output, i)},
	}}
}

type sample struct {
	at         time.Time
	cpu        time.Duration
	heap       uint64
	goroutines int
	conns      int
	natsMsgs   int64
}

func (e *streamsEnv) sample() sample {
	e.b.Helper()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	v := e.varz()
	return sample{
		at:         time.Now(),
		cpu:        cpuTime(),
		heap:       mem.HeapAlloc,
		goroutines: runtime.NumGoroutine(),
		conns:      v.Connections,
		natsMsgs:   v.InMsgs,
	}
}

type varz struct {
	Connections int   `json:"connections"`
	InMsgs      int64 `json:"in_msgs"`
}

func (e *streamsEnv) varz() varz {
	e.b.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, e.monitoringURL+"/varz", nil)
	if err != nil {
		e.b.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.b.Fatal(err)
	}
	defer resp.Body.Close()
	var v varz
	if err = json.NewDecoder(resp.Body).Decode(&v); err != nil {
		e.b.Fatal(err)
	}
	return v
}

func measureStreams(b *testing.B, e *streamsEnv, shape streamShape, n int) {
	b.Helper()
	ctx := context.Background()

	time.Sleep(settleTime)
	baselineStart := e.sample()
	time.Sleep(baselineWindow)
	before := e.sample()
	baselineCPU := cpuRate(baselineStart, before)

	if shape.workers == 0 {
		e.reservePorts(n)
	}
	specs := make([]domain.StreamSpec, n)
	for i := range n {
		specs[i] = shape.build(e, i)
	}

	logger := slog.New(slog.DiscardHandler)
	c := core.New(ctx, logger, source.NewFactory(logger), stage.NewFactory(logger), sink.NewFactory(logger))
	defer func() { _ = c.Close(context.Background()) }()

	start := time.Now()
	for _, spec := range specs {
		if _, err := c.UpsertStream(ctx, spec); err != nil {
			b.Fatal(err)
		}
	}
	build := time.Since(start)

	if shape.workers > 0 {
		e.waitForConsumers(n)
	}
	time.Sleep(settleTime)

	idleStart := e.sample()
	time.Sleep(idleWindow)
	idleEnd := e.sample()

	perStream := func(v float64) float64 { return v / float64(n) }
	seconds := idleEnd.at.Sub(idleStart.at).Seconds()
	b.ReportMetric(perStream(float64(build.Microseconds())), "build-µs/stream")
	b.ReportMetric(perStream(float64(int64(idleEnd.heap)-int64(before.heap))/kibibyte), "heap-KiB/stream")
	b.ReportMetric(perStream(float64(idleEnd.goroutines-before.goroutines)), "goroutines/stream")
	b.ReportMetric(perStream(float64(idleEnd.conns-before.conns)), "nats-conns/stream")
	b.ReportMetric(perStream(float64(idleEnd.natsMsgs-idleStart.natsMsgs)/seconds), "nats-msgs/s/stream")
	b.ReportMetric(perStream(cpuRate(idleStart, idleEnd)-baselineCPU), "idle-cpu-µs/s/stream")
}

func cpuRate(from, to sample) float64 {
	return float64((to.cpu - from.cpu).Microseconds()) / to.at.Sub(from.at).Seconds()
}

func (e *streamsEnv) waitForConsumers(n int) {
	e.b.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		s, err := e.js.Stream(ctx, e.input)
		if err == nil {
			info, infoErr := s.Info(ctx)
			if infoErr == nil && info.State.Consumers >= n {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.b.Fatalf("stream %s did not get %d consumers", e.input, n)
}
