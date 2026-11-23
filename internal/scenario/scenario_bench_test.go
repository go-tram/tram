// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package scenario_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/amqptest"
	"github.com/go-tram/tram/internal/testutil/kafkatest"
	"github.com/go-tram/tram/internal/testutil/natstest"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

type scenario struct {
	name   string
	source string
	teams  int
	build  func(e *env) ([]domain.StageSpec, []domain.SinkSpec)
	// sizingOnly runs the scenario only with the shape and concurrency of
	// the sizing table, to compare brokers without the full matrix.
	sizingOnly bool
}

func scenarios() []scenario {
	return []scenario{
		{
			name:   "otlp-nats",
			source: "otlp",
			teams:  1,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return nil, []domain.SinkSpec{e.natsSink("out", "in")}
			},
		},
		{
			name:   "otlp-batch-nats",
			source: "otlp",
			teams:  1,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return []domain.StageSpec{batchStage()}, []domain.SinkSpec{e.natsSink("out", "batch")}
			},
		},
		{
			name:   "otlp-split-nats",
			source: "otlp",
			teams:  2,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				const teamZero = "record.attributes.exists(a, a.key == 'team' && a.value.string_value == 'team-0')"
				return []domain.StageSpec{
					{Name: "team-zero", Type: "filter", From: "in", Spec: celFilter(teamZero)},
					{Name: "other-teams", Type: "filter", From: "in", Spec: celFilter("!" + teamZero)},
				}, []domain.SinkSpec{
					e.natsSink("out-a", "team-zero"),
					e.natsSink("out-b", "other-teams"),
				}
			},
		},
		{
			name:       "otlp-batch-kafka",
			source:     "otlp",
			teams:      1,
			sizingOnly: true,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return []domain.StageSpec{batchStage()}, []domain.SinkSpec{e.kafkaSink("out", "batch")}
			},
		},
		{
			name:       "otlp-batch-amqp",
			source:     "otlp",
			teams:      1,
			sizingOnly: true,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return []domain.StageSpec{batchStage()}, []domain.SinkSpec{e.amqpSink("out", "batch")}
			},
		},
		{
			name:   "nats-filter-nats",
			source: "nats",
			teams:  1,
			build: func(e *env) ([]domain.StageSpec, []domain.SinkSpec) {
				return []domain.StageSpec{{
					Name: "filter", Type: "filter", From: "in",
					Spec: celFilter("record.attributes.exists(a, a.key == 'team')"),
				}}, []domain.SinkSpec{e.natsSink("out", "filter")}
			},
		},
	}
}

const (
	batchMaxRecords = 1000
	batchTimeout    = "10ms"

	// The shape and concurrency of the sizing table.
	sizingRecords     = 1000
	sizingConcurrency = 8
)

func batchStage() domain.StageSpec {
	return domain.StageSpec{
		Name: "batch", Type: "batch", From: "in",
		Spec: map[string]any{"max_records": float64(batchMaxRecords), "timeout": batchTimeout},
	}
}

func concurrencies() []int {
	return []int{1, 4, 8}
}

func shapes() []otlpgen.Shape {
	return []otlpgen.Shape{
		{Records: 1, Resources: 1, Attributes: 4},
		{Records: 100, Resources: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Attributes: 4},
	}
}

func BenchmarkScenario(b *testing.B) {
	natsURL := natstest.Start(b)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}

	brokers := &otherBrokers{kafka: kafkatest.Start(b), amqp: amqptest.Start(b)}

	var runs atomic.Int64
	for _, sc := range scenarios() {
		for _, concurrency := range concurrencies() {
			for _, shape := range shapes() {
				if sc.sizingOnly && (concurrency != sizingConcurrency || shape.Records != sizingRecords) {
					continue
				}
				shape.Teams = sc.teams
				name := fmt.Sprintf("%s/concurrency=%d/%s", sc.name, concurrency, shape)
				b.Run(name, func(b *testing.B) {
					e := newEnv(b, natsURL, js, &runs)
					e.brokers = brokers
					switch sc.source {
					case "otlp":
						benchmarkOTLP(b, e, sc, shape, concurrency)
					case "nats":
						benchmarkNATS(b, e, sc, shape, concurrency)
					}
				})
			}
		}
	}
}

type env struct {
	b       *testing.B
	natsURL string
	js      jetstream.JetStream
	prefix  string
	streams []string
	brokers *otherBrokers
}

// otherBrokers are the brokers besides NATS, for the scenarios that compare
// them.
type otherBrokers struct {
	kafka []string
	amqp  *amqptest.Broker
}

// kafkaSink is a broker sink with the Kafka driver to a topic of its own,
// which is deleted with its data after the benchmark.
func (e *env) kafkaSink(name, from string) domain.SinkSpec {
	topic := e.prefix + "-" + name
	kafkatest.CreateTopic(e.b, e.brokers.kafka, topic, 1)
	e.b.Cleanup(func() { kafkatest.DeleteTopic(e.b, e.brokers.kafka, topic) })
	seeds := make([]any, len(e.brokers.kafka))
	for i, s := range e.brokers.kafka {
		seeds[i] = s
	}
	return domain.SinkSpec{Name: name, Type: "broker", From: from, Spec: map[string]any{
		"driver":      "kafka",
		"kafka":       map[string]any{"brokers": seeds},
		"destination": map[string]any{"logs": fmt.Sprintf("'%s'", topic)},
	}}
}

// amqpSink is a broker sink with the AMQP driver to a queue of its own,
// which is deleted with its messages after the benchmark.
func (e *env) amqpSink(name, from string) domain.SinkSpec {
	queue := e.prefix + "." + name
	e.brokers.amqp.CreateQueue(e.b, queue)
	e.b.Cleanup(func() { e.brokers.amqp.DeleteQueue(e.b, queue) })
	return domain.SinkSpec{Name: name, Type: "broker", From: from, Spec: map[string]any{
		"driver":      "amqp",
		"amqp":        map[string]any{"url": e.brokers.amqp.URL},
		"destination": map[string]any{"logs": fmt.Sprintf("'%s'", queue)},
	}}
}

func newEnv(b *testing.B, natsURL string, js jetstream.JetStream, runs *atomic.Int64) *env {
	e := &env{b: b, natsURL: natsURL, js: js, prefix: fmt.Sprintf("bench%d", runs.Add(1))}
	b.Cleanup(func() {
		for _, s := range e.streams {
			_ = js.DeleteStream(context.Background(), s)
		}
	})
	return e
}

func (e *env) stream(suffix string) string {
	name := e.prefix + "-" + suffix
	e.streams = append(e.streams, name)
	return name
}

// natsSink is a broker sink with the NATS driver. TRAM creates no streams, so
// the benchmark sets its stream up first, like a deployment does.
func (e *env) natsSink(name, from string) domain.SinkSpec {
	stream := e.stream(name)
	if _, err := e.js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:      stream,
		Subjects:  []string{stream + ".>"},
		MaxBytes:  256 << 20,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    time.Minute,
	}); err != nil {
		e.b.Fatal(err)
	}
	return domain.SinkSpec{Name: name, Type: "broker", From: from, Spec: map[string]any{
		"driver": "nats",
		"nats":   map[string]any{"url": e.natsURL},
		"destination": map[string]any{
			"logs": fmt.Sprintf(
				"'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')",
				stream,
			),
		},
	}}
}

func startStream(b *testing.B, in domain.SourceSpec, stages []domain.StageSpec, sinks []domain.SinkSpec) {
	logger := slog.New(slog.DiscardHandler)
	c := core.New(
		context.Background(),
		logger,
		source.NewFactory(logger),
		stage.NewFactory(logger),
		sink.NewFactory(logger),
	)
	b.Cleanup(func() { _ = c.Close() })
	_, err := c.UpsertStream(
		context.Background(),
		domain.StreamSpec{Name: "scenario", Source: in, Stages: stages, Sinks: sinks},
	)
	if err != nil {
		b.Fatal(err)
	}
}

func benchmarkOTLP(b *testing.B, e *env, sc scenario, shape otlpgen.Shape, concurrency int) {
	port := freePort(b)
	stages, sinks := sc.build(e)
	startStream(
		b,
		domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(port)}},
		stages,
		sinks,
	)

	body, err := proto.Marshal(otlpgen.Logs(shape))
	if err != nil {
		b.Fatal(err)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/logs", port)
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: concurrency}}
	b.Cleanup(client.CloseIdleConnections)

	send := func() (time.Duration, error) { return postOnce(client, url, body) }
	if _, sendErr := send(); sendErr != nil {
		b.Fatal(sendErr)
	}

	latencies := make([]time.Duration, b.N)
	var next atomic.Int64
	var wg sync.WaitGroup

	b.ReportAllocs()
	b.ResetTimer()
	cpuStart := cpuTime()
	for range concurrency {
		wg.Go(func() { sendUntilDone(b, send, latencies, &next) })
	}
	wg.Wait()
	cpu := cpuTime() - cpuStart
	b.StopTimer()

	report(b, shape, cpu)
	slices.Sort(latencies)
	b.ReportMetric(float64(percentile(latencies, 0.50).Microseconds())/1000, "p50-ms")
	b.ReportMetric(float64(percentile(latencies, 0.99).Microseconds())/1000, "p99-ms")
}

// postOnce posts body to url and returns how long the answer took.
func postOnce(client *http.Client, url string, body []byte) (time.Duration, error) {
	start := time.Now()
	resp, err := client.Post(url, "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %s", resp.Status)
	}
	return time.Since(start), nil
}

// sendUntilDone sends requests until b.N are taken from next and records the
// latency of each at its index.
func sendUntilDone(b *testing.B, send func() (time.Duration, error), latencies []time.Duration, next *atomic.Int64) {
	for {
		i := next.Add(1) - 1
		if i >= int64(b.N) {
			return
		}
		latency, err := send()
		if err != nil {
			b.Error(err)
			return
		}
		latencies[i] = latency
	}
}

func benchmarkNATS(b *testing.B, e *env, sc scenario, shape otlpgen.Shape, concurrency int) {
	ctx := context.Background()
	input := e.stream("in")
	if _, err := e.js.CreateStream(
		ctx,
		jetstream.StreamConfig{Name: input, Subjects: []string{input + ".logs"}},
	); err != nil {
		b.Fatal(err)
	}
	_, payload, err := (&domain.Event{Logs: otlpgen.Logs(shape)}).MarshalOTLPData()
	if err != nil {
		b.Fatal(err)
	}
	publish(b, e.js, input+".logs", payload, b.N)

	stages, sinks := sc.build(e)
	output := e.streams[len(e.streams)-1]
	want := uint64(b.N * shape.Resources)

	b.ReportAllocs()
	b.ResetTimer()
	cpuStart := cpuTime()
	// Building the stream is timed too, because it starts consuming at once.
	// It takes milliseconds, against about a second of benchmark time.
	startStream(b, domain.SourceSpec{Name: "in", Type: "broker", Spec: map[string]any{
		"driver":  "nats",
		"signal":  "logs",
		"workers": float64(concurrency),
		"nats":    map[string]any{"url": e.natsURL, "stream": input, "subject": input + ".logs", "durable": input},
	}}, stages, sinks)
	waitForMessages(b, e.js, output, want)
	cpu := cpuTime() - cpuStart
	b.StopTimer()

	report(b, shape, cpu)
}

func report(b *testing.B, shape otlpgen.Shape, cpu time.Duration) {
	records := float64(shape.Records * b.N)
	b.ReportMetric(records/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
	b.ReportMetric(float64(cpu.Nanoseconds())/records, "cpu-ns/record")
	b.ReportMetric(records/cpu.Seconds(), "records/s/core")
}

func cpuTime() time.Duration {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(float64(len(sorted))*p))]
}

func freePort(b *testing.B) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// publish sends payload n times as plain OTLP, as the broker sink does.
func publish(b *testing.B, js jetstream.JetStream, subject string, payload []byte, n int) {
	b.Helper()
	acks := make([]jetstream.PubAckFuture, 0, n)
	for range n {
		ack, err := js.PublishMsgAsync(&nats.Msg{Subject: subject, Data: payload})
		if err != nil {
			b.Fatal(err)
		}
		acks = append(acks, ack)
	}
	for _, ack := range acks {
		select {
		case <-ack.Ok():
		case err := <-ack.Err():
			b.Fatal(err)
		}
	}
}

func waitForMessages(b *testing.B, js jetstream.JetStream, stream string, want uint64) {
	b.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		s, err := js.Stream(ctx, stream)
		if err == nil {
			info, infoErr := s.Info(ctx)
			if infoErr == nil && info.State.Msgs >= want {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatalf("stream %s did not receive %d messages", stream, want)
}

// celFilter is the spec of a filter stage with the CEL driver that keeps the
// log records expression keeps.
func celFilter(expression string) map[string]any {
	return map[string]any{"driver": "cel", "cel": map[string]any{"logs": expression}}
}
