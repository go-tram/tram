// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker_test

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	sinkbroker "github.com/go-tram/tram/internal/adapters/sink/broker"
	"github.com/go-tram/tram/internal/adapters/source/broker"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/amqptest"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/kafkatest"
	"github.com/go-tram/tram/internal/testutil/natstest"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

type countingSubscriber struct {
	records atomic.Int64
	want    int64
	done    chan struct{}
	once    sync.Once
}

func (c *countingSubscriber) Name() string { return "counter" }
func (c *countingSubscriber) From() string { return "in" }
func (c *countingSubscriber) Stop() error  { return nil }
func (c *countingSubscriber) Notify(_ context.Context, e *domain.Event) error {
	if c.records.Add(int64(e.RecordCount())) >= c.want {
		c.once.Do(func() { close(c.done) })
	}
	return nil
}

// Receives OTLP messages published to NATS beforehand.
func BenchmarkConsume(b *testing.B) {
	url := natstest.Start(b)
	nc, err := nats.Connect(url)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}

	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
	}
	run := 0
	for _, workers := range []int{1, 8} {
		for _, shape := range shapes {
			_, payload, marshalErr := (&domain.Event{Logs: otlpgen.Logs(shape)}).MarshalOTLPData()
			if marshalErr != nil {
				b.Fatal(marshalErr)
			}
			b.Run(fmt.Sprintf("workers=%d/%s", workers, shape), func(b *testing.B) {
				run++
				consume(b, js, url, fmt.Sprintf("consume-%d", run), workers, shape.Records, payload)
			})
		}
	}
}

func consume(b *testing.B, js jetstream.JetStream, url, name string, workers, records int, payload []byte) {
	subject := name + ".logs"
	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subject}}); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = js.DeleteStream(context.Background(), name) })
	publish(b, js, subject, payload, b.N)

	v := validator.New(validator.WithRequiredStructEnabled())
	src, err := broker.New(domain.SourceSpec{Name: "broker", Spec: map[string]any{
		"driver":  "nats",
		"signal":  "logs",
		"workers": float64(workers),
		"nats":    map[string]any{"url": url, "stream": name, "subject": subject},
	}}, v, slog.New(slog.DiscardHandler))
	if err != nil {
		b.Fatal(err)
	}
	counter := &countingSubscriber{want: int64(records * b.N), done: make(chan struct{})}
	_ = src.Subscribe(counter)

	b.ReportAllocs()
	b.ResetTimer()
	cpu := benchcpu.Start()
	stopped := make(chan error, 1)
	go func() { stopped <- src.Run(ctx) }()
	select {
	case <-counter.done:
	case <-time.After(time.Minute):
		b.Fatalf("received %d of %d records", counter.records.Load(), counter.want)
	}
	cpu.Report(b)
	b.StopTimer()

	_ = src.Stop()
	if runErr := <-stopped; runErr != nil {
		b.Fatal(runErr)
	}
	b.ReportMetric(float64(records*b.N)/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "messages/s")
}

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

// Receives OTLP messages published to a Kafka topic beforehand.
func BenchmarkConsumeKafka(b *testing.B) {
	brokers := kafkatest.Start(b)
	seeds := make([]any, len(brokers))
	for i, s := range brokers {
		seeds[i] = s
	}
	run := 0
	benchmarkConsume(b, func(b *testing.B, workers int, records int, payload []byte) {
		run++
		topic := fmt.Sprintf("consume-%d", run)
		kafkatest.CreateTopic(b, brokers, topic, 1)
		publishWithSink(
			b,
			map[string]any{"driver": "kafka", "kafka": map[string]any{"brokers": seeds}},
			topic,
			payload,
			b.N,
		)
		consumeWith(b, map[string]any{
			"driver": "kafka", "signal": "logs", "workers": float64(workers),
			"kafka": map[string]any{"brokers": seeds, "topic": topic, "group": topic},
		}, records)
	})
}

// Receives OTLP messages published to an ActiveMQ Artemis queue beforehand.
func BenchmarkConsumeAMQP(b *testing.B) {
	artemis := amqptest.Start(b)
	run := 0
	benchmarkConsume(b, func(b *testing.B, workers int, records int, payload []byte) {
		run++
		queue := fmt.Sprintf("consume.%d", run)
		artemis.CreateQueue(b, queue)
		publishWithSink(
			b,
			map[string]any{"driver": "amqp", "amqp": map[string]any{"url": artemis.URL}},
			queue,
			payload,
			b.N,
		)
		consumeWith(b, map[string]any{
			"driver": "amqp", "signal": "logs", "workers": float64(workers),
			"amqp": map[string]any{"url": artemis.URL, "address": queue},
		}, records)
	})
}

// benchmarkConsume runs run for the shapes and worker counts of all brokers,
// with the payload of the shape.
func benchmarkConsume(b *testing.B, run func(b *testing.B, workers, records int, payload []byte)) {
	b.Helper()
	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
	}
	for _, workers := range []int{1, 8} {
		for _, shape := range shapes {
			_, payload, err := (&domain.Event{Logs: otlpgen.Logs(shape)}).MarshalOTLPData()
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("workers=%d/%s", workers, shape), func(b *testing.B) {
				run(b, workers, shape.Records, payload)
			})
		}
	}
}

// publishWithSink publishes n messages with the payload to destination
// through a broker sink with the driver spec, as a producer outside TRAM
// would, and waits until the broker has them.
func publishWithSink(b *testing.B, driverSpec map[string]any, destination string, payload []byte, n int) {
	b.Helper()
	spec := map[string]any{"destination": map[string]any{"logs": fmt.Sprintf("'%s'", destination)}}
	maps.Copy(spec, driverSpec)
	sink, err := sinkbroker.New(domain.SinkSpec{Name: "publish", From: "in", Spec: spec},
		validator.New(validator.WithRequiredStructEnabled()), slog.New(slog.DiscardHandler))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sink.Stop() }()
	event, err := domain.UnmarshalOTLPData(domain.SignalLogs, payload)
	if err != nil {
		b.Fatal(err)
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, publishConcurrency)
	for range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if notifyErr := sink.Notify(context.Background(), event); notifyErr != nil {
				b.Error(notifyErr)
			}
		})
	}
	wg.Wait()
}

// publishConcurrency bounds the messages publishWithSink sends at once.
const publishConcurrency = 64

// consumeWith runs a broker source with the spec until it delivered records
// records per message for b.N messages, timing only the delivery.
func consumeWith(b *testing.B, spec map[string]any, records int) {
	b.Helper()
	src, err := broker.New(domain.SourceSpec{Name: "broker", Spec: spec},
		validator.New(validator.WithRequiredStructEnabled()), slog.New(slog.DiscardHandler))
	if err != nil {
		b.Fatal(err)
	}
	counter := &countingSubscriber{want: int64(records * b.N), done: make(chan struct{})}
	_ = src.Subscribe(counter)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	cpu := benchcpu.Start()
	stopped := make(chan error, 1)
	go func() { stopped <- src.Run(ctx) }()
	select {
	case <-counter.done:
	case <-time.After(time.Minute):
		b.Fatalf("received %d of %d records", counter.records.Load(), counter.want)
	}
	cpu.Report(b)
	b.StopTimer()

	_ = src.Stop()
	if runErr := <-stopped; runErr != nil {
		b.Fatal(runErr)
	}
	b.ReportMetric(float64(records*b.N)/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "messages/s")
}
