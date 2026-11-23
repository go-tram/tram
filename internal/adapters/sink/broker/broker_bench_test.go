// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker_test

import (
	"fmt"
	"log/slog"
	"maps"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-tram/tram/internal/adapters/sink/broker"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/amqptest"
	"github.com/go-tram/tram/internal/testutil/kafkatest"
	"github.com/go-tram/tram/internal/testutil/natstest"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

// benchTeams is how many teams, and so destinations, the largest shape has.
const benchTeams = 10

// Publishes to NATS through the NATS driver, one subject per team.
func BenchmarkNotify(b *testing.B) {
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
	if _, err = js.CreateStream(b.Context(), jetstream.StreamConfig{
		Name: "bench", Subjects: []string{"bench.>"}, MaxBytes: 256 << 20, MaxAge: 60e9,
	}); err != nil {
		b.Fatal(err)
	}
	benchmarkNotify(b, map[string]any{"driver": "nats", "nats": map[string]any{"url": url}}, "'bench.' + ")
}

// Publishes to Kafka through the Kafka driver, one topic per team.
func BenchmarkNotifyKafka(b *testing.B) {
	brokers := kafkatest.Start(b)
	for i := range benchTeams {
		kafkatest.CreateTopic(b, brokers, fmt.Sprintf("bench-team-%d", i), 1)
	}
	seeds := make([]any, len(brokers))
	for i, s := range brokers {
		seeds[i] = s
	}
	benchmarkNotify(b, map[string]any{"driver": "kafka", "kafka": map[string]any{"brokers": seeds}}, "'bench-' + ")
}

// Publishes to ActiveMQ Artemis through the AMQP 1.0 driver, one queue per
// team.
func BenchmarkNotifyAMQP(b *testing.B) {
	artemis := amqptest.Start(b)
	for i := range benchTeams {
		artemis.CreateQueue(b, fmt.Sprintf("bench.team-%d", i))
	}
	benchmarkNotify(b, map[string]any{"driver": "amqp", "amqp": map[string]any{"url": artemis.URL}}, "'bench.' + ")
}

// benchmarkNotify measures the sink with the driver spec on the shapes of all
// brokers; prefix is the CEL expression that starts each destination, which
// ends with the record's team.
func benchmarkNotify(b *testing.B, driverSpec map[string]any, prefix string) {
	b.Helper()
	v := validator.New(validator.WithRequiredStructEnabled())
	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: benchTeams, Attributes: 4},
	}
	for _, shape := range shapes {
		spec := map[string]any{"destination": map[string]any{
			"logs": prefix + "logs.resource_logs[0].scope_logs[0].log_records[0].attributes.get_attr('team')",
		}}
		maps.Copy(spec, driverSpec)
		sink, newErr := broker.New(
			domain.SinkSpec{Name: "broker", From: "in", Spec: spec},
			v,
			slog.New(slog.DiscardHandler),
		)
		if newErr != nil {
			b.Fatal(newErr)
		}
		b.Cleanup(func() { _ = sink.Stop() })

		event := &domain.Event{Logs: otlpgen.Logs(shape)}
		b.Run(shape.String(), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if notifyErr := sink.Notify(b.Context(), event); notifyErr != nil {
					b.Fatal(notifyErr)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(shape.Teams*b.N)/b.Elapsed().Seconds(), "messages/s")
		})
	}
}
