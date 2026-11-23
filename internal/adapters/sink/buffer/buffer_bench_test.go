// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package buffer_test

import (
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/sink/buffer"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

// benchQueueBytes lets the queue hold everything a benchmark writes.
const benchQueueBytes = 1 << 32

func newBenchSink(b *testing.B) *buffer.Sink {
	b.Helper()
	sink, err := buffer.New(domain.SinkSpec{Name: "outbox", From: "in", Spec: map[string]any{
		"driver":    "sqlite",
		"sqlite":    map[string]any{},
		"queue":     "bench",
		"max_bytes": float64(benchQueueBytes),
	}}, validator.New(validator.WithRequiredStructEnabled()), slog.New(slog.DiscardHandler), b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = sink.Stop() })
	return sink
}

// Stores events in a SQLite queue in a temporary directory. Every commit is
// synced to disk, so the numbers depend on the disk of the machine. One
// caller pays a sync per event; concurrent callers share a commit.
func BenchmarkNotify(b *testing.B) {
	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
	}
	for _, shape := range shapes {
		event := &domain.Event{Logs: otlpgen.Logs(shape)}
		b.Run("serial/"+shape.String(), func(b *testing.B) {
			sink := newBenchSink(b)
			b.ReportAllocs()
			for b.Loop() {
				if err := sink.Notify(b.Context(), event); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
		})
		b.Run("parallel/"+shape.String(), func(b *testing.B) {
			sink := newBenchSink(b)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := sink.Notify(b.Context(), event); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
