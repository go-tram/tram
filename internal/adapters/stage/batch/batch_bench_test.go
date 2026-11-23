// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package batch_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/stage/batch"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

type countingSubscriber struct {
	batches atomic.Int64
}

func (c *countingSubscriber) Name() string { return "counter" }
func (c *countingSubscriber) From() string { return "batch" }
func (c *countingSubscriber) Stop() error  { return nil }
func (c *countingSubscriber) Notify(context.Context, *domain.Event) error {
	c.batches.Add(1)
	return nil
}

func newBenchBatch(b *testing.B, spec map[string]any) (*batch.Batch, *countingSubscriber) {
	b.Helper()
	logger := slog.Default()
	batch, err := batch.New(domain.StageSpec{Name: "batch", From: "source", Spec: spec}, validator.New(), logger)
	if err != nil {
		b.Fatal(err)
	}
	sub := &countingSubscriber{}
	_ = batch.Subscribe(sub)
	b.Cleanup(func() { _ = batch.Stop() })
	return batch, sub
}

func BenchmarkBatch(b *testing.B) {
	for _, shape := range otlpgen.Shapes() {
		event := &domain.Event{Logs: otlpgen.Logs(shape)}
		b.Run("flush-per-event/"+shape.String(), func(b *testing.B) { benchFlushPerEvent(b, shape, event) })
		b.Run("parallel-8-events/"+shape.String(), func(b *testing.B) { benchParallelEvents(b, shape, event) })
	}
}

func benchFlushPerEvent(b *testing.B, shape otlpgen.Shape, event *domain.Event) {
	batch, _ := newBenchBatch(b, map[string]any{"max_records": shape.Records, "timeout": "1h"})
	b.ReportAllocs()
	cpu := benchcpu.Start()
	for b.Loop() {
		if err := batch.Notify(b.Context(), event); err != nil {
			b.Fatal(err)
		}
	}
	cpu.Report(b)
	b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
}

func benchParallelEvents(b *testing.B, shape otlpgen.Shape, event *domain.Event) {
	batch, sub := newBenchBatch(b, map[string]any{"max_records": 8 * shape.Records, "timeout": "1ms"})
	b.SetParallelism(8)
	b.ReportAllocs()
	cpu := benchcpu.Start()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := batch.Notify(context.Background(), event); err != nil {
				b.Error(err)
				return
			}
		}
	})
	cpu.Report(b)
	b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(float64(b.N)/float64(max(1, sub.batches.Load())), "events/batch")
}
