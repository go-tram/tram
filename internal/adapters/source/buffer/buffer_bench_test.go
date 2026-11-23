// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package buffer_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"

	sinkbuffer "github.com/go-tram/tram/internal/adapters/sink/buffer"
	"github.com/go-tram/tram/internal/adapters/source/buffer"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
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

// Delivers events a buffer sink stored in a SQLite queue beforehand. Every
// delivered event is removed from the queue, which writes to disk as well.
func BenchmarkConsume(b *testing.B) {
	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
	}
	for _, workers := range []int{1, 8} {
		for _, shape := range shapes {
			b.Run(fmt.Sprintf("workers=%d/%s", workers, shape), func(b *testing.B) {
				consume(b, workers, shape)
			})
		}
	}
}

func consume(b *testing.B, workers int, shape otlpgen.Shape) {
	dir := b.TempDir()
	v := validator.New(validator.WithRequiredStructEnabled())
	logger := slog.New(slog.DiscardHandler)

	sink, err := sinkbuffer.New(domain.SinkSpec{Name: "outbox", From: "in", Spec: map[string]any{
		"driver": "sqlite", "sqlite": map[string]any{}, "queue": "bench", "max_bytes": float64(1 << 32),
	}}, v, logger, dir)
	if err != nil {
		b.Fatal(err)
	}
	event := &domain.Event{Logs: otlpgen.Logs(shape)}
	var wg sync.WaitGroup
	for range b.N {
		wg.Go(func() {
			if notifyErr := sink.Notify(context.Background(), event); notifyErr != nil {
				b.Error(notifyErr)
			}
		})
	}
	wg.Wait()
	if err = sink.Stop(); err != nil {
		b.Fatal(err)
	}

	src, err := buffer.New(domain.SourceSpec{Name: "outbox", Spec: map[string]any{
		"driver": "sqlite", "sqlite": map[string]any{}, "queue": "bench", "workers": float64(workers),
	}}, v, logger, dir)
	if err != nil {
		b.Fatal(err)
	}
	counter := &countingSubscriber{want: int64(shape.Records * b.N), done: make(chan struct{})}
	_ = src.Subscribe(counter)

	b.ReportAllocs()
	b.ResetTimer()
	cpu := benchcpu.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
}
