// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/go-playground/validator/v10"

	sourcebase "github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

type benchSource struct {
	*sourcebase.Source[struct{}]

	shutdown chan struct{}
}

func (s *benchSource) Run(ctx context.Context) error {
	select {
	case <-s.shutdown:
	case <-ctx.Done():
	}
	return nil
}

func (s *benchSource) Stop() error {
	close(s.shutdown)
	return nil
}

type benchSourceFactory struct{ src *benchSource }

func (f benchSourceFactory) New(domain.SourceSpec, string) (domain.Source, error) { return f.src, nil }

type countingSink struct {
	name, from string
	records    *atomic.Int64
}

func (s *countingSink) Name() string { return s.name }
func (s *countingSink) From() string { return s.from }
func (s *countingSink) Stop() error  { return nil }
func (s *countingSink) Notify(_ context.Context, e *domain.Event) error {
	if e.Logs == nil {
		return nil
	}
	n := 0
	for _, rl := range e.Logs.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	s.records.Add(int64(n))
	return nil
}

type countingSinkFactory struct{ records *atomic.Int64 }

func (f countingSinkFactory) New(spec domain.SinkSpec, _ string) (domain.Sink, error) {
	return &countingSink{name: spec.Name, from: spec.From, records: f.records}, nil
}

type topology struct {
	name   string
	stages []domain.StageSpec
	sinks  int
}

func noopChain(n int) []domain.StageSpec {
	stages := make([]domain.StageSpec, n)
	from := "in"
	for i := range stages {
		name := fmt.Sprintf("noop-%d", i)
		stages[i] = domain.StageSpec{Name: name, Type: "noop", From: from}
		from = name
	}
	return stages
}

func topologies() []topology {
	return []topology{
		{name: "direct", sinks: 1},
		{name: "noop", stages: noopChain(1), sinks: 1},
		{name: "noop-x5", stages: noopChain(5), sinks: 1},
		{name: "cel-record-attribute", stages: []domain.StageSpec{{
			Name: "cel", Type: "validation", From: "in",
			Spec: map[string]any{
				"driver":      "cel",
				"on_mismatch": "drop",
				"cel": map[string]any{
					"logs": "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(l, l.attributes.exists(a, a.key == 'team'))))",
				},
			},
		}}, sinks: 1},
		{name: "fanout-4-sinks", sinks: 4},
	}
}

func (t topology) spec() domain.StreamSpec {
	spec := domain.StreamSpec{
		Name:   "bench",
		Source: domain.SourceSpec{Name: "in", Type: "bench"},
		Stages: t.stages,
	}
	last := "in"
	if len(t.stages) > 0 {
		last = t.stages[len(t.stages)-1].Name
	}
	for i := range t.sinks {
		spec.Sinks = append(spec.Sinks, domain.SinkSpec{Name: fmt.Sprintf("out-%d", i), Type: "counting", From: last})
	}
	return spec
}

func BenchmarkPipeline(b *testing.B) {
	for _, topo := range topologies() {
		for _, shape := range otlpgen.Shapes() {
			b.Run(topo.name+"/"+shape.String(), func(b *testing.B) {
				pipeline(b, topo.spec(), topo.sinks, shape)
			})
		}
	}
}

func pipeline(b *testing.B, spec domain.StreamSpec, sinks int, shape otlpgen.Shape) {
	src := &benchSource{shutdown: make(chan struct{})}
	base, err := sourcebase.New[struct{}]("in", nil, validator.New())
	if err != nil {
		b.Fatal(err)
	}
	src.Source = base

	var records atomic.Int64
	c := core.New(
		b.Context(),
		slog.New(slog.DiscardHandler),
		benchSourceFactory{src},
		stage.NewFactory(slog.New(slog.DiscardHandler)),
		countingSinkFactory{&records},
	)
	b.Cleanup(func() { _ = c.Close(context.Background()) })
	if _, upsertErr := c.UpsertStream(b.Context(), spec); upsertErr != nil {
		b.Fatal(upsertErr)
	}

	event := &domain.Event{Logs: otlpgen.Logs(shape)}
	b.ReportAllocs()
	cpu := benchcpu.Start()
	for b.Loop() {
		if notifyErr := src.NotifySubscribers(b.Context(), event); notifyErr != nil {
			b.Fatal(notifyErr)
		}
	}
	cpu.Report(b)

	b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
	if want := int64(shape.Records * b.N * sinks); records.Load() != want {
		b.Fatalf("sinks received %d records, want %d", records.Load(), want)
	}
}
