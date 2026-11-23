// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func keepRatios() []struct {
	name string
	keep func(team string) bool
} {
	return []struct {
		name string
		keep func(team string) bool
	}{
		{name: "keep-all", keep: func(string) bool { return true }},
		{name: "keep-half", keep: func(team string) bool { return team == "team-0" }},
		{name: "keep-none", keep: func(string) bool { return false }},
	}
}

func BenchmarkFilter(b *testing.B) {
	for _, ratio := range keepRatios() {
		for _, shape := range otlpgen.Shapes() {
			shape.Teams = 2

			logs := otlpgen.Logs(shape)
			b.Run("logs/"+ratio.name+"/"+shape.String(), func(b *testing.B) {
				benchmarkFilter(b, shape, func() (any, error) {
					return domain.FilterLogs(
						logs,
						func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, r *logspb.LogRecord) (bool, error) {
							return ratio.keep(team(r.GetAttributes())), nil
						},
					)
				})
			})

			metrics := otlpgen.Metrics(shape)
			b.Run("metrics/"+ratio.name+"/"+shape.String(), func(b *testing.B) {
				benchmarkFilter(b, shape, func() (any, error) {
					return domain.FilterMetrics(
						metrics,
						func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, _ *metricspb.Metric, dp proto.Message) (bool, error) {
							return ratio.keep(team(dp.(*metricspb.NumberDataPoint).GetAttributes())), nil
						},
					)
				})
			})

			traces := otlpgen.Traces(shape)
			b.Run("traces/"+ratio.name+"/"+shape.String(), func(b *testing.B) {
				benchmarkFilter(b, shape, func() (any, error) {
					return domain.FilterSpans(
						traces,
						func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, s *tracepb.Span) (bool, error) {
							return ratio.keep(team(s.GetAttributes())), nil
						},
					)
				})
			})
		}
	}
}

func benchmarkFilter(b *testing.B, shape otlpgen.Shape, filter func() (any, error)) {
	b.ReportAllocs()
	cpu := benchcpu.Start()
	for b.Loop() {
		if _, err := filter(); err != nil {
			b.Fatal(err)
		}
	}
	cpu.Report(b)
	b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
}
