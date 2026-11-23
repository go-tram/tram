// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package valuemap_test

import (
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"
	"go.opentelemetry.io/otel"

	"github.com/go-tram/tram/internal/adapters/stage/valuemap"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func BenchmarkNotifyLogs(b *testing.B) {
	sources := []struct {
		name string
		spec map[string]any
	}{
		{
			name: "hit",
			spec: map[string]any{
				"source_attr": "team",
				"target_attr": "team.id",
				"default":     "unknown",
				"table":       map[string]string{"team-0": "cart", "team-1": "checkout"},
			},
		},
		{
			name: "no-entry",
			spec: map[string]any{
				"source_attr": "team",
				"target_attr": "team.id",
				"default":     "unknown",
				"table":       map[string]string{"other": "cart"},
			},
		},
		{
			name: "missing-attr",
			spec: map[string]any{
				"source_attr": "customer",
				"target_attr": "customer.id",
				"default":     "unknown",
				"table":       map[string]string{"team-0": "cart", "team-1": "checkout"},
			},
		},
		{
			name: "resource-level",
			spec: map[string]any{
				"source_attr": "service.name",
				"target_attr": "service.id",
				"default":     "unknown",
				"level":       "resource",
				"table":       map[string]string{"service-0": "shop"},
			},
		},
	}

	for _, source := range sources {
		logger := slog.Default()
		mapper, err := valuemap.New(
			domain.StageSpec{Name: "mapper", From: "source", Spec: source.spec},
			validator.New(),
			logger,
			otel.GetMeterProvider(),
			"stream",
		)
		if err != nil {
			b.Fatal(err)
		}

		for _, shape := range otlpgen.Shapes() {
			shape.Teams = 2
			event := &domain.Event{Logs: otlpgen.Logs(shape)}

			b.Run(source.name+"/"+shape.String(), func(b *testing.B) {
				b.ReportAllocs()
				cpu := benchcpu.Start()
				for b.Loop() {
					if notifyErr := mapper.Notify(b.Context(), event); notifyErr != nil {
						b.Fatal(notifyErr)
					}
				}
				cpu.Report(b)
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}
