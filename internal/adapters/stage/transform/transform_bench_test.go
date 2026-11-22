// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package transform_test

import (
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"
	"go.opentelemetry.io/otel"

	"github.com/go-tram/tram/internal/adapters/stage/transform"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

// BenchmarkNotifyLogs measures the stage on the shapes of the value map
// benchmark, so the two can be compared: one statement per record, the
// statements of a typical normalization, and one statement per resource.
func BenchmarkNotifyLogs(b *testing.B) {
	specs := []struct {
		name  string
		stmts []map[string]any
	}{
		{name: "set", stmts: []map[string]any{
			{"set": "attributes['team.id']", "value": "attr(record.attributes, 'team')"},
		}},
		{name: "normalize", stmts: []map[string]any{
			{"set": "attributes['team.id']", "value": "attr(record.attributes, 'team')"},
			{"delete": "attributes['attr-0']"},
			{"rename": "attributes['attr-1']", "to": "attribute.one"},
			{"truncate": "attributes['attr-2']", "length": float64(4)},
		}},
		{name: "resource", stmts: []map[string]any{
			{
				"set":   "resource.attributes['service.id']",
				"value": "attr(resource.attributes, 'service.name').upperAscii()",
			},
		}},
	}
	for _, spec := range specs {
		stage, err := transform.New(
			domain.StageSpec{Name: "normalize", From: "source", Spec: celSpec("", "logs", spec.stmts...)},
			validator.New(), slog.New(slog.DiscardHandler), otel.GetMeterProvider(), "stream",
		)
		if err != nil {
			b.Fatal(err)
		}
		for _, shape := range otlpgen.Shapes() {
			shape.Teams = 2
			event := &domain.Event{Logs: otlpgen.Logs(shape)}

			b.Run(spec.name+"/"+shape.String(), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if notifyErr := stage.Notify(b.Context(), event); notifyErr != nil {
						b.Fatal(notifyErr)
					}
				}
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}
