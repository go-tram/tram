// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cel_test

import (
	"testing"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func BenchmarkCELValidator(b *testing.B) {
	expressions := []struct {
		name string
		logs string
	}{
		{name: "empty", logs: ""},
		{
			name: "resource-attribute",
			logs: "logs.resource_logs.all(r, r.resource.attributes.exists(a, a.key == 'service.name' && a.value.string_value.startsWith('service-')))",
		},
		{
			name: "record-attribute",
			logs: "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(l, l.attributes.exists(a, a.key == 'team'))))",
		},
		{
			name: "record-attribute-drop",
			logs: "logs.resource_logs.all(r, r.scope_logs.all(s, s.log_records.all(l, l.attributes.exists(a, a.key == 'missing'))))",
		},
	}

	for _, expr := range expressions {
		stage, err := newStage("cel", map[string]any{"logs": expr.logs, "on_mismatch": "drop"})
		if err != nil {
			b.Fatal(err)
		}

		for _, shape := range otlpgen.Shapes() {
			event := &domain.Event{Logs: otlpgen.Logs(shape)}
			b.Run(expr.name+"/"+shape.String(), func(b *testing.B) {
				b.ReportAllocs()
				cpu := benchcpu.Start()
				for b.Loop() {
					if notifyErr := stage.Notify(b.Context(), event); notifyErr != nil {
						b.Fatal(notifyErr)
					}
				}
				cpu.Report(b)
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}
