// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package encryption_test

import (
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/stage/encryption"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func BenchmarkNotifyLogs(b *testing.B) {
	variants := []struct {
		name string
		spec map[string]any
	}{
		{name: "body-only", spec: localSpec()},
		{name: "body-plus-attributes", spec: localSpec("team", "attr-0", "attr-1", "attr-2", "attr-3")},
	}

	for _, variant := range variants {
		stage, err := encryption.New(
			domain.StageSpec{Name: "enc", From: "source", Spec: variant.spec},
			validator.New(),
			slog.Default(),
		)
		if err != nil {
			b.Fatal(err)
		}

		for _, shape := range otlpgen.Shapes() {
			shape.Teams = 2
			event := &domain.Event{Logs: otlpgen.Logs(shape)}

			b.Run(variant.name+"/"+shape.String(), func(b *testing.B) {
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
