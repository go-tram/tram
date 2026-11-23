// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain_test

import (
	"testing"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func BenchmarkOTLPDataRoundTrip(b *testing.B) {
	signals := []struct {
		name  string
		event func(otlpgen.Shape) *domain.Event
	}{
		{name: "logs", event: func(s otlpgen.Shape) *domain.Event { return &domain.Event{Logs: otlpgen.Logs(s)} }},
		{
			name:  "metrics",
			event: func(s otlpgen.Shape) *domain.Event { return &domain.Event{Metrics: otlpgen.Metrics(s)} },
		},
		{
			name:  "traces",
			event: func(s otlpgen.Shape) *domain.Event { return &domain.Event{Traces: otlpgen.Traces(s)} },
		},
	}

	for _, signal := range signals {
		for _, shape := range otlpgen.Shapes() {
			event := signal.event(shape)
			_, data, err := event.MarshalOTLPData()
			if err != nil {
				b.Fatal(err)
			}

			b.Run(signal.name+"/"+shape.String(), func(b *testing.B) {
				benchRoundTrip(b, event, shape.Records, len(data))
			})
		}
	}
}

func benchRoundTrip(b *testing.B, event *domain.Event, records, size int) {
	b.SetBytes(int64(size))
	b.ReportAllocs()
	cpu := benchcpu.Start()
	for b.Loop() {
		sig, encoded, err := event.MarshalOTLPData()
		if err != nil {
			b.Fatal(err)
		}
		if _, err = domain.UnmarshalOTLPData(sig, encoded); err != nil {
			b.Fatal(err)
		}
	}
	cpu.Report(b)
	b.ReportMetric(float64(records*b.N)/b.Elapsed().Seconds(), "records/s")
}
