// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/sink/otlp"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

// Sends events to a local HTTP server that reads the body and answers 200,
// so the numbers are the sink's own work: encoding the request and the HTTP
// round trip over loopback, without the work of a real backend.
func BenchmarkNotify(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	b.Cleanup(server.Close)

	shapes := []otlpgen.Shape{
		{Records: 1, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 100, Resources: 1, Teams: 1, Attributes: 4},
		{Records: 1000, Resources: 10, Teams: 1, Attributes: 4},
	}
	for _, shape := range shapes {
		sink, err := otlp.New(domain.SinkSpec{Name: "otlp", From: "in", Spec: map[string]any{"url": server.URL}},
			validator.New(validator.WithRequiredStructEnabled()), slog.New(slog.DiscardHandler))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = sink.Stop() })
		event := &domain.Event{Logs: otlpgen.Logs(shape)}

		b.Run(shape.String(), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if notifyErr := sink.Notify(b.Context(), event); notifyErr != nil {
					b.Fatal(notifyErr)
				}
			}
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
