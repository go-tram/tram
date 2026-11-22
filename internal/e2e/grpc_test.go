// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/go-tram/tram/internal/core/domain"
)

// An OpenTelemetry SDK exports over gRPC to an OTLP source. The sink fails the
// first delivery, so the source answers Unavailable with RetryInfo; the
// exporter retries after the delay and the span arrives once.
func TestOTLPOverGRPCFromTheOpenTelemetrySDK(t *testing.T) {
	t.Parallel()

	p := newEmptyPipeline(t, domain.SourceSpec{})
	port := onFreePort(t, 0, func(port int) error {
		p.in = domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{
			"grpc_port":   float64(port),
			"retry_after": "1s",
		}}
		return p.tryUpdate(nil, []domain.SinkSpec{{
			Name: "out", Type: captureType, From: "in",
			Spec: map[string]any{"fail_first": float64(1)},
		}})
	})

	exporter, err := otlptracegrpc.New(t.Context(),
		otlptracegrpc.WithEndpoint(fmt.Sprintf("127.0.0.1:%d", port)),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     2 * time.Second,
			MaxElapsedTime:  20 * time.Second,
		}),
	)
	require.NoError(t, err)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	start := time.Now()
	_, span := provider.Tracer("e2e").Start(t.Context(), "checkout")
	span.End()
	require.NoError(t, provider.ForceFlush(t.Context()))

	spans := p.sinks.sink(t, "out").received()
	require.Len(t, spans, 1, "the retried export arrives once")
	got := spans[0].Traces.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
	assert.Equal(t, "checkout", got.GetName())
	assert.GreaterOrEqual(t, time.Since(start), time.Second,
		"the exporter waited for the RetryInfo delay before retrying")
}
