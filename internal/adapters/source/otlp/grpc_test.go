// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/core/domain"
)

// syncRecorder records events like recorder, safe for the server's goroutines.
type syncRecorder struct {
	mu     sync.Mutex
	events []*domain.Event
	err    error
	delay  time.Duration
}

func (r *syncRecorder) Name() string { return "recorder" }
func (r *syncRecorder) From() string { return "otlp" }
func (r *syncRecorder) Stop() error  { return nil }

func (r *syncRecorder) Notify(ctx context.Context, e *domain.Event) error {
	if r.delay > 0 {
		select {
		case <-time.After(r.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return r.err
}

func (r *syncRecorder) received() []*domain.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*domain.Event(nil), r.events...)
}

// startSource runs an OTLP source with the spec until the test ends.
func startSource(t *testing.T, spec map[string]any, rec *syncRecorder) {
	t.Helper()
	src, err := otlp.New(domain.SourceSpec{Name: "otlp", Type: otlp.Type, Spec: spec},
		validator.New(), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, src.Subscribe(rec))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(context.Background())
	}()
	t.Cleanup(func() {
		require.NoError(t, src.Stop())
		<-done
	})
}

func grpcSource(t *testing.T, rec *syncRecorder, extra map[string]any) *grpc.ClientConn {
	t.Helper()
	port := freePort(t)
	spec := map[string]any{"grpc_port": float64(port)}
	maps.Copy(spec, extra)
	startSource(t, spec, rec)
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func logsRequest(body string) *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
		}}}},
	}}}
}

func TestGRPCExportsEverySignal(t *testing.T) {
	t.Parallel()
	rec := &syncRecorder{}
	conn := grpcSource(t, rec, nil)
	ctx := t.Context()

	_, err := collogspb.NewLogsServiceClient(conn).Export(ctx, logsRequest("hello"))
	require.NoError(t, err)
	_, err = colmetricspb.NewMetricsServiceClient(conn).Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{Name: "requests"}},
		}}}},
	})
	require.NoError(t, err)
	_, err = coltracepb.NewTraceServiceClient(conn).Export(ctx, &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{Name: "checkout"}},
		}}}},
	})
	require.NoError(t, err)

	events := rec.received()
	require.Len(t, events, 3)
	assert.Equal(
		t,
		"hello",
		events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0].GetBody().GetStringValue(),
	)
	assert.Equal(
		t,
		"requests",
		events[1].Metrics.GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()[0].GetName(),
	)
	assert.Equal(t, "checkout", events[2].Traces.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0].GetName())
	for _, e := range events {
		require.NotNil(t, e.Request, "the request size is recorded")
		assert.Equal(t, "identity", e.Request.Encoding)
		assert.Positive(t, e.Request.DecodedBytes)
		assert.Equal(t, e.Request.DecodedBytes, e.Request.WireBytes)
	}
}

func TestGRPCAcceptsGzip(t *testing.T) {
	t.Parallel()
	rec := &syncRecorder{}
	conn := grpcSource(t, rec, nil)

	_, err := collogspb.NewLogsServiceClient(conn).Export(t.Context(),
		logsRequest(strings.Repeat("compressible ", 1000)), grpc.UseCompressor(gzip.Name))
	require.NoError(t, err)

	events := rec.received()
	require.Len(t, events, 1)
	assert.Equal(t, "gzip", events[0].Request.Encoding)
	assert.Less(t, events[0].Request.WireBytes, events[0].Request.DecodedBytes)
}

func TestGRPCStatusCodes(t *testing.T) {
	t.Parallel()
	duplicate := logsRequest("x")
	duplicate.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Attributes = []*commonpb.KeyValue{
		{Key: "team"}, {Key: "team"},
	}
	cases := []struct {
		name     string
		rec      *syncRecorder
		extra    map[string]any
		request  *collogspb.ExportLogsServiceRequest
		code     codes.Code
		retryIn  time.Duration
		received int
	}{
		{name: "rejected by a stage", rec: &syncRecorder{err: domain.ErrEventRejected},
			request: logsRequest("x"), code: codes.InvalidArgument, received: 1},
		{name: "failed delivery is retriable", rec: &syncRecorder{err: errors.New("sink down")},
			extra: map[string]any{"retry_after": "3s"}, request: logsRequest("x"),
			code: codes.Unavailable, retryIn: 3 * time.Second, received: 1},
		{name: "delivery timeout is retriable", rec: &syncRecorder{delay: time.Second},
			extra: map[string]any{"delivery_timeout": "50ms"}, request: logsRequest("x"),
			code: codes.Unavailable, retryIn: 5 * time.Second},
		{name: "duplicate attribute key", rec: &syncRecorder{},
			request: duplicate, code: codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := grpcSource(t, tc.rec, tc.extra)

			_, err := collogspb.NewLogsServiceClient(conn).Export(t.Context(), tc.request)

			st := status.Convert(err)
			assert.Equal(t, tc.code, st.Code(), st.Message())
			if tc.retryIn > 0 {
				require.Len(t, st.Details(), 1, "OTLP clients wait for the RetryInfo delay")
				info, ok := st.Details()[0].(*errdetails.RetryInfo)
				require.True(t, ok)
				assert.Equal(t, tc.retryIn, info.GetRetryDelay().AsDuration())
			}
			assert.Len(t, tc.rec.received(), tc.received)
		})
	}
}

func TestGRPCRefusesMessagesAboveTheBodyLimit(t *testing.T) {
	t.Parallel()
	rec := &syncRecorder{}
	conn := grpcSource(t, rec, nil)

	_, err := collogspb.NewLogsServiceClient(conn).Export(t.Context(),
		logsRequest(strings.Repeat("x", otlp.MaxBodyBytes+1)), grpc.MaxCallSendMsgSize(otlp.MaxBodyBytes*2))

	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Empty(t, rec.received())
}

// One source can listen for both protocols; both deliver to its stream.
func TestHTTPAndGRPCOnOneSource(t *testing.T) {
	t.Parallel()
	rec := &syncRecorder{}
	httpPort, grpcPort := freePort(t), freePort(t)
	startSource(t, map[string]any{"http_port": float64(httpPort), "grpc_port": float64(grpcPort)}, rec)

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = collogspb.NewLogsServiceClient(conn).Export(t.Context(), logsRequest("over gRPC"))
	require.NoError(t, err)

	body, err := proto.Marshal(&logspb.LogsData{ResourceLogs: logsRequest("over HTTP").GetResourceLogs()})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/v1/logs", httpPort), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set(contentTypeHeader, protobufContentType)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Len(t, rec.received(), 2)
}

func TestSpecNeedsADistinctPort(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		spec map[string]any
		ok   bool
	}{
		{name: "no port", spec: map[string]any{}},
		{name: "the same port twice", spec: map[string]any{"http_port": float64(4318), "grpc_port": float64(4318)}},
		{name: "port out of range", spec: map[string]any{"grpc_port": float64(70000)}},
		{name: "gRPC only", spec: map[string]any{"grpc_port": float64(4317)}, ok: true},
		{name: "both", spec: map[string]any{"http_port": float64(4318), "grpc_port": float64(4317)}, ok: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validator.New().Struct(specOf(t, tc.spec))
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func specOf(t *testing.T, raw map[string]any) otlp.Spec {
	t.Helper()
	var spec otlp.Spec
	if v, ok := raw["http_port"].(float64); ok {
		spec.HTTPPort = int(v)
	}
	if v, ok := raw["grpc_port"].(float64); ok {
		spec.GRPCPort = int(v)
	}
	return spec
}
