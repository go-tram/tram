// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

import (
	"context"
	"errors"
	"fmt"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	// Registers the gzip compressor, so clients may compress requests.
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/go-tram/tram/internal/core/domain"
)

// newGRPCServer serves the OTLP/gRPC services of o. It answers like the HTTP
// side, in gRPC's terms: Unavailable with RetryInfo where HTTP answers 503,
// the only code OTLP clients retry; InvalidArgument where it answers 422; and
// ResourceExhausted for messages larger than the HTTP body limit.
func newGRPCServer(o *OTLP) *grpc.Server {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxBodyBytes),
		grpc.StatsHandler(sizeHandler{}),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: idleTimeout}),
	)
	collogspb.RegisterLogsServiceServer(server, logsService{o: o})
	colmetricspb.RegisterMetricsServiceServer(server, metricsService{o: o})
	coltracepb.RegisterTraceServiceServer(server, traceService{o: o})
	return server
}

type logsService struct {
	collogspb.UnimplementedLogsServiceServer

	o *OTLP
}

func (s logsService) Export(
	ctx context.Context,
	req *collogspb.ExportLogsServiceRequest,
) (*collogspb.ExportLogsServiceResponse, error) {
	logs := &logspb.LogsData{ResourceLogs: req.GetResourceLogs()}
	if err := s.o.export(ctx, &domain.Event{Logs: logs}); err != nil {
		return nil, err
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

type metricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer

	o *OTLP
}

func (s metricsService) Export(
	ctx context.Context,
	req *colmetricspb.ExportMetricsServiceRequest,
) (*colmetricspb.ExportMetricsServiceResponse, error) {
	metrics := &metricspb.MetricsData{ResourceMetrics: req.GetResourceMetrics()}
	if err := s.o.export(ctx, &domain.Event{Metrics: metrics}); err != nil {
		return nil, err
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type traceService struct {
	coltracepb.UnimplementedTraceServiceServer

	o *OTLP
}

func (s traceService) Export(
	ctx context.Context,
	req *coltracepb.ExportTraceServiceRequest,
) (*coltracepb.ExportTraceServiceResponse, error) {
	traces := &tracepb.TracesData{ResourceSpans: req.GetResourceSpans()}
	if err := s.o.export(ctx, &domain.Event{Traces: traces}); err != nil {
		return nil, err
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// export passes a received gRPC request on, like receive does an HTTP one.
func (o *OTLP) export(ctx context.Context, event *domain.Event) error {
	if key, duplicate := duplicateKey(event); duplicate {
		return status.Errorf(codes.InvalidArgument, "attribute key %q appears more than once", key)
	}
	md, _ := metadata.FromIncomingContext(ctx)
	if err := applyHeaders(event, o.headers, func(m headerMapping) []string { return md.Get(m.metadata) }); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if size, ok := ctx.Value(sizeKey{}).(*requestSize); ok {
		event.Request = &domain.Request{
			Encoding:     size.encoding,
			WireBytes:    size.wire,
			DecodedBytes: size.decoded,
		}
	}

	deliveryCtx, cancel := context.WithTimeout(ctx, o.deliveryTimeout)
	defer cancel()
	if err := o.NotifySubscribers(deliveryCtx, event); err != nil {
		if errors.Is(err, domain.ErrEventRejected) {
			return status.Error(codes.InvalidArgument, "event rejected")
		}
		return o.unavailable()
	}
	return nil
}

// unavailable is the retriable answer, with the delay clients should wait,
// as the Retry-After header is on the HTTP side.
func (o *OTLP) unavailable() error {
	st := status.New(codes.Unavailable, "error notifying subscribers")
	detailed, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(o.retryDelay)})
	if err != nil {
		return st.Err()
	}
	return detailed.Err()
}

// sizeHandler records the size of each request as received and decoded, for
// the request size metric. gRPC reports it before it calls the service.
type sizeHandler struct{}

type sizeKey struct{}

type requestSize struct {
	encoding      string
	wire, decoded int
}

func (sizeHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, sizeKey{}, &requestSize{encoding: encodingIdentity})
}

func (sizeHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	size, ok := ctx.Value(sizeKey{}).(*requestSize)
	if !ok {
		return
	}
	switch s := s.(type) {
	case *stats.InHeader:
		if s.Compression != "" {
			size.encoding = s.Compression
		}
	case *stats.InPayload:
		size.wire, size.decoded = s.CompressedLength, s.Length
		if size.wire == 0 {
			size.wire = s.Length
		}
	}
}

func (sizeHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (sizeHandler) HandleConn(context.Context, stats.ConnStats) {}

// stopGRPC stops the server gracefully, and at once after the shutdown timeout.
func stopGRPC(server *grpc.Server, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		server.Stop()
		return fmt.Errorf("gRPC server did not stop within %s", timeout)
	}
}
