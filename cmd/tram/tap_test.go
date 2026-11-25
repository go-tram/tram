// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

// fakeTapServer sends responses, then ends with err, or waits for the client
// to cancel if err is nil.
type fakeTapServer struct {
	v1.UnimplementedTapServiceServer

	responses []*v1.TapStreamResponse
	err       error
	requests  chan *v1.TapStreamRequest
}

func (s *fakeTapServer) TapStream(
	req *v1.TapStreamRequest,
	stream grpc.ServerStreamingServer[v1.TapStreamResponse],
) error {
	s.requests <- req
	for _, resp := range s.responses {
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	if s.err != nil {
		return s.err
	}
	<-stream.Context().Done()
	return nil
}

func serveTaps(t *testing.T, srv *fakeTapServer) string {
	t.Helper()
	srv.requests = make(chan *v1.TapStreamRequest, 1)
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	v1.RegisterTapServiceServer(s, srv)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	return l.Addr().String()
}

func runTapCmd(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := newRootCmd(slog.New(slog.DiscardHandler), &slog.LevelVar{})
	cmd.SetArgs(append([]string{"tap"}, args...))
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SilenceUsage = true
	err := cmd.ExecuteContext(t.Context())
	return out.String(), errOut.String(), err
}

func logsResponse(component string, kind v1.ComponentKind, body string) *v1.TapStreamResponse {
	return &v1.TapStreamResponse{
		Component: component,
		Kind:      kind,
		Time:      timestamppb.New(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)),
		Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				SeverityText: "ERROR",
				Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
				Attributes: []*commonpb.KeyValue{{
					Key: "code", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}},
				}},
			}},
		}}}}},
	}
}

func TestTapText(t *testing.T) {
	t.Parallel()

	failed := logsResponse("loki", v1.ComponentKind_COMPONENT_KIND_SINK, "disk full")
	failed.Sink = &v1.SinkResult{Error: "connection refused", Duration: durationpb.New(1500 * time.Microsecond)}
	failed.Dropped = 3
	srv := &fakeTapServer{responses: []*v1.TapStreamResponse{
		logsResponse("otlp", v1.ComponentKind_COMPONENT_KIND_SOURCE, "disk full"),
		failed,
	}}
	addr := serveTaps(t, srv)

	out, errOut, err := runTapCmd(t, "my-stream",
		"--admin-endpoint", addr, "--component", "loki", "--rate", "5", "--limit", "2")
	require.NoError(t, err)

	req := <-srv.requests
	assert.Equal(t, "my-stream", req.GetName())
	assert.Equal(t, "loki", req.GetComponent())
	assert.InDelta(t, 5, req.GetRate(), 0)

	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 4, out)
	assert.Contains(t, lines[0], "source otlp  logs 1")
	assert.Equal(t, "    ERROR disk full code=42", lines[1])
	assert.Contains(t, lines[2], "sink   loki  logs 1  error 1.5ms: connection refused")
	assert.Equal(t, "dropped 3 events\n", errOut)
}

func TestTapJSON(t *testing.T) {
	t.Parallel()

	addr := serveTaps(t, &fakeTapServer{responses: []*v1.TapStreamResponse{
		logsResponse("otlp", v1.ComponentKind_COMPONENT_KIND_SOURCE, "hello"),
	}})

	out, _, err := runTapCmd(t, "my-stream", "--admin-endpoint", addr, "-o", "json", "--limit", "1")
	require.NoError(t, err)

	var resp v1.TapStreamResponse
	require.NoError(t, protojson.Unmarshal([]byte(strings.TrimSpace(out)), &resp))
	assert.Equal(t, "otlp", resp.GetComponent())
	record := resp.GetLogs().GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0]
	assert.Equal(t, "hello", record.GetBody().GetStringValue())
}

func TestTapEnds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "when the stream is deleted",
			err:  status.Error(codes.NotFound, "stream not found"),
			want: "stream not found",
		},
		{
			name: "when the endpoint is not the admin API",
			err:  status.Error(codes.Unimplemented, "unknown service"),
			want: "serves no taps",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			addr := serveTaps(t, &fakeTapServer{err: tc.err})
			_, _, err := runTapCmd(t, "my-stream", "--admin-endpoint", addr)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestTapRejectsUnknownOutput(t *testing.T) {
	t.Parallel()

	_, _, err := runTapCmd(t, "my-stream", "-o", "yaml")
	require.ErrorContains(t, err, "invalid --output")
}
