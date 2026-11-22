// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/core/domain"
)

const routerAttr = "tram.router.id"

// headerSpec maps X-Tram-Router, written in lower case to show that header
// names are matched regardless of case, to routerAttr.
func headerSpec(port string, n int) map[string]any {
	return map[string]any{
		port:                float64(n),
		"header_attributes": map[string]any{"x-tram-router": routerAttr},
	}
}

// twoResources is a request with one resource that claims a router itself
// and one without a resource.
func twoResources() *logspb.LogsData {
	record := []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
		Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}},
	}}}}
	return &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{
		{Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			{
				Key:   "service.name",
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "shop"}},
			},
			{
				Key:   routerAttr,
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "someone-else"}},
			},
		}}, ScopeLogs: record},
		{ScopeLogs: record},
	}}
}

// routers returns the router attribute of every resource of the event, ""
// for a resource without one.
func routers(e *domain.Event) []string {
	var out []string
	for _, rl := range e.Logs.GetResourceLogs() {
		v := ""
		for _, a := range rl.GetResource().GetAttributes() {
			if a.GetKey() == routerAttr {
				v = a.GetValue().GetStringValue()
			}
		}
		out = append(out, v)
	}
	return out
}

func postLogs(t *testing.T, port int, header http.Header) int {
	t.Helper()
	body, err := proto.Marshal(twoResources())
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/v1/logs", port), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header = header
	req.Header.Set(contentTypeHeader, protobufContentType)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestHeaderAttributesOverHTTP(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		header  http.Header
		status  int
		routers []string
	}{
		{
			name:    "the header sets the attribute on every resource, replacing what the client sent",
			header:  http.Header{"X-Tram-Router": {"router-a"}},
			status:  http.StatusOK,
			routers: []string{"router-a", "router-a"},
		},
		{
			name:    "without the header, a value the client sent is removed",
			header:  http.Header{},
			status:  http.StatusOK,
			routers: []string{"", ""},
		},
		{
			name:    "an empty header counts as missing",
			header:  http.Header{"X-Tram-Router": {""}},
			status:  http.StatusOK,
			routers: []string{"", ""},
		},
		{
			name:   "a header with several values is refused",
			header: http.Header{"X-Tram-Router": {"spoofed", "router-a"}},
			status: http.StatusUnprocessableEntity,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &syncRecorder{}
			port := freePort(t)
			startSource(t, headerSpec("http_port", port), rec)

			assert.Equal(t, tc.status, postLogs(t, port, tc.header))

			events := rec.received()
			if tc.status != http.StatusOK {
				assert.Empty(t, events)
				return
			}
			require.Len(t, events, 1)
			assert.Equal(t, tc.routers, routers(events[0]))
		})
	}
}

func TestHeaderAttributesOverGRPC(t *testing.T) {
	t.Parallel()
	rec := &syncRecorder{}
	port := freePort(t)
	startSource(t, headerSpec("grpc_port", port), rec)
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := collogspb.NewLogsServiceClient(conn)
	request := &collogspb.ExportLogsServiceRequest{ResourceLogs: twoResources().GetResourceLogs()}

	ctx := metadata.AppendToOutgoingContext(t.Context(), "X-Tram-Router", "router-b")
	_, err = client.Export(ctx, request)
	require.NoError(t, err)

	repeated := metadata.AppendToOutgoingContext(t.Context(), "x-tram-router", "spoofed", "x-tram-router", "router-b")
	_, err = client.Export(repeated, request)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	events := rec.received()
	require.Len(t, events, 1, "the request with a repeated header was refused")
	assert.Equal(t, []string{"router-b", "router-b"}, routers(events[0]))
}

func TestHeaderAttributesAreValidated(t *testing.T) {
	t.Parallel()
	for name, mapping := range map[string]map[string]any{
		"invalid header name":           {"X Tram": "a"},
		"empty attribute":               {"X-Tram": ""},
		"the same header in two cases":  {"X-Tram": "a", "x-tram": "b"},
		"two headers for one attribute": {"X-One": "a", "X-Two": "a"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := otlp.New(domain.SourceSpec{Name: "otlp", Type: otlp.Type, Spec: map[string]any{
				"http_port": float64(freePort(t)), "header_attributes": mapping,
			}}, validator.New(), slog.New(slog.DiscardHandler))
			assert.ErrorContains(t, err, "header_attributes")
		})
	}
}
