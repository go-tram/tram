// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api_test

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/go-tram/tram/internal/adapters/api"
	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

// generatingCore runs stream "s": a generator emitting every 10ms into a
// debug sink.
func generatingCore(t *testing.T) *core.Core {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	c := core.New(t.Context(), logger, source.NewFactory(logger), stage.NewFactory(logger), sink.NewFactory(logger))
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	_, err := c.UpsertStream(t.Context(), domain.StreamSpec{
		Name:   "s",
		Source: domain.SourceSpec{Name: "in", Type: "generator", Spec: map[string]any{"interval": "10ms"}},
		Sinks:  []domain.SinkSpec{{Name: "out", Type: "debug", From: "in"}},
	})
	require.NoError(t, err)
	return c
}

// serveAdmin serves the admin API for c on a free port until the test ends
// and returns its address.
func serveAdmin(t *testing.T, c *core.Core) string {
	t.Helper()
	var lc net.ListenConfig
	listener, err := lc.Listen(t.Context(), "tcp", anyLocalAddr)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- api.NewAdmin(anyLocalAddr, c, c, slog.New(slog.DiscardHandler)).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	return listener.Addr().String()
}

func tapClient(t *testing.T, grpcAddr string) v1.TapServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return v1.NewTapServiceClient(conn)
}

func TestTapStream(t *testing.T) {
	t.Parallel()

	c := generatingCore(t)
	client := tapClient(t, serveAdmin(t, c))

	stream, err := client.TapStream(t.Context(), &v1.TapStreamRequest{Name: "s", Component: "out"})
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "out", resp.GetComponent())
	assert.Equal(t, v1.ComponentKind_COMPONENT_KIND_SINK, resp.GetKind())
	require.NotNil(t, resp.GetSink())
	assert.Empty(t, resp.GetSink().GetError())
	assert.NotNil(t, resp.GetTime())
}

func TestTapStreamErrors(t *testing.T) {
	t.Parallel()

	c := generatingCore(t)
	client := tapClient(t, serveAdmin(t, c))

	cases := []struct {
		name string
		req  *v1.TapStreamRequest
		want codes.Code
	}{
		{name: "unknown stream", req: &v1.TapStreamRequest{Name: "unknown"}, want: codes.NotFound},
		{name: "unknown component", req: &v1.TapStreamRequest{Name: "s", Component: "x"}, want: codes.NotFound},
		{name: "negative rate", req: &v1.TapStreamRequest{Name: "s", Rate: -1}, want: codes.InvalidArgument},
		{name: "no name", req: &v1.TapStreamRequest{}, want: codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stream, err := client.TapStream(t.Context(), tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			assert.Equal(t, tc.want, status.Code(err), "error: %v", err)
		})
	}
}

func TestTapStreamEndsWhenTheStreamIsDeleted(t *testing.T) {
	t.Parallel()

	c := generatingCore(t)
	client := tapClient(t, serveAdmin(t, c))

	stream, err := client.TapStream(t.Context(), &v1.TapStreamRequest{Name: "s"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)

	require.NoError(t, c.DeleteStream(t.Context(), "s"))
	for {
		if _, err = stream.Recv(); err != nil {
			break
		}
	}
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTapStreamIsNotServedByTheStreamAPI(t *testing.T) {
	t.Parallel()

	c := generatingCore(t)
	_, grpcAddr := serveAPIWith(t, c, nil, slog.New(slog.DiscardHandler))
	client := tapClient(t, grpcAddr)

	stream, err := client.TapStream(t.Context(), &v1.TapStreamRequest{Name: "s"})
	require.NoError(t, err)
	_, err = stream.Recv()
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestTapStreamOverHTTP(t *testing.T) {
	t.Parallel()

	c := generatingCore(t)
	httpAddr := serveAdmin(t, c)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://"+httpAddr+"/v1/streams/s/tap?component=in&rate=50", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The gateway streams one JSON object per line.
	lines := bufio.NewScanner(resp.Body)
	require.True(t, lines.Scan(), "a first line: %v", lines.Err())
	var line struct {
		Result struct {
			Component string `json:"component"`
			Kind      string `json:"kind"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(lines.Bytes(), &line))
	assert.Equal(t, "in", line.Result.Component)
	assert.Equal(t, "COMPONENT_KIND_SOURCE", line.Result.Kind)
}

func TestAdminEndsOpenTapsWhenItStops(t *testing.T) {
	t.Parallel()

	c := generatingCore(t)
	var lc net.ListenConfig
	listener, err := lc.Listen(t.Context(), "tcp", anyLocalAddr)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- api.NewAdmin(anyLocalAddr, c, c, slog.New(slog.DiscardHandler)).Serve(ctx, listener) }()

	stream, err := tapClient(t, listener.Addr().String()).TapStream(t.Context(), &v1.TapStreamRequest{Name: "s"})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)

	cancel()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("the admin API waited for the open tap")
	}
	for {
		if _, err = stream.Recv(); err != nil {
			break
		}
	}
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestAdminServesTheStreamService(t *testing.T) {
	t.Parallel()

	c := generatingCore(t)
	addr := serveAdmin(t, c)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	resp, err := v1.NewStreamServiceClient(conn).GetStream(t.Context(), &v1.GetStreamRequest{Name: "s"})
	require.NoError(t, err)
	assert.Equal(t, "s", resp.GetStream().GetName())

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/v1/streams", nil)
	require.NoError(t, err)
	httpResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer httpResp.Body.Close()
	assert.Equal(t, http.StatusOK, httpResp.StatusCode)
}
