// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/api"
	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

const anyLocalAddr = "127.0.0.1:0"

func run(ctx context.Context, a *api.API) <-chan error {
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	return done
}

func TestRunFailsWhenPortIsInUse(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", anyLocalAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = taken.Close() })

	a, err := api.New(nil, anyLocalAddr, taken.Addr().String(), nil, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := run(ctx, a)

	select {
	case err = <-done:
		require.ErrorContains(t, err, "HTTP gateway failed to listen")
		// serveAPI retries on exactly this error.
		require.ErrorIs(t, err, syscall.EADDRINUSE)
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("Run kept running although the HTTP port is in use")
	}
}

func TestRunStopsWithContext(t *testing.T) {
	t.Parallel()

	a, err := api.New(nil, anyLocalAddr, anyLocalAddr, nil, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := run(ctx, a)
	cancel()

	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after its context was cancelled")
	}
}

func TestListStreams(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	c := core.New(t.Context(), logger, source.NewFactory(logger), stage.NewFactory(logger), sink.NewFactory(logger))
	t.Cleanup(func() { assert.NoError(t, c.Close(context.Background())) })
	for _, name := range []string{"b", "a"} {
		_, err := c.UpsertStream(t.Context(), domain.StreamSpec{
			Name:   name,
			Source: domain.SourceSpec{Name: "in", Type: "generator", Spec: map[string]any{"interval": "1h"}},
			Sinks:  []domain.SinkSpec{{Name: "out", Type: "debug", From: "in"}},
		})
		require.NoError(t, err)
	}

	httpAddr := serveAPI(t, c, nil, logger)

	var body struct {
		Streams []struct {
			Name string `json:"name"`
		} `json:"streams"`
	}
	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+httpAddr+"/v1/streams", nil)
		require.NoError(t, reqErr)
		resp, getErr := http.DefaultClient.Do(req)
		if getErr != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&body) == nil
	}, 5*time.Second, 20*time.Millisecond)

	require.Len(t, body.Streams, 2)
	assert.Equal(t, "a", body.Streams[0].Name, "ordered by name")
	assert.Equal(t, "b", body.Streams[1].Name)
}

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()

	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("tram_metric 1\n"))
	})
	httpAddr := serveAPI(t, nil, metrics, slog.New(slog.DiscardHandler))

	var body []byte
	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+httpAddr+"/metrics", nil)
		require.NoError(t, reqErr)
		resp, getErr := http.DefaultClient.Do(req)
		if getErr != nil {
			return false
		}
		defer resp.Body.Close()
		body, getErr = io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusOK && getErr == nil
	}, 5*time.Second, 20*time.Millisecond)

	assert.Equal(t, "tram_metric 1\n", string(body))
}

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", anyLocalAddr)
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func putStream(t *testing.T, httpAddr, name string, body map[string]any) (int, map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPut,
		"http://"+httpAddr+"/v1/streams/"+name,
		bytes.NewReader(data),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

func TestPutStreamDryRun(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	c := core.New(t.Context(), logger, source.NewFactory(logger), stage.NewFactory(logger), sink.NewFactory(logger))
	t.Cleanup(func() { assert.NoError(t, c.Close(context.Background())) })

	stream := map[string]any{
		"name":   "s",
		"source": map[string]any{"name": "in", "type": "generator", "spec": map[string]any{"interval": "1h"}},
		"sinks":  []any{map[string]any{"name": "out", "type": "debug", "from": "in"}},
	}
	httpAddr := serveAPI(t, c, nil, logger)
	created, _ := putStream(t, httpAddr, "s", map[string]any{"stream": stream})
	require.Equal(t, http.StatusOK, created)

	t.Run("plans a hot swap without applying it", func(t *testing.T) {
		t.Parallel()

		updated := map[string]any{
			"name":   "s",
			"source": map[string]any{"name": "in", "type": "generator", "spec": map[string]any{"interval": "1h"}},
			"sinks":  []any{map[string]any{"name": "out2", "type": "debug", "from": "in"}},
		}
		status, resp := putStream(t, httpAddr, "s", map[string]any{"stream": updated, "dry_run": true})
		require.Equal(t, http.StatusOK, status)
		plan, ok := resp["plan"].(map[string]any)
		require.True(t, ok, "dry run must return a plan: %v", resp)
		assert.Equal(t, "STREAM_PLAN_ACTION_HOT_SWAP", plan["action"])
		assert.Equal(t, []any{"out2"}, plan["createdNodes"])
		assert.Equal(t, []any{"out"}, plan["stoppedNodes"])

		got, err := c.GetStream(t.Context(), "s")
		require.NoError(t, err)
		require.Len(t, got.Sinks, 1)
		assert.Equal(t, "out", got.Sinks[0].Name, "the dry run did not change the running stream")
	})

	t.Run("plans a rebuild for a new stream", func(t *testing.T) {
		t.Parallel()

		status, resp := putStream(t, httpAddr, "new", map[string]any{"stream": stream, "dry_run": true})
		require.Equal(t, http.StatusOK, status)
		plan := resp["plan"].(map[string]any)
		assert.Equal(t, "STREAM_PLAN_ACTION_REBUILD", plan["action"])
		assert.Equal(t, false, plan["sourceReplaced"])

		_, err := c.GetStream(t.Context(), "new")
		require.ErrorIs(t, err, domain.ErrStreamNotFound)
	})

	t.Run("rejects an invalid spec", func(t *testing.T) {
		t.Parallel()

		broken := map[string]any{
			"name":   "s",
			"source": map[string]any{"name": "in", "type": "generator", "spec": map[string]any{"interval": "1h"}},
			"sinks":  []any{map[string]any{"name": "bad", "type": "does-not-exist", "from": "in"}},
		}
		status, _ := putStream(t, httpAddr, "s", map[string]any{"stream": broken, "dry_run": true})
		assert.Equal(t, http.StatusBadRequest, status)
	})
}

// serveAPI starts the API on free ports and returns its HTTP address once the
// gateway serves; the API stops when the test ends. The gateway dials the
// gRPC endpoint by its address, so the ports are chosen before the API
// listens, and a parallel test may take one meanwhile: then it tries again
// with new ports.
func serveAPI(t *testing.T, c ports.Core, metrics http.Handler, logger *slog.Logger) string {
	t.Helper()
	const attempts = 5
	for range attempts {
		httpAddr, grpcAddr := freeAddr(t), freeAddr(t)
		a, err := api.New(c, grpcAddr, httpAddr, metrics, logger)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := run(ctx, a)

		serving, runErr := waitServing(t, httpAddr, done)
		if serving {
			t.Cleanup(func() {
				cancel()
				require.NoError(t, <-done)
			})
			return httpAddr
		}
		cancel()
		if !errors.Is(runErr, syscall.EADDRINUSE) {
			require.NoError(t, runErr, "the API stopped before it served")
			t.Fatal("the API did not serve in time")
		}
	}
	t.Fatalf("the API found no free ports in %d attempts", attempts)
	return ""
}

// waitServing waits until the HTTP gateway at httpAddr answers, with any
// status, or Run returns, and then reports whether it serves, or Run's error.
func waitServing(t *testing.T, httpAddr string, done <-chan error) (bool, error) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return false, err
		case <-deadline:
			return false, nil
		case <-ticker.C:
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+httpAddr+"/", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		return true, nil
	}
}
