// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

// bufferedPipeline runs a writer stream, OTLP source to a buffer sink, and a
// reader stream, buffer source to a capture sink, on one instance with one
// data directory, so both share the queue file.
type bufferedPipeline struct {
	core    *core.Core
	sinks   *captureFactory
	baseURL string
}

func newBufferedPipeline(t *testing.T, readerSink domain.SinkSpec) *bufferedPipeline {
	t.Helper()
	dataDir := t.TempDir()
	p := &bufferedPipeline{
		sinks: &captureFactory{
			real:  sink.NewFactory(discard(), sink.WithDataDir(dataDir)),
			sinks: map[string]*captureSink{},
		},
	}
	p.core = core.New(
		context.Background(),
		discard(),
		source.NewFactory(discard(), source.WithDataDir(dataDir)),
		stage.NewFactory(discard()),
		p.sinks,
	)
	t.Cleanup(func() { _ = p.core.Close(context.Background()) })

	port := onFreePort(t, 0, func(port int) error {
		_, err := p.core.UpsertStream(context.Background(), domain.StreamSpec{
			Name: "writer",
			Source: domain.SourceSpec{
				Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(port)},
			},
			Sinks: []domain.SinkSpec{{
				Name: "outbox", Type: "buffer", From: "in",
				Spec: map[string]any{
					"driver": "sqlite", "sqlite": map[string]any{},
					"queue": "outbox", "max_age": "1h",
				},
			}},
		})
		return err
	})
	p.baseURL = fmt.Sprintf("http://127.0.0.1:%d", port)

	readerSink.From = "outbox"
	_, err := p.core.UpsertStream(context.Background(), domain.StreamSpec{
		Name: "reader",
		Source: domain.SourceSpec{
			Name: "outbox", Type: "buffer",
			Spec: map[string]any{
				"driver": "sqlite", "sqlite": map[string]any{},
				"queue": "outbox", "workers": float64(4),
				"retry_initial": "20ms", "retry_max": "100ms",
			},
		},
		Sinks: []domain.SinkSpec{readerSink},
	})
	require.NoError(t, err)
	return p
}

func (p *bufferedPipeline) postLogs(t *testing.T, logs *logspb.LogsData) int {
	t.Helper()
	body, err := proto.Marshal(logs)
	require.NoError(t, err)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(p.baseURL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// Behind a buffer, the client gets 200 once its request is stored, also
// while the reader's sink fails; the records arrive once the sink works
// again.
func TestBufferAcceptsWhileTheSinkFails(t *testing.T) {
	t.Parallel()

	p := newBufferedPipeline(t, domain.SinkSpec{
		Name: "flaky", Type: captureType,
		Spec: map[string]any{"fail_first": float64(3)},
	})

	require.Equal(t, http.StatusOK, p.postLogs(t, request("shop", rec{"r1", "A"})))
	require.Equal(t, http.StatusOK, p.postLogs(t, request("shop", rec{"r2", "B"})))

	flaky := p.sinks.sink(t, "flaky")
	require.Eventually(
		t,
		func() bool { return len(arrived(flaky.received())) == 2 },
		10*time.Second,
		10*time.Millisecond,
	)
	assert.ElementsMatch(t, []string{"shop/e2e/r1", "shop/e2e/r2"}, arrived(flaky.received()))
}

// What the reader's sink rejects for good is dropped; the client, which was
// answered when the event was stored, is not told.
func TestBufferDropsWhatTheSinkRejects(t *testing.T) {
	t.Parallel()

	p := newBufferedPipeline(t, domain.SinkSpec{
		Name: "refusing", Type: captureType, Spec: map[string]any{"fail": "permanent"},
	})

	require.Equal(t, http.StatusOK, p.postLogs(t, request("shop", rec{"r1", "A"})))
	time.Sleep(500 * time.Millisecond)
	assert.Empty(t, p.sinks.sink(t, "refusing").received())
}

// A suspended writer accepts nothing; the reader delivers what the queue
// still holds.
func TestSuspendedWriterStopsTheFlow(t *testing.T) {
	t.Parallel()

	p := newBufferedPipeline(t, captureSinkSpec("out", "outbox"))
	require.Equal(t, http.StatusOK, p.postLogs(t, request("shop", rec{"r1", "A"})))
	out := p.sinks.sink(t, "out")
	require.Eventually(t, func() bool { return len(arrived(out.received())) == 1 }, 10*time.Second, 10*time.Millisecond)

	writer, err := p.core.GetStream(t.Context(), "writer")
	require.NoError(t, err)
	writer.Suspended = true
	_, err = p.core.UpsertStream(t.Context(), *writer)
	require.NoError(t, err)

	code := p.postLogs(t, request("shop", rec{"r2", "A"}))
	assert.NotEqual(t, http.StatusOK, code,
		"a suspended stream opens no port; the client cannot connect and gets no 200")
	time.Sleep(300 * time.Millisecond)
	assert.Len(t, arrived(out.received()), 1, "nothing new arrives while the writer is suspended")
}
