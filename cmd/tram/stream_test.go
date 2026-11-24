// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/api"
	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
)

// serveAdmin runs a core with its admin API until the test ends and returns
// the API's address.
func serveAdmin(t *testing.T) string {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	c := core.New(t.Context(), logger, source.NewFactory(logger), stage.NewFactory(logger), sink.NewFactory(logger))
	var lc net.ListenConfig
	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- api.NewAdmin("", c, c, logger).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
		require.NoError(t, c.Close())
	})
	return listener.Addr().String()
}

const demoStream = `{"stream": {"name": "demo",
  "source": {"name": "gen", "type": "generator", "spec": {"interval": "1h"}},
  "stages": [{"name": "pass", "type": "noop", "from": "gen"}],
  "sinks": [{"name": "out", "type": "debug", "from": "pass"}]}}`

func TestStreamCommands(t *testing.T) {
	t.Parallel()

	addr := serveAdmin(t)
	dir := t.TempDir()
	file := writeFile(t, dir, "demo.json", demoStream)
	stream := func(args ...string) string {
		t.Helper()
		out, err := runCmd(t, append(append([]string{"stream"}, args...), "--admin-endpoint", addr)...)
		require.NoError(t, err, out)
		return out
	}

	assert.Equal(t, "demo: would be created\n", stream("apply", "-f", dir, "--dry-run"))
	assert.Contains(t, stream("list"), "NAME  SOURCE  STAGES  SINKS  SUSPENDED\n", "nothing applied by the dry run")
	assert.Equal(t, "demo: created\n", stream("apply", "-f", dir))
	assert.Equal(t, "demo: unchanged\n", stream("apply", "-f", file))
	assert.Contains(t, stream("list"), "demo  generator  1       1      false")
	assert.Equal(t, "demo: unchanged\n", stream("diff", "-f", file))

	got := stream("get", "demo")
	assert.True(t, strings.HasPrefix(got, "{\n  \"stream\": {\n    \"name\": \"demo\""), got)
	// What get writes applies unchanged.
	writeFile(t, dir, "demo.json", got)
	assert.Equal(t, "demo: unchanged\n", stream("apply", "-f", file))

	writeFile(
		t,
		dir,
		"demo.json",
		strings.Replace(demoStream, `"type": "noop"`, `"type": "delay", "spec": {"delay": "1ms"}`, 1),
	)
	diff := stream("diff", "-f", file)
	assert.Contains(t, diff, "demo: would be updated, stages and sinks replaced")
	assert.Contains(t, diff, `-        "type": "noop",`)
	assert.Contains(t, diff, `+        "type": "delay",`)
	assert.Equal(t, "demo: updated, stages and sinks replaced\n", stream("apply", "-f", file))

	assert.Equal(t, "deleted demo\n", stream("delete", "demo"))
	out, err := runCmd(t, "stream", "delete", "demo", "--admin-endpoint", addr)
	require.ErrorContains(t, err, "demo: stream not found", out)
}

func TestStreamWritesAreRefusedInKubernetes(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"stream", "delete", "demo", "--engine", "telemetry"},
		{"stream", "apply", "-f", ".", "-l", "app=tram"},
	} {
		_, err := runCmd(t, args...)
		require.ErrorIs(t, err, errWriteInKubernetes, args)
	}
}

func TestStreamDiffShowsWhyApplyingWouldFail(t *testing.T) {
	t.Parallel()

	addr := serveAdmin(t)
	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = taken.Close() })
	port := strconv.Itoa(taken.Addr().(*net.TCPAddr).Port)

	file := writeFile(t, t.TempDir(), "otlp.json", `{"name": "otlp",
  "source": {"name": "in", "type": "otlp", "spec": {"http_port": `+port+`}},
  "sinks": [{"name": "out", "type": "debug", "from": "in"}]}`)

	out, err := runCmd(t, "stream", "diff", "-f", file, "--admin-endpoint", addr)
	require.ErrorIs(t, err, errApplyWouldFail)
	assert.Contains(t, out, "otlp: applying would fail:\n  ")
	assert.Contains(t, out, "port unavailable")
	assert.Contains(t, out, `+    "name": "otlp",`, "the diff is shown anyway")
}
