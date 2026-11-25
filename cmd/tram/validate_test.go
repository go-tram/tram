// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func runCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := newRootCmd(slog.New(slog.DiscardHandler), &slog.LevelVar{})
	cmd.SetArgs(args)
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	err := cmd.ExecuteContext(t.Context())
	return out.String(), err
}

const validStream = `{"stream": {
  "name": "valid",
  "source": {"name": "gen", "type": "generator", "spec": {"interval": "1s"}},
  "stages": [{"name": "errors", "type": "filter", "from": "gen",
    "spec": {"driver": "cel", "cel": {"logs": "record.severity_number >= 17"}}}],
  "sinks": [{"name": "out", "type": "debug", "from": "errors"}]
}}`

func TestValidateAcceptsValidStreams(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "valid.json", validStream)

	out, err := runCmd(t, "validate", dir)
	require.NoError(t, err, out)
	assert.Contains(t, out, "ok   "+filepath.Join(dir, "valid.json")+" (valid)")
}

func TestValidateReportsEveryInvalidComponent(t *testing.T) {
	t.Parallel()

	file := writeFile(t, t.TempDir(), "broken.json", `{
  "name": "broken",
  "source": {"name": "in", "type": "broker", "spec": {"driver": "nats", "signal": "logs", "nats": {}}},
  "stages": [{"name": "bad-cel", "type": "filter", "from": "in",
    "spec": {"driver": "cel", "cel": {"logs": "record.("}}}],
  "sinks": [{"name": "unknown", "type": "nowhere", "from": "bad-cel"}]
}`)

	out, err := runCmd(t, "validate", file)
	require.ErrorContains(t, err, "1 of 1 stream files are invalid")
	assert.Contains(t, out, "FAIL "+file)
	for _, component := range []string{"in", "bad-cel", "unknown"} {
		assert.Contains(t, out, "stream spec is invalid: "+component, out)
	}
}

func TestValidateRejectsAStreamThatIsNoTree(t *testing.T) {
	t.Parallel()

	file := writeFile(t, t.TempDir(), "s.json", `{
  "name": "s",
  "source": {"name": "gen", "type": "generator", "spec": {"interval": "1s"}},
  "sinks": [{"name": "out", "type": "debug", "from": "missing"}]
}`)

	out, err := runCmd(t, "validate", file)
	require.Error(t, err)
	assert.Contains(t, out, "sink from is unknown to stream")
}

func TestValidateRejectsTheSameStreamTwice(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "a.json", validStream)
	writeFile(t, dir, "b.json", validStream)

	out, err := runCmd(t, "validate", dir)
	require.Error(t, err)
	assert.Contains(t, out, "stream valid is also defined in "+filepath.Join(dir, "a.json"))
}

func TestValidateOpensNoPort(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = taken.Close() })
	port := strconv.Itoa(taken.Addr().(*net.TCPAddr).Port)

	file := writeFile(t, t.TempDir(), "otlp.json", `{
  "name": "otlp",
  "source": {"name": "in", "type": "otlp", "spec": {"http_port": `+port+`}},
  "sinks": [{"name": "out", "type": "debug", "from": "in"}]
}`)

	out, err := runCmd(t, "validate", file)
	require.NoError(t, err, "the port in use does not matter: %s", out)
}

func TestValidateResolvesSecrets(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := writeFile(t, dir, "s.json", `{
  "name": "s",
  "source": {"name": "gen", "type": "generator", "spec": {"interval": "1s"}},
  "sinks": [{"name": "out", "type": "otlp", "from": "gen", "spec": {
    "url": "http://localhost:4318/v1/logs",
    "headers": {"Authorization": {"valueFrom": {"secretKeyRef": {"name": "otlp", "key": "token"}}}}
  }}]
}`)

	out, err := runCmd(t, "validate", file)
	require.Error(t, err)
	assert.Contains(t, out, `secret "otlp" key "token" is not in the secrets file`)

	secrets := writeFile(t, dir, "secrets.json", `{"otlp/token": "Bearer x"}`)
	out, err = runCmd(t, "validate", file, "--secrets", secrets)
	require.NoError(t, err, out)
}

func TestValidateConnectsToNoBroker(t *testing.T) {
	t.Parallel()

	file := writeFile(t, t.TempDir(), "s.json", `{
  "name": "s",
  "source": {"name": "in", "type": "broker", "spec": {"driver": "nats", "signal": "logs",
    "nats": {"url": "nats://127.0.0.1:1", "subject": "logs.>", "stream": "logs", "durable": "tram"}}},
  "sinks": [{"name": "out", "type": "broker", "from": "in", "spec": {"driver": "nats",
    "destination": {"logs": "'copy.logs'"}, "nats": {"url": "nats://127.0.0.1:1"}}}]
}`)

	out, err := runCmd(t, "validate", file)
	require.NoError(t, err, "no broker listens there: %s", out)
}
