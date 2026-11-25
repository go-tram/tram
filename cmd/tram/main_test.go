// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogLevelFlag(t *testing.T) {
	t.Parallel()

	run := func(args ...string) (*slog.LevelVar, error) {
		level := &slog.LevelVar{}
		cmd := newRootCmd(slog.New(slog.DiscardHandler), level)
		cmd.SetArgs(args)
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		return level, cmd.Execute()
	}

	level, err := run()
	require.NoError(t, err)
	assert.Equal(t, slog.LevelInfo, level.Level(), "info by default")

	level, err = run("--log-level", "debug")
	require.NoError(t, err)
	assert.Equal(t, slog.LevelDebug, level.Level())

	_, err = run("--log-level", "loud")
	assert.ErrorContains(t, err, "invalid --log-level")
}

func TestErrorsAreReportedOnce(t *testing.T) {
	t.Parallel()

	var logged, out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	root := newRootCmd(logger, &slog.LevelVar{})

	tap, _, err := root.Find([]string{"tap"})
	require.NoError(t, err)
	reportError(&out, tap, errors.New("stream not found"), logger)
	assert.Equal(t, "Error: stream not found\n", out.String(), "a CLI command prints the message")
	assert.Empty(t, logged.String())

	out.Reset()
	server, _, err := root.Find([]string{"server"})
	require.NoError(t, err)
	reportError(&out, server, errors.New("port in use"), logger)
	assert.Empty(t, out.String())
	assert.Contains(t, logged.String(), `msg="TRAM stopped with an error" error="port in use"`, "the server logs")
}

func TestErrorsAreNotPrintedByCobra(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	cmd := newRootCmd(slog.New(slog.DiscardHandler), &slog.LevelVar{})
	cmd.SetArgs([]string{"tap", "s", "--admin-endpoint", "127.0.0.1:1"})
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	require.Error(t, cmd.ExecuteContext(t.Context()))
	assert.NotContains(t, out.String(), "Error:", "main reports the error, not cobra")
	assert.NotContains(t, out.String(), "Usage:", "an error of the server shows no usage")
}
