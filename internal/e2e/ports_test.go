// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
)

// A port another test took between choosing and listening makes creating the
// stream fail with ErrPortUnavailable; onFreePort then creates it on another
// port.
func TestOnFreePortMovesToAnotherPort(t *testing.T) {
	t.Parallel()

	// Take the port on all interfaces, as the OTLP source listens: on macOS,
	// a port taken only on 127.0.0.1 is still free on the others.
	var lc net.ListenConfig
	taken, err := lc.Listen(t.Context(), "tcp", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = taken.Close() })
	takenPort := taken.Addr().(*net.TCPAddr).Port

	c := core.New(context.Background(), discard(),
		source.NewFactory(discard()), stage.NewFactory(discard()), sink.NewFactory(discard()))
	t.Cleanup(func() { _ = c.Close() })

	var attempts []int
	port := onFreePort(t, takenPort, func(port int) error {
		attempts = append(attempts, port)
		_, upsertErr := c.UpsertStream(context.Background(), domain.StreamSpec{
			Name:   "ports",
			Source: domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{"http_port": float64(port)}},
			Sinks:  []domain.SinkSpec{{Name: "out", Type: "noop", From: "in"}},
		})
		return upsertErr
	})

	assert.NotEqual(t, takenPort, port, "the stream moved to another port")
	assert.Equal(t, []int{takenPort, port}, attempts, "the taken port first, then a new one")
}
