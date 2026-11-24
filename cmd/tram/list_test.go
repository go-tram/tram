// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core/domain"
)

// TestListMatchesTheFactories checks every listed type against its factory,
// and that the drivers listed are the ones the component knows: a component
// with drivers names them in the error about an unknown one.
func TestListMatchesTheFactories(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	dataDir := t.TempDir()
	sources := source.NewFactory(logger, source.WithDataDir(dataDir))
	stages := stage.NewFactory(logger)
	sinks := sink.NewFactory(logger, sink.WithDataDir(dataDir))

	check := func(c component, spec map[string]any) error {
		switch c.Kind {
		case domain.KindSource:
			return sources.Check(domain.SourceSpec{Name: "c", Type: c.Type, Spec: spec})
		case domain.KindStage:
			s, err := stages.New(domain.StageSpec{Name: "c", Type: c.Type, From: "in", Spec: spec}, "stream")
			if err == nil {
				err = s.Stop()
			}
			return err
		case domain.KindSink:
			return sinks.Check(domain.SinkSpec{Name: "c", Type: c.Type, From: "in", Spec: spec})
		default:
			return fmt.Errorf("unknown kind %s", c.Kind)
		}
	}

	for _, c := range components() {
		t.Run(string(c.Kind)+"/"+c.Type, func(t *testing.T) {
			t.Parallel()

			err := check(c, map[string]any{"driver": "no-such-driver"})
			require.Error(t, err)
			for _, unknownType := range []error{
				domain.ErrInvalidSourceType, domain.ErrInvalidStageType, domain.ErrInvalidSinkType,
			} {
				require.NotErrorIs(t, err, unknownType, "the factory knows the type")
			}
			if len(c.Drivers) == 0 {
				require.NotErrorIs(t, err, driver.ErrUnknown, "the component has drivers: %v", err)
				return
			}
			require.ErrorIs(t, err, driver.ErrUnknown)
			_, available, found := strings.Cut(err.Error(), "available: ")
			require.True(t, found, err.Error())
			assert.ElementsMatch(t, c.Drivers, strings.Split(available, ", "))
		})
	}
}

func TestListFiltersAndPrintsJSON(t *testing.T) {
	t.Parallel()

	out, err := runCmd(t, "list", "sinks", "-o", "json")
	require.NoError(t, err)
	var listed []component
	require.NoError(t, json.Unmarshal([]byte(out), &listed))
	require.NotEmpty(t, listed)
	for _, c := range listed {
		assert.Equal(t, domain.KindSink, c.Kind)
	}

	_, err = runCmd(t, "list", "drivers")
	require.Error(t, err)
}
