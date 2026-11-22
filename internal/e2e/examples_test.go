// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"

	natsdriver "github.com/go-tram/tram/internal/adapters/broker/nats"
	sqlitebuffer "github.com/go-tram/tram/internal/adapters/buffer/sqlite"
	sinkbroker "github.com/go-tram/tram/internal/adapters/sink/broker"
	buffersink "github.com/go-tram/tram/internal/adapters/sink/buffer"
	sinkdebug "github.com/go-tram/tram/internal/adapters/sink/debug"
	sinkotlp "github.com/go-tram/tram/internal/adapters/sink/otlp"
	sourcebroker "github.com/go-tram/tram/internal/adapters/source/broker"
	buffersource "github.com/go-tram/tram/internal/adapters/source/buffer"
	"github.com/go-tram/tram/internal/adapters/source/generator"
	sourceotlp "github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/adapters/stage/batch"

	"github.com/go-tram/tram/internal/adapters/stage/delay"
	"github.com/go-tram/tram/internal/adapters/stage/filter"
	celfilter "github.com/go-tram/tram/internal/adapters/stage/filter/cel"
	"github.com/go-tram/tram/internal/adapters/stage/noop"
	"github.com/go-tram/tram/internal/adapters/stage/transform"
	celtransform "github.com/go-tram/tram/internal/adapters/stage/transform/cel"
	stagevalidation "github.com/go-tram/tram/internal/adapters/stage/validation"
	celvalidation "github.com/go-tram/tram/internal/adapters/stage/validation/cel"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testrun"
)

type exampleComponent struct {
	Name string         `json:"name"`
	Type string         `json:"type"`
	Spec map[string]any `json:"spec"`
}

type exampleStream struct {
	Source exampleComponent   `json:"source"`
	Stages []exampleComponent `json:"stages"`
	Sinks  []exampleComponent `json:"sinks"`
}

func TestExamplesAreValid(t *testing.T) {
	t.Parallel()

	configs, err := filepath.Glob("../../examples/config/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, configs)
	testConfigs, err := filepath.Glob("../../examples/tests/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, testConfigs)
	configs = append(configs, testConfigs...)
	for _, path := range configs {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			var file struct {
				Stream exampleStream `json:"stream"`
			}
			require.NoError(t, json.Unmarshal(readFile(t, path), &file))
			checkExample(t, file.Stream)
		})
	}

	manifests, err := filepath.Glob("../../examples/k8s/examples/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, manifests)
	for _, path := range manifests {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			var manifest struct {
				Kind     string `json:"kind"`
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Spec exampleStream `json:"spec"`
			}
			require.NoError(t, yaml.Unmarshal(readFile(t, path), &manifest))
			if manifest.Kind != "Stream" {
				t.Skipf("%s holds a %s, not a Stream", filepath.Base(path), manifest.Kind)
			}
			assert.Empty(t, validation.IsDNS1123Subdomain(manifest.Metadata.Name),
				"Kubernetes rejects the name %q", manifest.Metadata.Name)
			checkExample(t, manifest.Spec)
		})
	}
}

// TestExampleTestsPass runs the test cases of the examples in examples/tests/,
// the same way `tram test` does.
func TestExampleTestsPass(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	configs, err := filepath.Glob("../../examples/tests/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, configs)
	for _, path := range configs {
		spec, loadErr := testrun.LoadStream(path)
		require.NoError(t, loadErr)
		require.NotEmpty(t, spec.Tests, "%s carries no test cases", path)
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			results, runErr := core.RunStreamTests(t.Context(), spec, stage.NewFactory(logger), logger)
			require.NoError(t, runErr)
			for _, result := range results {
				assert.True(t, result.Passed, "%s: %v", result.Name, result.Problems)
			}
		})
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func checkExample(t *testing.T, stream exampleStream) {
	t.Helper()

	check := func(kind string, c exampleComponent, parse func(string, map[string]any) error) {
		assert.NoError(t, parse(c.Type, c.Spec), "%s %s", kind, c.Name)
	}
	check("source", stream.Source, parseSource)
	for _, stage := range stream.Stages {
		check("stage", stage, parseStage)
	}
	for _, sink := range stream.Sinks {
		check("sink", sink, parseSink)
	}
}

func parse[T any](raw map[string]any) error {
	spec, err := domain.ParseSpec(raw, new(T))
	if err != nil {
		return err
	}
	return validator.New(validator.WithRequiredStructEnabled()).Struct(spec)
}

func parseSource(typ string, raw map[string]any) error {
	switch typ {
	case sourceotlp.Type:
		return parse[sourceotlp.Spec](raw)
	case sourcebroker.Type:
		return parseDriver[sourcebroker.Spec, natsdriver.SubscriberSpec](raw, natsdriver.Name)
	case buffersource.Type:
		return parseDriver[buffersource.Spec, sqlitebuffer.Spec](raw, sqlitebuffer.Name)
	case generator.Type:
		return parse[generator.Spec](raw)
	default:
		return unknownType(typ)
	}
}

func parseStage(typ string, raw map[string]any) error {
	switch typ {
	case batch.Type:
		return parse[batch.Spec](raw)
	case filter.Type:
		return parseDriver[filter.Spec, celfilter.Spec](raw, celfilter.Name)
	case stagevalidation.Type:
		return parseDriver[stagevalidation.Spec, celvalidation.Spec](raw, celvalidation.Name)
	case delay.Type:
		return parse[delay.Spec](raw)
	case noop.Type:
		return parse[noop.Spec](raw)
	case transform.Type:
		return parseDriver[transform.Spec, celtransform.Spec](raw, celtransform.Name)
	default:
		return unknownType(typ)
	}
}

func parseSink(typ string, raw map[string]any) error {
	switch typ {
	case sinkdebug.Type:
		return parse[sinkdebug.Spec](raw)
	case sinkbroker.Type:
		return parseDriver[sinkbroker.Spec, natsdriver.PublisherSpec](raw, natsdriver.Name)
	case sinkotlp.Type:
		return parse[sinkotlp.Spec](raw)
	case buffersink.Type:
		return parseDriver[buffersink.Spec, sqlitebuffer.Spec](raw, sqlitebuffer.Name)
	default:
		return unknownType(typ)
	}
}

// parseDriver checks the spec of a component with drivers: the generic
// fields as T, the options of the driver called name, the only driver the
// examples use, as D.
func parseDriver[T, D any](raw map[string]any, name string) error {
	if raw["driver"] != name {
		return fmt.Errorf("unknown driver %v", raw["driver"])
	}
	opts, ok := raw[name].(map[string]any)
	if !ok {
		return fmt.Errorf("missing options of driver %s", name)
	}
	generic := maps.Clone(raw)
	delete(generic, name)
	if err := parse[T](generic); err != nil {
		return err
	}
	return parse[D](opts)
}

func unknownType(typ string) error {
	return fmt.Errorf("unknown component type %q", typ)
}
