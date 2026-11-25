// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package testrun prepares a stream spec for a local test run.
package testrun

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/go-tram/tram/internal/core/domain"
)

const (
	valueFromKey         = "valueFrom"
	secretKeyRefKey      = "secretKeyRef"
	configMapSelectorKey = "configMapSelector"
)

// ErrUnresolvableRef is a reference that cannot be resolved locally. The
// error names the reference, never a secret value.
var ErrUnresolvableRef = errors.New("unresolvable reference")

// Prepare returns a copy of spec in which every reference inside a stage spec
// is resolved from secrets, keyed "<name>/<key>". Source and sink specs are
// left untouched: a test run never instantiates them, the source is a test
// source and every sink is a capture sink. A reference that cannot be
// resolved is an error naming the reference, never its value.
func Prepare(spec domain.StreamSpec, secrets map[string]string) (domain.StreamSpec, error) {
	out := spec
	out.Stages = make([]domain.StageSpec, len(spec.Stages))
	for i, stage := range spec.Stages {
		resolved, err := resolveRefs(stage.Spec, secrets)
		if err != nil {
			return domain.StreamSpec{}, fmt.Errorf("stage %s: %w", stage.Name, err)
		}
		resolvedMap, ok := resolved.(map[string]any)
		if !ok {
			return domain.StreamSpec{}, fmt.Errorf("stage %s: spec is not an object", stage.Name)
		}
		stage.Spec = resolvedMap
		out.Stages[i] = stage
	}
	return out, nil
}

// PrepareAll is Prepare for every component: the references in the specs of
// the source and the sinks are resolved too, for a check that creates them.
func PrepareAll(spec domain.StreamSpec, secrets map[string]string) (domain.StreamSpec, error) {
	out, err := Prepare(spec, secrets)
	if err != nil {
		return domain.StreamSpec{}, err
	}
	if out.Source.Spec, err = resolveSpec(spec.Source.Name, spec.Source.Spec, secrets); err != nil {
		return domain.StreamSpec{}, err
	}
	out.Sinks = make([]domain.SinkSpec, len(spec.Sinks))
	for i, sink := range spec.Sinks {
		if sink.Spec, err = resolveSpec(sink.Name, sink.Spec, secrets); err != nil {
			return domain.StreamSpec{}, err
		}
		out.Sinks[i] = sink
	}
	return out, nil
}

// resolveSpec resolves the references in the spec of the component name; a
// nil spec stays nil.
func resolveSpec(name string, spec map[string]any, secrets map[string]string) (map[string]any, error) {
	if spec == nil {
		return spec, nil
	}
	resolved, err := resolveRefs(spec, secrets)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	resolvedMap, _ := resolved.(map[string]any)
	return resolvedMap, nil
}

// resolveRefs returns a copy of value in which every secret reference has
// been replaced by its value from secrets.
func resolveRefs(value any, secrets map[string]string) (any, error) {
	if isRef(value) {
		return resolveRef(value, secrets)
	}
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			resolved, err := resolveRefs(item, secrets)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			out[key] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			resolved, err := resolveRefs(item, secrets)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out[i] = resolved
		}
		return out, nil
	default:
		return value, nil
	}
}

// isRef reports whether value is a reference:
// {"valueFrom": {"secretKeyRef": {"name": ..., "key": ...}}} or
// {"valueFrom": {"configMapSelector": {...}}}.
func isRef(value any) bool {
	object, ok := value.(map[string]any)
	if !ok || len(object) != 1 {
		return false
	}
	valueFrom, ok := object[valueFromKey].(map[string]any)
	return ok && len(valueFrom) == 1
}

func resolveRef(value any, secrets map[string]string) (any, error) {
	object, _ := value.(map[string]any)
	valueFrom, ok := object[valueFromKey].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported valueFrom", ErrUnresolvableRef)
	}
	if _, isConfigMap := valueFrom[configMapSelectorKey]; isConfigMap {
		return nil, fmt.Errorf("%w: configMapSelector cannot be resolved locally, provide the value directly",
			ErrUnresolvableRef)
	}
	selector, ok := valueFrom[secretKeyRefKey].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported valueFrom", ErrUnresolvableRef)
	}
	name, _ := selector["name"].(string)
	key, _ := selector["key"].(string)
	resolved, ok := secrets[name+"/"+key]
	if !ok {
		return nil, fmt.Errorf("%w: secret %q key %q is not in the secrets file",
			ErrUnresolvableRef, name, key)
	}
	return resolved, nil
}

// LoadSecrets reads a JSON file mapping "<name>/<key>" to secret values.
func LoadSecrets(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	secrets := map[string]string{}
	if err = json.Unmarshal(data, &secrets); err != nil {
		return nil, fmt.Errorf("invalid secrets file %s: %w", path, err)
	}
	return secrets, nil
}
