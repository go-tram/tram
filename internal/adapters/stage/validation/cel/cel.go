// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package cel is the CEL driver of the validation stage: one boolean CEL
// expression per signal, evaluated once on the whole signal of an event as
// `logs`, `metrics` or `traces`.
package cel

import (
	"errors"
	"fmt"

	"cel.dev/cel-go/cel"
	"github.com/go-playground/validator/v10"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	validationrules "github.com/go-tram/tram/internal/adapters/stage/validation/rules"
	"github.com/go-tram/tram/internal/core/domain"
)

const Name = "cel"

// Spec is the options of the CEL driver, under `cel` in the validation spec.
// A signal without an expression matches.
type Spec struct {
	Logs    string `json:"logs,omitempty"`
	Metrics string `json:"metrics,omitempty"`
	Traces  string `json:"traces,omitempty"`
}

type Driver struct {
	validator *validator.Validate
}

var _ validationrules.Driver = (*Driver)(nil)

func New(validator *validator.Validate) *Driver {
	return &Driver{validator: validator}
}

func (d *Driver) Compile(opts map[string]any) (validationrules.Rules, error) {
	spec, err := domain.ParseSpec(opts, new(Spec))
	if err != nil {
		return nil, err
	}
	if err = d.validator.Struct(spec); err != nil {
		return nil, err
	}

	env, err := cel.NewEnv(
		cel.TypeDescs(
			commonpb.File_opentelemetry_proto_common_v1_common_proto,
			resourcepb.File_opentelemetry_proto_resource_v1_resource_proto,
			logspb.File_opentelemetry_proto_logs_v1_logs_proto,
			metricspb.File_opentelemetry_proto_metrics_v1_metrics_proto,
			tracepb.File_opentelemetry_proto_trace_v1_trace_proto,
		),
	)
	if err != nil {
		return nil, errors.Join(ErrEnvironmentInitialization, err)
	}

	r := &rules{}
	if expression := spec.Logs; expression != "" {
		r.logs, err = compile(env, expression,
			cel.Variable("logs", cel.ObjectType("opentelemetry.proto.logs.v1.LogsData")))
		if err != nil {
			return nil, errors.Join(ErrInvalidLogsExpression, err)
		}
	}
	if expression := spec.Metrics; expression != "" {
		r.metrics, err = compile(env, expression,
			cel.Variable("metrics", cel.ObjectType("opentelemetry.proto.metrics.v1.MetricsData")))
		if err != nil {
			return nil, errors.Join(ErrInvalidMetricsExpression, err)
		}
	}
	if expression := spec.Traces; expression != "" {
		r.traces, err = compile(env, expression,
			cel.Variable("traces", cel.ObjectType("opentelemetry.proto.trace.v1.TracesData")))
		if err != nil {
			return nil, errors.Join(ErrInvalidTracesExpression, err)
		}
	}
	return r, nil
}

func compile(env *cel.Env, expression string, vars ...cel.EnvOption) (cel.Program, error) {
	env, err := env.Extend(vars...)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression: %w", issues.Err())
	}
	program, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL program: %w", err)
	}
	return program, nil
}

// rules holds a program per signal; a signal without an expression has no
// program and matches.
type rules struct {
	logs    cel.Program
	metrics cel.Program
	traces  cel.Program
}

func (r *rules) Logs(data *logspb.LogsData) (bool, error) {
	return eval(r.logs, "logs", data)
}

func (r *rules) Metrics(data *metricspb.MetricsData) (bool, error) {
	return eval(r.metrics, "metrics", data)
}

func (r *rules) Traces(data *tracepb.TracesData) (bool, error) {
	return eval(r.traces, "traces", data)
}

func eval(program cel.Program, name string, data any) (bool, error) {
	if program == nil {
		return true, nil
	}
	out, _, err := program.Eval(map[string]any{name: data})
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrEvaluation, err)
	}
	match, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("%w: expression returned %T instead of bool", ErrEvaluation, out.Value())
	}
	return match, nil
}
