// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package cel is the CEL driver of the filter stage: one boolean CEL
// expression per signal, evaluated per log record, metric data point or span.
package cel

import (
	"errors"
	"fmt"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/interpreter"
	"github.com/go-playground/validator/v10"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	filterrules "github.com/go-tram/tram/internal/adapters/stage/filter/rules"
	"github.com/go-tram/tram/internal/core/domain"
)

const Name = "cel"

// Spec is the options of the CEL driver, under `cel` in the filter spec.
type Spec struct {
	Logs    string `json:"logs,omitempty"    validate:"required_without_all=Metrics Traces"`
	Metrics string `json:"metrics,omitempty" validate:"required_without_all=Logs Traces"`
	Traces  string `json:"traces,omitempty"  validate:"required_without_all=Logs Metrics"`
}

type Driver struct {
	validator *validator.Validate
}

var _ filterrules.Driver = (*Driver)(nil)

func New(validator *validator.Validate) *Driver {
	return &Driver{validator: validator}
}

func (d *Driver) Compile(opts map[string]any) (filterrules.Rules, error) {
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
		cel.Variable("resource", cel.ObjectType("opentelemetry.proto.resource.v1.Resource")),
		cel.Variable("scope", cel.ObjectType("opentelemetry.proto.common.v1.InstrumentationScope")),
	)
	if err != nil {
		return nil, errors.Join(ErrEnvironmentInitialization, err)
	}

	r := &rules{}
	if expression := spec.Logs; expression != "" {
		r.logs, err = compile(env, expression,
			cel.Variable("record", cel.ObjectType("opentelemetry.proto.logs.v1.LogRecord")))
		if err != nil {
			return nil, errors.Join(ErrInvalidLogsExpression, err)
		}
	}

	if expression := spec.Metrics; expression != "" {
		r.metrics, err = compile(env, expression,
			cel.Variable("metric", cel.ObjectType("opentelemetry.proto.metrics.v1.Metric")),
			cel.Variable("datapoint", cel.DynType))
		if err != nil {
			return nil, errors.Join(ErrInvalidMetricsExpression, err)
		}
	}

	if expression := spec.Traces; expression != "" {
		r.traces, err = compile(env, expression,
			cel.Variable("span", cel.ObjectType("opentelemetry.proto.trace.v1.Span")))
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
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("CEL expression must return a bool, not %s", t)
	}
	return env.Program(ast)
}

// rules holds a program per signal; a nil program keeps every record.
type rules struct {
	logs    cel.Program
	metrics cel.Program
	traces  cel.Program
}

// Predicates shares one activation between the predicates of an event, so
// evaluating a record allocates nothing for the variables.
func (r *rules) Predicates() filterrules.Predicates {
	vars := &activation{emptyResource: &resourcepb.Resource{}, emptyScope: &commonpb.InstrumentationScope{}}
	var p filterrules.Predicates
	if r.logs != nil {
		p.Logs = func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, record *logspb.LogRecord) (bool, error) {
			vars.set(resource, scope)
			vars.record = record
			return eval(r.logs, vars)
		}
	}
	if r.metrics != nil {
		p.Metrics = func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, metric *metricspb.Metric, dataPoint proto.Message) (bool, error) {
			vars.set(resource, scope)
			vars.metric = metric
			vars.dataPoint = dataPoint
			return eval(r.metrics, vars)
		}
	}
	if r.traces != nil {
		p.Traces = func(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, span *tracepb.Span) (bool, error) {
			vars.set(resource, scope)
			vars.span = span
			return eval(r.traces, vars)
		}
	}
	return p
}

func eval(program cel.Program, vars *activation) (bool, error) {
	out, _, err := program.Eval(vars)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrEvaluation, err)
	}
	keep, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("%w: expression returned %T instead of bool", ErrEvaluation, out.Value())
	}
	return keep, nil
}

type activation struct {
	emptyResource *resourcepb.Resource
	emptyScope    *commonpb.InstrumentationScope

	resource  *resourcepb.Resource
	scope     *commonpb.InstrumentationScope
	record    *logspb.LogRecord
	metric    *metricspb.Metric
	dataPoint proto.Message
	span      *tracepb.Span
}

func (a *activation) set(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope) {
	a.resource, a.scope = resource, scope
	if a.resource == nil {
		a.resource = a.emptyResource
	}
	if a.scope == nil {
		a.scope = a.emptyScope
	}
}

func (a *activation) ResolveName(name string) (any, bool) {
	switch name {
	case "resource":
		return a.resource, true
	case "scope":
		return a.scope, true
	case "record":
		return a.record, a.record != nil
	case "metric":
		return a.metric, a.metric != nil
	case "datapoint":
		return a.dataPoint, a.dataPoint != nil
	case "span":
		return a.span, a.span != nil
	default:
		return nil, false
	}
}

func (a *activation) Parent() interpreter.Activation {
	return nil
}
