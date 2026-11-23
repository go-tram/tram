// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package celentry evaluates a CEL expression per log record, metric data
// point and span, so sinks can route each entry by its own key, e.g. the NATS
// sink by subject and the OTLP sink by URL.
package celentry

import (
	"errors"
	"fmt"
	"reflect"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/interpreter"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/core/domain"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	logsVariable    = "logs"
	metricsVariable = "metrics"
	tracesVariable  = "traces"

	resourceVariable = "resource"
	scopeVariable    = "scope"
	recordVariable   = "record"

	metricVariable    = "metric"
	dataPointVariable = "datapoint"
	spanVariable      = "span"

	resourceType = "opentelemetry.proto.resource.v1.Resource"
	scopeType    = "opentelemetry.proto.common.v1.InstrumentationScope"

	getAttrArguments = 2
)

var (
	ErrEnvironment              = errors.New("failed to initialize CEL environment")
	ErrInvalidLogsExpression    = errors.New("invalid logs CEL expression")
	ErrInvalidMetricsExpression = errors.New("invalid metrics CEL expression")
	ErrInvalidTracesExpression  = errors.New("invalid traces CEL expression")
)

// Expressions holds an expression per signal. An empty one compiles to true.
type Expressions struct {
	Logs    string
	Metrics string
	Traces  string
}

type Programs struct {
	Logs    cel.Program
	Metrics cel.Program
	Traces  cel.Program
}

// Compile compiles each expression with the variables of its entries. extra
// declares further variables, which callers set per entry with Vars.Set.
func Compile(e Expressions, extra ...cel.EnvOption) (Programs, error) {
	getAttrFunc := cel.Function("get_attr",
		cel.MemberOverload("attributes_get_attr",
			[]*cel.Type{
				cel.ListType(cel.ObjectType("opentelemetry.proto.common.v1.KeyValue")),
				cel.StringType,
			},
			cel.StringType,
			cel.FunctionBinding(getAttr),
		),
	)

	opts := append([]cel.EnvOption{
		cel.TypeDescs(
			commonpb.File_opentelemetry_proto_common_v1_common_proto,
			resourcepb.File_opentelemetry_proto_resource_v1_resource_proto,
			logspb.File_opentelemetry_proto_logs_v1_logs_proto,
			metricspb.File_opentelemetry_proto_metrics_v1_metrics_proto,
			tracespb.File_opentelemetry_proto_trace_v1_trace_proto,
		),
		getAttrFunc,
	}, extra...)
	baseEnv, err := cel.NewEnv(opts...)
	if err != nil {
		return Programs{}, errors.Join(ErrEnvironment, err)
	}

	var compiled Programs
	compiled.Logs, err = compile(
		baseEnv,
		e.Logs,
		cel.Variable(logsVariable, cel.ObjectType("opentelemetry.proto.logs.v1.LogsData")),
		cel.Variable(resourceVariable, cel.ObjectType(resourceType)),
		cel.Variable(scopeVariable, cel.ObjectType(scopeType)),
		cel.Variable(recordVariable, cel.ObjectType("opentelemetry.proto.logs.v1.LogRecord")),
	)
	if err != nil {
		return Programs{}, errors.Join(ErrInvalidLogsExpression, err)
	}

	compiled.Metrics, err = compile(
		baseEnv,
		e.Metrics,
		cel.Variable(metricsVariable, cel.ObjectType("opentelemetry.proto.metrics.v1.MetricsData")),
		cel.Variable(resourceVariable, cel.ObjectType(resourceType)),
		cel.Variable(scopeVariable, cel.ObjectType(scopeType)),
		cel.Variable(metricVariable, cel.ObjectType("opentelemetry.proto.metrics.v1.Metric")),
		// Its type depends on the metric's type.
		cel.Variable(dataPointVariable, cel.DynType),
	)
	if err != nil {
		return Programs{}, errors.Join(ErrInvalidMetricsExpression, err)
	}

	compiled.Traces, err = compile(
		baseEnv,
		e.Traces,
		cel.Variable(tracesVariable, cel.ObjectType("opentelemetry.proto.trace.v1.TracesData")),
		cel.Variable(resourceVariable, cel.ObjectType(resourceType)),
		cel.Variable(scopeVariable, cel.ObjectType(scopeType)),
		cel.Variable(spanVariable, cel.ObjectType("opentelemetry.proto.trace.v1.Span")),
	)
	if err != nil {
		return Programs{}, errors.Join(ErrInvalidTracesExpression, err)
	}
	return compiled, nil
}

func compile(celEnv *cel.Env, expression string, opts ...cel.EnvOption) (cel.Program, error) {
	env, err := celEnv.Extend(opts...)
	if err != nil {
		return nil, err
	}
	if expression == "" {
		expression = "true"
	}
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("failed to compile CEL expression: %w", issues.Err())
	}

	celProgram, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to build CEL program: %w", err)
	}
	return celProgram, nil
}

func getAttr(args ...ref.Val) ref.Val {
	if len(args) != getAttrArguments {
		return types.NewErr("get_attr expects exactly 2 arguments")
	}

	goList, err := args[0].ConvertToNative(reflect.TypeFor[[]*commonpb.KeyValue]())
	if err != nil {
		return types.NewErr("failed to convert attributes to Go slice: %v", err)
	}

	list, ok := goList.([]*commonpb.KeyValue)
	if !ok {
		return types.NewErr("attributes is not of type []*KeyValue")
	}

	searchKey, ok := args[1].Value().(string)
	if !ok {
		return types.NewErr("invalid key type")
	}

	for _, kv := range list {
		if kv.GetKey() == searchKey {
			return types.String(kv.GetValue().GetStringValue())
		}
	}
	return types.String("")
}

// String evaluates program, which must return a string.
func String(program cel.Program, vars *Vars) (string, error) {
	out, _, err := program.Eval(vars)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate CEL: %w", err)
	}
	value, ok := out.Value().(string)
	if !ok {
		return "", fmt.Errorf("CEL expression returned %T instead of a string", out.Value())
	}
	return value, nil
}

// Resolve returns the key of the entry in vars, usually by evaluating program.
type Resolve func(program cel.Program, vars *Vars) (string, error)

// The ...Key functions resolve a key per entry, with the variables set to the
// entry, its resource and scope, and a payload that holds only the entry. The
// payload is built once per scope.

//nolint:dupl // Same shape as SpanKey, for another signal.
func LogKey(program cel.Program, resolve Resolve) domain.LogKey {
	vars := &Vars{name: logsVariable}
	var resourceIn *logspb.ResourceLogs
	var scopeIn *logspb.ScopeLogs
	current := make([]*logspb.LogRecord, 1)
	return func(r *logspb.ResourceLogs, s *logspb.ScopeLogs, record *logspb.LogRecord) (string, error) {
		if r != resourceIn || s != scopeIn {
			resourceIn, scopeIn = r, s
			vars.setContainers(r.GetResource(), s.GetScope())
			vars.value = &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
				Resource:  r.GetResource(),
				SchemaUrl: r.GetSchemaUrl(),
				ScopeLogs: []*logspb.ScopeLogs{{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), LogRecords: current}},
			}}}
		}
		current[0] = record
		vars.record = record
		return resolve(program, vars)
	}
}

//nolint:dupl // Same shape as LogKey, for another signal.
func SpanKey(program cel.Program, resolve Resolve) domain.SpanKey {
	vars := &Vars{name: tracesVariable}
	var resourceIn *tracespb.ResourceSpans
	var scopeIn *tracespb.ScopeSpans
	current := make([]*tracespb.Span, 1)
	return func(r *tracespb.ResourceSpans, s *tracespb.ScopeSpans, span *tracespb.Span) (string, error) {
		if r != resourceIn || s != scopeIn {
			resourceIn, scopeIn = r, s
			vars.setContainers(r.GetResource(), s.GetScope())
			vars.value = &tracespb.TracesData{ResourceSpans: []*tracespb.ResourceSpans{{
				Resource:   r.GetResource(),
				SchemaUrl:  r.GetSchemaUrl(),
				ScopeSpans: []*tracespb.ScopeSpans{{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), Spans: current}},
			}}}
		}
		current[0] = span
		vars.span = span
		return resolve(program, vars)
	}
}

// MetricKey resolves per data point. The payload holds the whole metric.
func MetricKey(program cel.Program, resolve Resolve) domain.MetricKey {
	vars := &Vars{name: metricsVariable}
	var resourceIn *metricspb.ResourceMetrics
	var scopeIn *metricspb.ScopeMetrics
	current := make([]*metricspb.Metric, 1)
	return func(
		r *metricspb.ResourceMetrics,
		s *metricspb.ScopeMetrics,
		m *metricspb.Metric,
		dataPoint proto.Message,
	) (string, error) {
		if r != resourceIn || s != scopeIn {
			resourceIn, scopeIn = r, s
			vars.setContainers(r.GetResource(), s.GetScope())
			vars.value = &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{
				{
					Resource:  r.GetResource(),
					SchemaUrl: r.GetSchemaUrl(),
					ScopeMetrics: []*metricspb.ScopeMetrics{
						{Scope: s.GetScope(), SchemaUrl: s.GetSchemaUrl(), Metrics: current},
					},
				},
			}}
		}
		current[0] = m
		vars.metric = m
		vars.dataPoint = dataPoint
		return resolve(program, vars)
	}
}

// Vars are the variables of one entry.
type Vars struct {
	name      string
	value     any
	resource  any
	scope     any
	record    any
	metric    any
	dataPoint proto.Message
	span      any

	extraName  string
	extraValue any

	emptyResource *resourcepb.Resource
	emptyScope    *commonpb.InstrumentationScope
}

// Set sets a variable declared through the extra options of Compile. Vars hold
// one such variable.
func (v *Vars) Set(name string, value any) {
	v.extraName, v.extraValue = name, value
}

// setContainers sets the resource and scope variables. A missing resource or
// scope is an empty message, so expressions can read its fields without checks.
func (v *Vars) setContainers(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope) {
	if resource == nil {
		if v.emptyResource == nil {
			v.emptyResource = &resourcepb.Resource{}
		}
		resource = v.emptyResource
	}
	if scope == nil {
		if v.emptyScope == nil {
			v.emptyScope = &commonpb.InstrumentationScope{}
		}
		scope = v.emptyScope
	}
	v.resource, v.scope = resource, scope
}

func (v *Vars) ResolveName(name string) (any, bool) {
	switch name {
	case v.name:
		return v.value, true
	case resourceVariable:
		return v.resource, true
	case scopeVariable:
		return v.scope, true

	case recordVariable:
		return v.record, true
	case metricVariable:
		return v.metric, true
	case dataPointVariable:
		// Absent for a metric without data points, so reading it fails.
		return v.dataPoint, v.dataPoint != nil
	case spanVariable:
		return v.span, true
	}
	if name == v.extraName && name != "" {
		return v.extraValue, true
	}
	return nil, false
}

func (v *Vars) Parent() interpreter.Activation {
	return nil
}
