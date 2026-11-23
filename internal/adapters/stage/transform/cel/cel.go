// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package cel is the CEL driver of the transform stage: a list of statements
// per signal, each changing one attribute, with the value and an optional
// condition as CEL expressions.
package cel

import (
	"errors"
	"fmt"
	"math"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/ext"
	"github.com/google/cel-go/interpreter"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/stage/transform/statements"
	"github.com/go-tram/tram/internal/core/domain"
)

const Name = "cel"

// Spec is the options of the CEL driver, under `cel` in the transform spec:
// the statements per signal, run in order.
type Spec struct {
	Logs    []Statement `json:"logs,omitempty"`
	Metrics []Statement `json:"metrics,omitempty"`
	Traces  []Statement `json:"traces,omitempty"`
}

// Statement changes one attribute. Exactly one of Set, Delete, Rename and
// Truncate names the target path; Value, To and Length are their arguments.
type Statement struct {
	Set      string `json:"set,omitempty"`
	Delete   string `json:"delete,omitempty"`
	Rename   string `json:"rename,omitempty"`
	Truncate string `json:"truncate,omitempty"`

	// Value is the CEL expression whose result Set sets.
	Value string `json:"value,omitempty"`
	// To is the key Rename moves the value to, in the same attributes.
	To string `json:"to,omitempty"`
	// Length is the number of characters Truncate keeps.
	Length int `json:"length,omitempty"`
	// When is an optional boolean CEL expression; the statement only runs
	// where it is true.
	When string `json:"when,omitempty"`
}

var (
	ErrNoStatements       = errors.New("cel needs statements for at least one of logs, metrics and traces")
	ErrInvalidStatement   = errors.New("invalid statement")
	ErrEnvironment        = errors.New("failed to initialize the CEL environment")
	errValueNotSupported  = errors.New("unsupported value type")
	errConditionNotABool  = errors.New("condition did not return a bool")
	errUnsupportedOperand = errors.New("attr needs a list of attributes")
)

type Driver struct{}

var _ statements.Driver = Driver{}

func New() Driver { return Driver{} }

func (Driver) Compile(opts map[string]any) (statements.Program, error) {
	spec, err := domain.ParseSpec(opts, new(Spec))
	if err != nil {
		return nil, err
	}
	if len(spec.Logs)+len(spec.Metrics)+len(spec.Traces) == 0 {
		return nil, ErrNoStatements
	}
	base, err := baseEnv()
	if err != nil {
		return nil, errors.Join(ErrEnvironment, err)
	}
	p := &program{}
	if p.logs, err = compileSignal(base, domain.SignalLogs, spec.Logs,
		cel.Variable("record", cel.ObjectType("opentelemetry.proto.logs.v1.LogRecord"))); err != nil {
		return nil, err
	}
	if p.metrics, err = compileSignal(base, domain.SignalMetrics, spec.Metrics,
		cel.Variable("metric", cel.ObjectType("opentelemetry.proto.metrics.v1.Metric")),
		cel.Variable("datapoint", cel.DynType)); err != nil {
		return nil, err
	}
	if p.traces, err = compileSignal(base, domain.SignalTraces, spec.Traces,
		cel.Variable("span", cel.ObjectType("opentelemetry.proto.trace.v1.Span"))); err != nil {
		return nil, err
	}
	return p, nil
}

// baseEnv knows the OTLP types and the attr function, without variables.
func baseEnv() (*cel.Env, error) {
	// The registry converts the attribute values attr cannot turn into a CEL
	// scalar, arrays and key-value lists, into CEL objects.
	registry, err := types.NewRegistry(&commonpb.AnyValue{})
	if err != nil {
		return nil, err
	}
	return cel.NewEnv(
		cel.TypeDescs(
			commonpb.File_opentelemetry_proto_common_v1_common_proto,
			resourcepb.File_opentelemetry_proto_resource_v1_resource_proto,
			logspb.File_opentelemetry_proto_logs_v1_logs_proto,
			metricspb.File_opentelemetry_proto_metrics_v1_metrics_proto,
			tracepb.File_opentelemetry_proto_trace_v1_trace_proto,
		),
		// String functions such as split, replace, lowerAscii and substring.
		ext.Strings(),
		// attr(attributes, 'key') is the value of key in attributes, or null.
		// The attributes are dyn, so the attributes of a data point, whose
		// type depends on the metric, work like those of records and spans.
		cel.Function("attr",
			cel.Overload("attr_dyn_string", []*cel.Type{cel.DynType, cel.StringType}, cel.DynType,
				cel.BinaryBinding(func(list, key ref.Val) ref.Val { return attr(registry, list, key) })),
		),
	)
}

// compileSignal compiles the statements of one signal; the entry variables
// are those of the signal's entries.
func compileSignal(
	base *cel.Env,
	signal domain.Signal,
	specs []Statement,
	entryVars ...cel.EnvOption,
) (*signalProgram, error) {
	if len(specs) == 0 {
		return nil, nil //nolint:nilnil // A signal without statements has no program.
	}
	envs := map[statements.Context]*cel.Env{
		statements.ContextEntry: nil, statements.ContextScope: nil, statements.ContextResource: nil,
	}
	var err error
	resourceVar := cel.Variable("resource", cel.ObjectType("opentelemetry.proto.resource.v1.Resource"))
	scopeVar := cel.Variable("scope", cel.ObjectType("opentelemetry.proto.common.v1.InstrumentationScope"))
	if envs[statements.ContextResource], err = base.Extend(resourceVar); err != nil {
		return nil, errors.Join(ErrEnvironment, err)
	}
	if envs[statements.ContextScope], err = base.Extend(resourceVar, scopeVar); err != nil {
		return nil, errors.Join(ErrEnvironment, err)
	}
	if envs[statements.ContextEntry], err = base.Extend(
		append([]cel.EnvOption{resourceVar, scopeVar}, entryVars...)...); err != nil {
		return nil, errors.Join(ErrEnvironment, err)
	}

	sp := &signalProgram{}
	for i, spec := range specs {
		st, compileErr := compileStatement(envs, spec)
		if compileErr != nil {
			return nil, fmt.Errorf("%w %d of %s: %w", ErrInvalidStatement, i+1, signal, compileErr)
		}
		switch st.path.Context {
		case statements.ContextResource:
			sp.resource = append(sp.resource, st)
		case statements.ContextScope:
			sp.scope = append(sp.scope, st)
		case statements.ContextEntry:
			sp.entry = append(sp.entry, st)
		}
	}
	return sp, nil
}

type operation int

const (
	opSet operation = iota
	opDelete
	opRename
	opTruncate
)

type statement struct {
	op     operation
	path   statements.Path
	to     string
	length int
	value  cel.Program
	when   cel.Program
}

func compileStatement(envs map[statements.Context]*cel.Env, spec Statement) (*statement, error) {
	st := &statement{}
	var target string
	ops := 0
	for _, candidate := range []struct {
		op   operation
		path string
	}{{opSet, spec.Set}, {opDelete, spec.Delete}, {opRename, spec.Rename}, {opTruncate, spec.Truncate}} {
		if candidate.path != "" {
			st.op, target = candidate.op, candidate.path
			ops++
		}
	}
	if ops != 1 {
		return nil, errors.New("needs exactly one of set, delete, rename and truncate")
	}
	var err error
	if st.path, err = statements.ParsePath(target); err != nil {
		return nil, err
	}
	env := envs[st.path.Context]

	switch st.op {
	case opSet:
		if spec.Value == "" {
			return nil, errors.New("set needs a value")
		}
		if st.value, err = compileExpression(env, spec.Value, false); err != nil {
			return nil, fmt.Errorf("value: %w", err)
		}
	case opRename:
		if spec.To == "" {
			return nil, errors.New("rename needs to")
		}
		st.to = spec.To
	case opTruncate:
		if spec.Length < 1 {
			return nil, errors.New("truncate needs a length of at least 1")
		}
		st.length = spec.Length
	case opDelete:
	}
	if st.op != opSet && spec.Value != "" {
		return nil, errors.New("value is only for set")
	}
	if spec.When != "" {
		if st.when, err = compileExpression(env, spec.When, true); err != nil {
			return nil, fmt.Errorf("when: %w", err)
		}
	}
	return st, nil
}

func compileExpression(env *cel.Env, expression string, boolean bool) (cel.Program, error) {
	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	if boolean && ast.OutputType() != cel.BoolType && ast.OutputType() != cel.DynType {
		return nil, fmt.Errorf("must return a bool, not %s", ast.OutputType())
	}
	return env.Program(ast)
}

type program struct {
	logs, metrics, traces *signalProgram
}

func (p *program) Logs() statements.SignalProgram    { return orNil(p.logs) }
func (p *program) Metrics() statements.SignalProgram { return orNil(p.metrics) }
func (p *program) Traces() statements.SignalProgram  { return orNil(p.traces) }

// orNil keeps a nil *signalProgram from becoming a non-nil interface.
func orNil(sp *signalProgram) statements.SignalProgram {
	if sp == nil {
		return nil
	}
	return sp
}

// signalProgram holds the statements of one signal by context, each list in
// the order of the spec.
type signalProgram struct {
	resource, scope, entry []*statement
}

func (sp *signalProgram) Resource(resource *resourcepb.Resource, onError statements.OnError) statements.Action {
	if len(sp.resource) == 0 {
		return statements.Continue
	}
	return run(sp.resource, &activation{resource: resource}, &resource.Attributes, onError)
}

func (sp *signalProgram) Scope(
	resource *resourcepb.Resource,
	scope *commonpb.InstrumentationScope,
	onError statements.OnError,
) statements.Action {
	if len(sp.scope) == 0 {
		return statements.Continue
	}
	return run(sp.scope, &activation{resource: resource, scope: scope}, &scope.Attributes, onError)
}

func (sp *signalProgram) Entry(entry statements.Entry, onError statements.OnError) statements.Action {
	if len(sp.entry) == 0 {
		return statements.Continue
	}
	vars := &activation{
		resource: entry.Resource, scope: entry.Scope,
		record: entry.Record, metric: entry.Metric, dataPoint: entry.DataPoint, span: entry.Span,
	}
	return run(sp.entry, vars, entry.Attributes, onError)
}

// activation holds the variables of one evaluation. A struct costs one
// allocation per resource, scope or entry, a map several.
type activation struct {
	resource  *resourcepb.Resource
	scope     *commonpb.InstrumentationScope
	record    *logspb.LogRecord
	metric    *metricspb.Metric
	dataPoint proto.Message
	span      *tracepb.Span
}

var _ interpreter.Activation = (*activation)(nil)

func (a *activation) ResolveName(name string) (any, bool) {
	switch name {
	case "resource":
		return a.resource, a.resource != nil
	case "scope":
		return a.scope, a.scope != nil
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

func (a *activation) Parent() interpreter.Activation { return nil }

// run runs the statements of one context on the attributes. A failed
// statement asks onError; anything but Continue ends the run.
func run(
	list []*statement,
	vars *activation,
	attributes *[]*commonpb.KeyValue,
	onError statements.OnError,
) statements.Action {
	for _, st := range list {
		if err := st.apply(vars, attributes); err != nil {
			if action := onError(fmt.Errorf("%s: %w", st.path, err)); action != statements.Continue {
				return action
			}
		}
	}
	return statements.Continue
}

func (st *statement) apply(vars *activation, attributes *[]*commonpb.KeyValue) error {
	if st.when != nil {
		out, _, err := st.when.Eval(vars)
		if err != nil {
			return err
		}
		ok, isBool := out.(types.Bool)
		if !isBool {
			return errConditionNotABool
		}
		if !ok {
			return nil
		}
	}
	key := st.path.Key
	switch st.op {
	case opSet:
		out, _, err := st.value.Eval(vars)
		if err != nil {
			return err
		}
		value, err := anyValue(out)
		if err != nil {
			return err
		}
		statements.Set(attributes, key, value)
	case opDelete:
		statements.Delete(attributes, key)
	case opRename:
		statements.Rename(attributes, key, st.to)
	case opTruncate:
		statements.Truncate(attributes, key, st.length)
	}
	return nil
}

// anyValue converts the result of a value expression into an attribute
// value. Lists, maps and null are not supported; null is a failure rather
// than a deletion, so a missing attribute does not silently remove another.
func anyValue(v ref.Val) (*commonpb.AnyValue, error) {
	switch v := v.(type) {
	case types.String:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: string(v)}}, nil
	case types.Bool:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: bool(v)}}, nil
	case types.Int:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(v)}}, nil
	case types.Uint:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("%w: uint %d does not fit an int attribute", errValueNotSupported, uint64(v))
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(v)}}, nil
	case types.Double:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: float64(v)}}, nil
	case types.Bytes:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte(v)}}, nil
	default:
		return nil, fmt.Errorf("%w: %s", errValueNotSupported, v.Type())
	}
}

// attr returns the value of key in a list of attributes as a CEL value: a
// string, bool, int, double or bytes, the AnyValue for arrays and key-value
// lists, and null if the key is missing.
func attr(registry *types.Registry, list, key ref.Val) ref.Val {
	lister, ok := list.(traits.Lister)
	if !ok {
		return types.NewErr("%s", errUnsupportedOperand.Error())
	}
	want, ok := key.(types.String)
	if !ok {
		return types.NewErr("attr needs a string key")
	}
	for it := lister.Iterator(); it.HasNext() == types.True; {
		kv, isKV := it.Next().Value().(*commonpb.KeyValue)
		if !isKV || kv.GetKey() != string(want) {
			continue
		}
		switch v := kv.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			return types.String(v.StringValue)
		case *commonpb.AnyValue_BoolValue:
			return types.Bool(v.BoolValue)
		case *commonpb.AnyValue_IntValue:
			return types.Int(v.IntValue)
		case *commonpb.AnyValue_DoubleValue:
			return types.Double(v.DoubleValue)
		case *commonpb.AnyValue_BytesValue:
			return types.Bytes(v.BytesValue)
		default:
			return registry.NativeToValue(kv.GetValue())
		}
	}
	return types.NullValue
}
