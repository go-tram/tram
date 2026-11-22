// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package cel_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/go-tram/tram/internal/adapters/stage/transform/cel"
	st "github.com/go-tram/tram/internal/adapters/stage/transform/statements"
)

func statement(fields map[string]any) map[string]any { return fields }

func compile(t *testing.T, logs ...map[string]any) (st.Program, error) {
	t.Helper()
	list := make([]any, len(logs))
	for i, l := range logs {
		list[i] = l
	}
	return cel.New().Compile(map[string]any{"logs": list})
}

func TestCompileRefusesInvalidStatements(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		stmt map[string]any
	}{
		{name: "no operation", stmt: statement(map[string]any{"value": "'x'"})},
		{
			name: "two operations",
			stmt: statement(map[string]any{"set": "attributes['a']", "delete": "attributes['b']", "value": "'x'"}),
		},
		{name: "set without value", stmt: statement(map[string]any{"set": "attributes['a']"})},
		{name: "value on delete", stmt: statement(map[string]any{"delete": "attributes['a']", "value": "'x'"})},
		{name: "rename without to", stmt: statement(map[string]any{"rename": "attributes['a']"})},
		{name: "truncate without length", stmt: statement(map[string]any{"truncate": "attributes['a']"})},
		{name: "invalid path", stmt: statement(map[string]any{"delete": "body"})},
		{name: "invalid expression", stmt: statement(map[string]any{"set": "attributes['a']", "value": "1 +"})},
		{name: "condition is no bool", stmt: statement(map[string]any{"delete": "attributes['a']", "when": "'yes'"})},
		{name: "entry variable in a resource statement",
			stmt: statement(map[string]any{"set": "resource.attributes['a']", "value": "record.severity_text"})},
		{name: "scope variable in a resource statement",
			stmt: statement(map[string]any{"set": "resource.attributes['a']", "value": "scope.name"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := compile(t, tc.stmt)
			require.ErrorIs(t, err, cel.ErrInvalidStatement)
			assert.Contains(t, err.Error(), "statement 1 of logs")
		})
	}
}

func TestCompileNeedsStatements(t *testing.T) {
	t.Parallel()
	_, err := cel.New().Compile(map[string]any{})
	require.ErrorIs(t, err, cel.ErrNoStatements)
}

func TestSignalsWithoutStatementsHaveNoProgram(t *testing.T) {
	t.Parallel()
	p, err := compile(t, statement(map[string]any{"delete": "attributes['a']"}))
	require.NoError(t, err)
	assert.NotNil(t, p.Logs())
	assert.Nil(t, p.Metrics())
	assert.Nil(t, p.Traces())
}

func str(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}

// runEntry runs the logs statements of the spec on one record.
func runEntry(t *testing.T, resource *resourcepb.Resource, record *logspb.LogRecord, stmts ...map[string]any) []error {
	t.Helper()
	p, err := compile(t, stmts...)
	require.NoError(t, err)
	var failures []error
	p.Logs().Entry(st.Entry{
		Resource: resource, Scope: &commonpb.InstrumentationScope{},
		Record: record, Attributes: &record.Attributes,
	}, func(err error) st.Action {
		failures = append(failures, err)
		return st.Continue
	})
	return failures
}

func value(attrs []*commonpb.KeyValue, key string) *commonpb.AnyValue {
	for _, a := range attrs {
		if a.GetKey() == key {
			return a.GetValue()
		}
	}
	return nil
}

func TestAttrReadsEveryValueType(t *testing.T) {
	t.Parallel()
	resource := &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		{Key: "service.name", Value: str("checkout-eu")},
		{Key: "replicas", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 3}}},
		{Key: "canary", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
		{Key: "ratio", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 0.5}}},
	}}
	record := &logspb.LogRecord{SeverityText: "WARN"}

	failures := runEntry(
		t,
		resource,
		record,
		statement(
			map[string]any{
				"set":   "attributes['service']",
				"value": "attr(resource.attributes, 'service.name').split('-')[0]",
			},
		),
		statement(
			map[string]any{"set": "attributes['replicas']", "value": "attr(resource.attributes, 'replicas') + 1"},
		),
		statement(map[string]any{"set": "attributes['canary']", "value": "attr(resource.attributes, 'canary')"}),
		statement(map[string]any{"set": "attributes['ratio']", "value": "attr(resource.attributes, 'ratio') * 2.0"}),
		statement(map[string]any{"set": "attributes['missing']", "value": "attr(resource.attributes, 'nope') == null"}),
		statement(map[string]any{"set": "attributes['level']", "value": "record.severity_text.lowerAscii()"}),
	)

	require.Empty(t, failures)
	assert.Equal(t, "checkout", value(record.GetAttributes(), "service").GetStringValue())
	assert.Equal(t, int64(4), value(record.GetAttributes(), "replicas").GetIntValue())
	assert.True(t, value(record.GetAttributes(), "canary").GetBoolValue())
	assert.InDelta(t, 1.0, value(record.GetAttributes(), "ratio").GetDoubleValue(), 1e-9)
	assert.True(t, value(record.GetAttributes(), "missing").GetBoolValue())
	assert.Equal(t, "warn", value(record.GetAttributes(), "level").GetStringValue())
}

func TestFailedStatementsAreReportedAndSkipped(t *testing.T) {
	t.Parallel()
	record := &logspb.LogRecord{Attributes: []*commonpb.KeyValue{{Key: "user", Value: str("ann")}}}

	failures := runEntry(t, &resourcepb.Resource{}, record,
		// null is no value: the attribute is missing.
		statement(map[string]any{"set": "attributes['team']", "value": "attr(record.attributes, 'team')"}),
		// A list is not supported as a value.
		statement(map[string]any{"set": "attributes['list']", "value": "[1, 2]"}),
		// Statements after a failed one still run.
		statement(map[string]any{"rename": "attributes['user']", "to": "user.name"}),
	)

	require.Len(t, failures, 2)
	assert.Contains(t, failures[0].Error(), `attributes["team"]`)
	assert.Nil(t, value(record.GetAttributes(), "team"))
	assert.Equal(t, "ann", value(record.GetAttributes(), "user.name").GetStringValue())
}

func TestWhenLimitsTheStatement(t *testing.T) {
	t.Parallel()
	matching := &logspb.LogRecord{
		SeverityText: "DEBUG",
		Attributes:   []*commonpb.KeyValue{{Key: "payload", Value: str("x")}},
	}
	other := &logspb.LogRecord{
		SeverityText: "ERROR",
		Attributes:   []*commonpb.KeyValue{{Key: "payload", Value: str("x")}},
	}
	stmt := statement(map[string]any{"delete": "attributes['payload']", "when": "record.severity_text == 'DEBUG'"})

	require.Empty(t, runEntry(t, &resourcepb.Resource{}, matching, stmt))
	require.Empty(t, runEntry(t, &resourcepb.Resource{}, other, stmt))

	assert.Nil(t, value(matching.GetAttributes(), "payload"))
	assert.NotNil(t, value(other.GetAttributes(), "payload"))
}

func TestOnErrorEndsTheContext(t *testing.T) {
	t.Parallel()
	p, err := compile(t,
		statement(map[string]any{"set": "attributes['a']", "value": "attr(record.attributes, 'missing')"}),
		statement(map[string]any{"set": "attributes['b']", "value": "'never'"}),
	)
	require.NoError(t, err)
	record := &logspb.LogRecord{}

	action := p.Logs().Entry(st.Entry{
		Resource: &resourcepb.Resource{}, Scope: &commonpb.InstrumentationScope{},
		Record: record, Attributes: &record.Attributes,
	}, func(error) st.Action { return st.Drop })

	assert.Equal(t, st.Drop, action)
	assert.Nil(t, value(record.GetAttributes(), "b"), "statements after a drop do not run")
}
