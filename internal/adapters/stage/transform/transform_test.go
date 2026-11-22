// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package transform_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/stage/transform"
	"github.com/go-tram/tram/internal/core/domain"
)

type capture struct {
	mu     sync.Mutex
	events []*domain.Event
}

func (c *capture) Name() string { return "capture" }
func (c *capture) From() string { return "transform" }
func (c *capture) Stop() error  { return nil }
func (c *capture) Notify(_ context.Context, e *domain.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	return nil
}

func newStage(t *testing.T, spec map[string]any) (*transform.Transform, *capture) {
	t.Helper()
	s, err := transform.New(domain.StageSpec{Name: "normalize", Type: transform.Type, From: "in", Spec: spec},
		validator.New(), slog.New(slog.DiscardHandler), noop.NewMeterProvider(), "stream-1")
	require.NoError(t, err)
	c := &capture{}
	require.NoError(t, s.Subscribe(c))
	return s, c
}

// celSpec is a transform spec with the cel driver and the given statements.
func celSpec(onError string, signal string, stmts ...map[string]any) map[string]any {
	list := make([]any, len(stmts))
	for i, s := range stmts {
		list[i] = s
	}
	spec := map[string]any{"driver": "cel", "cel": map[string]any{signal: list}}
	if onError != "" {
		spec["on_error"] = onError
	}
	return spec
}

func str(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}

func kv(key, value string) *commonpb.KeyValue { return &commonpb.KeyValue{Key: key, Value: str(value)} }

func attrs(list []*commonpb.KeyValue) map[string]string {
	out := map[string]string{}
	for _, a := range list {
		out[a.GetKey()] = a.GetValue().GetStringValue()
	}
	return out
}

// twoRecords is one resource with a scope and two log records.
func twoRecords() *domain.Event {
	return &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{kv("k8s.namespace.name", "shop-prod")}},
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: "app"},
			LogRecords: []*logspb.LogRecord{
				{SeverityText: "INFO", Attributes: []*commonpb.KeyValue{kv("user", "ann"), kv("password", "s3cret")}},
				{
					SeverityText: "DEBUG",
					Attributes:   []*commonpb.KeyValue{kv("user", "bob"), kv("note", "a very long note")},
				},
			},
		}},
	}}}}
}

func TestTransformsLogs(t *testing.T) {
	t.Parallel()
	s, c := newStage(t, celSpec(
		"",
		"logs",
		map[string]any{
			"set":   "resource.attributes['env']",
			"value": "attr(resource.attributes, 'k8s.namespace.name').split('-')[1]",
		},
		map[string]any{"set": "scope.attributes['team']", "value": "'checkout'"},
		map[string]any{"delete": "attributes['password']"},
		map[string]any{"rename": "attributes['user']", "to": "user.name"},
		map[string]any{"truncate": "attributes['note']", "length": float64(6)},
		map[string]any{"set": "attributes['debug']", "value": "true", "when": "record.severity_text == 'DEBUG'"},
	))
	in := twoRecords()
	original := proto.Clone(in.Logs)

	require.NoError(t, s.Notify(t.Context(), in))

	require.Len(t, c.events, 1)
	rl := c.events[0].Logs.GetResourceLogs()[0]
	assert.Equal(
		t,
		map[string]string{"k8s.namespace.name": "shop-prod", "env": "prod"},
		attrs(rl.GetResource().GetAttributes()),
	)
	assert.Equal(t, map[string]string{"team": "checkout"}, attrs(rl.GetScopeLogs()[0].GetScope().GetAttributes()))
	records := rl.GetScopeLogs()[0].GetLogRecords()
	assert.Equal(t, map[string]string{"user.name": "ann"}, attrs(records[0].GetAttributes()))
	assert.Equal(
		t,
		map[string]string{"user.name": "bob", "note": "a very", "debug": ""},
		attrs(records[1].GetAttributes()),
	)
	assert.True(t, records[1].GetAttributes()[2].GetValue().GetBoolValue())
	assert.True(
		t,
		proto.Equal(original, in.Logs),
		"the input event is shared with sibling branches and stays as it was",
	)
}

func TestResourceStatementsRunOncePerResource(t *testing.T) {
	t.Parallel()
	// Appending to a list would show every extra run.
	s, c := newStage(t, celSpec(
		"",
		"logs",
		map[string]any{
			"set":   "resource.attributes['runs']",
			"value": "string(int(attr(resource.attributes, 'runs')) + 1)",
			"when":  "attr(resource.attributes, 'runs') != null",
		},
		map[string]any{
			"set":   "resource.attributes['runs']",
			"value": "'1'",
			"when":  "attr(resource.attributes, 'runs') == null",
		},
	))

	require.NoError(t, s.Notify(t.Context(), twoRecords()))

	assert.Equal(t, "1", attrs(c.events[0].Logs.GetResourceLogs()[0].GetResource().GetAttributes())["runs"],
		"two records, but the resource statements ran once")
}

func TestTransformsMetricDataPointsAndSpans(t *testing.T) {
	t.Parallel()
	metricsStage, metricsOut := newStage(t, celSpec("", "metrics",
		map[string]any{"set": "attributes['unit']", "value": "metric.unit"},
		map[string]any{"rename": "attributes['http.method']", "to": "http.request.method"},
	))
	event := &domain.Event{Metrics: &metricspb.MetricsData{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{
			{Name: "requests", Unit: "{request}", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				DataPoints: []*metricspb.NumberDataPoint{{Attributes: []*commonpb.KeyValue{kv("http.method", "GET")}}},
			}}},
			{Name: "latency", Unit: "s", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				DataPoints: []*metricspb.HistogramDataPoint{
					{Attributes: []*commonpb.KeyValue{kv("http.method", "POST")}},
				},
			}}},
		}}},
	}}}}
	require.NoError(t, metricsStage.Notify(t.Context(), event))
	metrics := metricsOut.events[0].Metrics.GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()
	assert.Equal(t, map[string]string{"unit": "{request}", "http.request.method": "GET"},
		attrs(metrics[0].GetSum().GetDataPoints()[0].GetAttributes()))
	assert.Equal(t, map[string]string{"unit": "s", "http.request.method": "POST"},
		attrs(metrics[1].GetHistogram().GetDataPoints()[0].GetAttributes()))
	assert.Nil(t, metricsOut.events[0].Metrics.GetResourceMetrics()[0].GetResource(),
		"a missing resource stays missing when no statement set anything on it")

	tracesStage, tracesOut := newStage(t, celSpec("", "traces",
		map[string]any{"set": "attributes['route']", "value": "span.name"},
	))
	spans := &domain.Event{Traces: &tracepb.TracesData{ResourceSpans: []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{Name: "GET /cart"}}}},
	}}}}
	require.NoError(t, tracesStage.Notify(t.Context(), spans))
	assert.Equal(t, map[string]string{"route": "GET /cart"},
		attrs(tracesOut.events[0].Traces.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0].GetAttributes()))
}

func TestSignalsWithoutStatementsPassUnchanged(t *testing.T) {
	t.Parallel()
	s, c := newStage(t, celSpec("", "traces", map[string]any{"delete": "attributes['x']"}))
	in := twoRecords()

	require.NoError(t, s.Notify(t.Context(), in))

	assert.Same(t, in.Logs, c.events[0].Logs, "logs have no statements, so they are not copied")
}

func TestOnError(t *testing.T) {
	t.Parallel()
	// Fails on the record without a team attribute, ann's.
	stmt := map[string]any{"set": "attributes['team.id']", "value": "attr(record.attributes, 'team')"}
	event := func() *domain.Event {
		e := twoRecords()
		records := e.Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()
		records[1].Attributes = append(records[1].Attributes, kv("team", "cart"))
		return e
	}

	t.Run("skip keeps the entry without the failed statement", func(t *testing.T) {
		t.Parallel()
		s, c := newStage(t, celSpec("", "logs", stmt))
		require.NoError(t, s.Notify(t.Context(), event()))
		records := c.events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()
		require.Len(t, records, 2)
		assert.NotContains(t, attrs(records[0].GetAttributes()), "team.id")
		assert.Equal(t, "cart", attrs(records[1].GetAttributes())["team.id"])
	})
	t.Run("drop removes the entry", func(t *testing.T) {
		t.Parallel()
		s, c := newStage(t, celSpec(transform.OnErrorDrop, "logs", stmt))
		require.NoError(t, s.Notify(t.Context(), event()))
		records := c.events[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()
		require.Len(t, records, 1)
		assert.Equal(t, "bob", attrs(records[0].GetAttributes())["user"])
	})
	t.Run("drop of every entry drops the event", func(t *testing.T) {
		t.Parallel()
		s, c := newStage(t, celSpec(transform.OnErrorDrop, "logs",
			map[string]any{"set": "attributes['x']", "value": "attr(record.attributes, 'nope')"}))
		require.NoError(t, s.Notify(t.Context(), event()))
		assert.Empty(t, c.events)
	})
	t.Run("reject rejects the event", func(t *testing.T) {
		t.Parallel()
		s, c := newStage(t, celSpec(transform.OnErrorReject, "logs", stmt))
		err := s.Notify(t.Context(), event())
		require.ErrorIs(t, err, domain.ErrEventRejected)
		assert.Contains(t, err.Error(), `attributes["team.id"]`)
		assert.Empty(t, c.events)
	})
}

func TestSpecIsValidated(t *testing.T) {
	t.Parallel()
	stmt := map[string]any{"delete": "attributes['x']"}
	for name, spec := range map[string]map[string]any{
		"unknown on_error": celSpec("ignore", "logs", stmt),
		"unknown driver":   {"driver": "ottl", "ottl": map[string]any{}},
		"missing driver":   {"cel": map[string]any{"logs": []any{stmt}}},
		"no statements":    {"driver": "cel", "cel": map[string]any{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := transform.New(domain.StageSpec{Name: "t", Type: transform.Type, Spec: spec},
				validator.New(), slog.New(slog.DiscardHandler), noop.NewMeterProvider(), "s")
			assert.Error(t, err)
		})
	}
}

func TestFailuresAreCounted(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	s, err := transform.New(domain.StageSpec{Name: "normalize", Type: transform.Type, Spec: celSpec("", "logs",
		map[string]any{"set": "attributes['x']", "value": "attr(record.attributes, 'nope')"})},
		validator.New(), slog.New(slog.DiscardHandler), provider, "stream-1")
	require.NoError(t, err)
	require.NoError(t, s.Subscribe(&capture{}))

	require.NoError(t, s.Notify(t.Context(), twoRecords()))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	m := rm.ScopeMetrics[0].Metrics[0]
	assert.Equal(t, "tram.stage.transform.failed", m.Name)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	assert.Equal(t, int64(2), sum.DataPoints[0].Value, "both records failed the statement")
	stream, _ := sum.DataPoints[0].Attributes.Value("stream")
	signal, _ := sum.DataPoints[0].Attributes.Value("signal")
	assert.Equal(t, "stream-1", stream.AsString())
	assert.Equal(t, "logs", signal.AsString())
}
