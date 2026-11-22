// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package valuemap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-playground/validator/v10"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/go-tram/tram/internal/adapters/stage/base"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/metrics"
)

const Type = "value_map"

// meterName names the meter the lookup metrics of the stage are created with,
// from the meter provider its factory passes, which serves them on /metrics in
// the Prometheus format like the core's metrics.
const meterName = "github.com/go-tram/tram/internal/adapters/stage/valuemap"

// Outcomes of a lookup, in the order of their metrics. A miss is a defined
// state, not an error: the record receives the default value.
const (
	outcomeMapped = iota
	outcomeMissingAttr
	outcomeNoEntry
	outcomeCount
)

const signalCount = 3

type ValueMap struct {
	*base.Stage[Spec]

	sourceAttr string
	targetAttr string
	def        string
	// level is the level the source attribute is read from, record or
	// resource.
	level   string
	table   map[string]string
	metrics lookupMetrics
	logger  *slog.Logger
}

// New takes the stream the stage belongs to and the provider its lookup
// metrics are created from, both passed on by the factory like the logger.
func New(
	stage domain.StageSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	provider metric.MeterProvider,
	stream string,
) (*ValueMap, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}

	spec := base.Spec()
	table := spec.Table
	if table == nil {
		table, err = readTable(spec.TableRef)
		if err != nil {
			return nil, err
		}
	}

	level := spec.Level
	if level == "" {
		level = LevelRecord
	}

	return &ValueMap{
		Stage:      base,
		sourceAttr: spec.SourceAttr,
		targetAttr: spec.TargetAttr,
		def:        spec.Default,
		level:      level,
		table:      table,
		metrics:    newLookupMetrics(stream, stage.Name, provider),
		logger:     logger.With(slog.String("stage", stage.Name)),
	}, nil
}

// readTable reads the JSON lookup table the spec's table_ref points to. The
// file is read once when the stage is created; a change to it needs a stream
// update with a changed spec.
func readTable(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Join(ErrReadingTableRef, fmt.Errorf("failed to read file: %w", err))
	}
	var table map[string]string
	if err = json.Unmarshal(data, &table); err != nil {
		return nil, errors.Join(ErrReadingTableRef, fmt.Errorf("invalid JSON: %w", err))
	}
	return table, nil
}

// Notify annotates every log record, metric data point and span with the
// result of the lookup and passes the event on as a copy. It does not modify
// the event; it is shared with sibling branches.
func (s *ValueMap) Notify(ctx context.Context, event *domain.Event) error {
	var counts lookupCounts
	mapped := *event

	if event.Logs != nil {
		mapped.Logs = s.mapLogs(&counts.logs, event.Logs)
	}
	if event.Metrics != nil {
		mapped.Metrics = s.mapMetrics(&counts.metrics, event.Metrics)
	}
	if event.Traces != nil {
		mapped.Traces = s.mapTraces(&counts.traces, event.Traces)
	}

	s.metrics.record(ctx, &counts)
	return s.NotifySubscribers(ctx, &mapped)
}

func (s *ValueMap) mapLogs(counts *signalCounts, data *logspb.LogsData) *logspb.LogsData {
	resources := make([]*logspb.ResourceLogs, len(data.GetResourceLogs()))
	for i, rl := range data.GetResourceLogs() {
		scopes := make([]*logspb.ScopeLogs, len(rl.GetScopeLogs()))
		for j, sl := range rl.GetScopeLogs() {
			records := make([]*logspb.LogRecord, len(sl.GetLogRecords()))
			for k, record := range sl.GetLogRecords() {
				records[k] = &logspb.LogRecord{
					TimeUnixNano:           record.GetTimeUnixNano(),
					ObservedTimeUnixNano:   record.GetObservedTimeUnixNano(),
					SeverityNumber:         record.GetSeverityNumber(),
					SeverityText:           record.GetSeverityText(),
					Body:                   record.GetBody(),
					Attributes:             s.setRecordAttr(counts, record.GetAttributes(), rl.GetResource()),
					DroppedAttributesCount: record.GetDroppedAttributesCount(),
					Flags:                  record.GetFlags(),
					TraceId:                record.GetTraceId(),
					SpanId:                 record.GetSpanId(),
					EventName:              record.GetEventName(),
				}
			}
			scopes[j] = &logspb.ScopeLogs{
				Scope:      sl.GetScope(),
				SchemaUrl:  sl.GetSchemaUrl(),
				LogRecords: records,
			}
		}
		resources[i] = &logspb.ResourceLogs{
			Resource:  rl.GetResource(),
			SchemaUrl: rl.GetSchemaUrl(),
			ScopeLogs: scopes,
		}
	}
	return &logspb.LogsData{ResourceLogs: resources}
}

func (s *ValueMap) mapMetrics(counts *signalCounts, data *metricspb.MetricsData) *metricspb.MetricsData {
	resources := make([]*metricspb.ResourceMetrics, len(data.GetResourceMetrics()))
	for i, rm := range data.GetResourceMetrics() {
		scopes := make([]*metricspb.ScopeMetrics, len(rm.GetScopeMetrics()))
		for j, sm := range rm.GetScopeMetrics() {
			metrics := make([]*metricspb.Metric, len(sm.GetMetrics()))
			for k, m := range sm.GetMetrics() {
				metrics[k] = s.mapMetric(counts, m, rm.GetResource())
			}
			scopes[j] = &metricspb.ScopeMetrics{
				Scope:     sm.GetScope(),
				SchemaUrl: sm.GetSchemaUrl(),
				Metrics:   metrics,
			}
		}
		resources[i] = &metricspb.ResourceMetrics{
			Resource:     rm.GetResource(),
			SchemaUrl:    rm.GetSchemaUrl(),
			ScopeMetrics: scopes,
		}
	}
	return &metricspb.MetricsData{ResourceMetrics: resources}
}

func (s *ValueMap) mapMetric(
	counts *signalCounts,
	m *metricspb.Metric,
	resource *resourcepb.Resource,
) *metricspb.Metric {
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		dps := s.mapNumberDataPoints(counts, data.Gauge.GetDataPoints(), resource)
		out := withoutData(m)
		out.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}}
		return out
	case *metricspb.Metric_Sum:
		dps := s.mapNumberDataPoints(counts, data.Sum.GetDataPoints(), resource)
		out := withoutData(m)
		out.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			DataPoints:             dps,
			AggregationTemporality: data.Sum.GetAggregationTemporality(),
			IsMonotonic:            data.Sum.GetIsMonotonic(),
		}}
		return out
	case *metricspb.Metric_Histogram:
		dps := s.mapHistogramDataPoints(counts, data.Histogram.GetDataPoints(), resource)
		out := withoutData(m)
		out.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints:             dps,
			AggregationTemporality: data.Histogram.GetAggregationTemporality(),
		}}
		return out
	case *metricspb.Metric_ExponentialHistogram:
		dps := s.mapExponentialHistogramDataPoints(counts, data.ExponentialHistogram.GetDataPoints(), resource)
		out := withoutData(m)
		out.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints:             dps,
			AggregationTemporality: data.ExponentialHistogram.GetAggregationTemporality(),
		}}
		return out
	case *metricspb.Metric_Summary:
		dps := s.mapSummaryDataPoints(counts, data.Summary.GetDataPoints(), resource)
		out := withoutData(m)
		out.Data = &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: dps}}
		return out
	}
	return m
}

// withoutData returns a copy of m without its data points, keeping its name,
// description, unit and metadata.
func withoutData(m *metricspb.Metric) *metricspb.Metric {
	return &metricspb.Metric{
		Name:        m.GetName(),
		Description: m.GetDescription(),
		Unit:        m.GetUnit(),
		Metadata:    m.GetMetadata(),
	}
}

func (s *ValueMap) mapNumberDataPoints(
	counts *signalCounts,
	dataPoints []*metricspb.NumberDataPoint,
	resource *resourcepb.Resource,
) []*metricspb.NumberDataPoint {
	out := make([]*metricspb.NumberDataPoint, len(dataPoints))
	for i, dp := range dataPoints {
		out[i] = &metricspb.NumberDataPoint{
			Attributes:        s.setRecordAttr(counts, dp.GetAttributes(), resource),
			StartTimeUnixNano: dp.GetStartTimeUnixNano(),
			TimeUnixNano:      dp.GetTimeUnixNano(),
			Value:             dp.GetValue(),
			Exemplars:         dp.GetExemplars(),
			Flags:             dp.GetFlags(),
		}
	}
	return out
}

func (s *ValueMap) mapHistogramDataPoints(
	counts *signalCounts,
	dataPoints []*metricspb.HistogramDataPoint,
	resource *resourcepb.Resource,
) []*metricspb.HistogramDataPoint {
	out := make([]*metricspb.HistogramDataPoint, len(dataPoints))
	for i, dp := range dataPoints {
		out[i] = &metricspb.HistogramDataPoint{
			Attributes:        s.setRecordAttr(counts, dp.GetAttributes(), resource),
			StartTimeUnixNano: dp.GetStartTimeUnixNano(),
			TimeUnixNano:      dp.GetTimeUnixNano(),
			Count:             dp.GetCount(),
			Sum:               dp.Sum,
			BucketCounts:      dp.GetBucketCounts(),
			ExplicitBounds:    dp.GetExplicitBounds(),
			Exemplars:         dp.GetExemplars(),
			Flags:             dp.GetFlags(),
			Min:               dp.Min,
			Max:               dp.Max,
		}
	}
	return out
}

func (s *ValueMap) mapExponentialHistogramDataPoints(
	counts *signalCounts,
	dataPoints []*metricspb.ExponentialHistogramDataPoint,
	resource *resourcepb.Resource,
) []*metricspb.ExponentialHistogramDataPoint {
	out := make([]*metricspb.ExponentialHistogramDataPoint, len(dataPoints))
	for i, dp := range dataPoints {
		out[i] = &metricspb.ExponentialHistogramDataPoint{
			Attributes:        s.setRecordAttr(counts, dp.GetAttributes(), resource),
			StartTimeUnixNano: dp.GetStartTimeUnixNano(),
			TimeUnixNano:      dp.GetTimeUnixNano(),
			Count:             dp.GetCount(),
			Sum:               dp.Sum,
			Scale:             dp.GetScale(),
			ZeroCount:         dp.GetZeroCount(),
			Positive:          dp.GetPositive(),
			Negative:          dp.GetNegative(),
			Flags:             dp.GetFlags(),
			Exemplars:         dp.GetExemplars(),
			Min:               dp.Min,
			Max:               dp.Max,
			ZeroThreshold:     dp.GetZeroThreshold(),
		}
	}
	return out
}

func (s *ValueMap) mapSummaryDataPoints(
	counts *signalCounts,
	dataPoints []*metricspb.SummaryDataPoint,
	resource *resourcepb.Resource,
) []*metricspb.SummaryDataPoint {
	out := make([]*metricspb.SummaryDataPoint, len(dataPoints))
	for i, dp := range dataPoints {
		out[i] = &metricspb.SummaryDataPoint{
			Attributes:        s.setRecordAttr(counts, dp.GetAttributes(), resource),
			StartTimeUnixNano: dp.GetStartTimeUnixNano(),
			TimeUnixNano:      dp.GetTimeUnixNano(),
			Count:             dp.GetCount(),
			Sum:               dp.GetSum(),
			QuantileValues:    dp.GetQuantileValues(),
			Flags:             dp.GetFlags(),
		}
	}
	return out
}

func (s *ValueMap) mapTraces(counts *signalCounts, data *tracepb.TracesData) *tracepb.TracesData {
	resources := make([]*tracepb.ResourceSpans, len(data.GetResourceSpans()))
	for i, rs := range data.GetResourceSpans() {
		scopes := make([]*tracepb.ScopeSpans, len(rs.GetScopeSpans()))
		for j, ss := range rs.GetScopeSpans() {
			spans := make([]*tracepb.Span, len(ss.GetSpans()))
			for k, span := range ss.GetSpans() {
				spans[k] = &tracepb.Span{
					TraceId:                span.GetTraceId(),
					SpanId:                 span.GetSpanId(),
					TraceState:             span.GetTraceState(),
					ParentSpanId:           span.GetParentSpanId(),
					Flags:                  span.GetFlags(),
					Name:                   span.GetName(),
					Kind:                   span.GetKind(),
					StartTimeUnixNano:      span.GetStartTimeUnixNano(),
					EndTimeUnixNano:        span.GetEndTimeUnixNano(),
					Attributes:             s.setRecordAttr(counts, span.GetAttributes(), rs.GetResource()),
					DroppedAttributesCount: span.GetDroppedAttributesCount(),
					Events:                 span.GetEvents(),
					DroppedEventsCount:     span.GetDroppedEventsCount(),
					Links:                  span.GetLinks(),
					DroppedLinksCount:      span.GetDroppedLinksCount(),
					Status:                 span.GetStatus(),
				}
			}
			scopes[j] = &tracepb.ScopeSpans{
				Scope:     ss.GetScope(),
				SchemaUrl: ss.GetSchemaUrl(),
				Spans:     spans,
			}
		}
		resources[i] = &tracepb.ResourceSpans{
			Resource:   rs.GetResource(),
			SchemaUrl:  rs.GetSchemaUrl(),
			ScopeSpans: scopes,
		}
	}
	return &tracepb.TracesData{ResourceSpans: resources}
}

// setRecordAttr looks up the source attribute, counts the outcome and returns
// a copy of attrs with the target attribute set to the result: the value the
// table maps the source value to, or the default on a miss. The level decides
// where the source attribute is read from: from the resource with level
// resource, from attrs, the record's own attributes, with level record.
func (s *ValueMap) setRecordAttr(
	counts *signalCounts,
	attrs []*commonpb.KeyValue,
	resource *resourcepb.Resource,
) []*commonpb.KeyValue {
	source := attrs
	if s.level == LevelResource {
		source = resource.GetAttributes()
	}

	value, outcome := s.lookup(source)
	counts.add(outcome)
	return setAttr(attrs, s.targetAttr, value)
}

// lookup returns the value the table maps the source attribute's value to, or
// the default when the attribute is missing or the value has no entry. A
// source attribute with a non-string value cannot have a table entry, so it
// counts as no_entry as well.
func (s *ValueMap) lookup(attrs []*commonpb.KeyValue) (string, int) {
	for _, attr := range attrs {
		if attr.GetKey() != s.sourceAttr {
			continue
		}
		value, isString := attr.GetValue().GetValue().(*commonpb.AnyValue_StringValue)
		if !isString {
			return s.def, outcomeNoEntry
		}
		mapped, inTable := s.table[value.StringValue]
		if !inTable {
			return s.def, outcomeNoEntry
		}
		return mapped, outcomeMapped
	}
	return s.def, outcomeMissingAttr
}

// setAttr returns a copy of attrs with the attribute key set to a string
// value, replacing an existing attribute or appending a new one. The
// attributes of the input are not modified.
func setAttr(attrs []*commonpb.KeyValue, key, value string) []*commonpb.KeyValue {
	out := make([]*commonpb.KeyValue, len(attrs), len(attrs)+1)
	copy(out, attrs)
	for i, attr := range out {
		if attr.GetKey() == key {
			out[i] = stringAttr(key, value)
			return out
		}
	}
	return append(out, stringAttr(key, value))
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}

// signalCounts accumulates the outcomes of one event per signal, so the
// metrics record once per event instead of once per record.
type signalCounts struct {
	mapped, missingAttr, noEntry int64
}

func (c *signalCounts) add(outcome int) {
	switch outcome {
	case outcomeMapped:
		c.mapped++
	case outcomeMissingAttr:
		c.missingAttr++
	case outcomeNoEntry:
		c.noEntry++
	}
}

type lookupCounts struct {
	logs, metrics, traces signalCounts
}

// lookupMetrics holds the lookup outcome counters of a stage. The counters are
// nil when they cannot be created, e.g. with a meter provider that refuses
// them, and the zero metrics then records nothing.
type lookupMetrics struct {
	counters [outcomeCount]metric.Int64Counter
	// options holds the add options per outcome and signal: stream, node and
	// signal.
	options [outcomeCount][signalCount][]metric.AddOption
}

func newLookupMetrics(stream, node string, provider metric.MeterProvider) lookupMetrics {
	lookupDescriptions := [outcomeCount]string{
		"Records the stage annotated with a value its table maps the source attribute to",
		"Records whose source attribute is missing and that received the default value",
		"Records whose source attribute has no entry in the table and that received the default value",
	}
	lookupNames := [outcomeCount]string{"mapped", "lookup_missing_attr", "lookup_no_entry"}

	meter := provider.Meter(meterName)
	var m lookupMetrics
	for outcome := range outcomeCount {
		counter, err := meter.Int64Counter(
			metrics.Name("stage", Type, lookupNames[outcome]),
			metric.WithUnit("{record}"),
			metric.WithDescription(lookupDescriptions[outcome]),
		)
		if err != nil {
			return lookupMetrics{}
		}
		m.counters[outcome] = counter
		for signal, sig := range [signalCount]domain.Signal{domain.SignalLogs, domain.SignalMetrics, domain.SignalTraces} {
			m.options[outcome][signal] = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(
				attribute.String(metrics.LabelStream, stream),
				attribute.String(metrics.LabelNode, node),
				attribute.String(metrics.LabelSignal, string(sig)),
			))}
		}
	}
	return m
}

func (m lookupMetrics) record(ctx context.Context, counts *lookupCounts) {
	if m.counters[outcomeMapped] == nil {
		return
	}
	for signal, counted := range [outcomeCount]signalCounts{counts.logs, counts.metrics, counts.traces} {
		m.add(ctx, outcomeMapped, signal, counted.mapped)
		m.add(ctx, outcomeMissingAttr, signal, counted.missingAttr)
		m.add(ctx, outcomeNoEntry, signal, counted.noEntry)
	}
}

func (m lookupMetrics) add(ctx context.Context, outcome, signal int, records int64) {
	if records > 0 {
		m.counters[outcome].Add(ctx, records, m.options[outcome][signal]...)
	}
}
