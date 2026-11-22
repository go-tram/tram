// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package transform

import (
	"slices"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// The copies are shallow: they share every field with the original but get
// a slice of attributes of their own. The operations on attributes replace
// key-value pairs instead of changing them, so the original, which sibling
// branches share, stays as it is. A deep copy would cost about ten times as
// much. TestCopiesKeepEveryField guards that a field added to OTLP is copied.

func copyResource(r *resourcepb.Resource) *resourcepb.Resource {
	if r == nil {
		return nil
	}
	return &resourcepb.Resource{
		Attributes:             slices.Clone(r.GetAttributes()),
		DroppedAttributesCount: r.GetDroppedAttributesCount(),
		EntityRefs:             r.GetEntityRefs(),
	}
}

func copyScope(s *commonpb.InstrumentationScope) *commonpb.InstrumentationScope {
	if s == nil {
		return nil
	}
	return &commonpb.InstrumentationScope{
		Name:                   s.GetName(),
		Version:                s.GetVersion(),
		Attributes:             slices.Clone(s.GetAttributes()),
		DroppedAttributesCount: s.GetDroppedAttributesCount(),
	}
}

func copyLogRecord(r *logspb.LogRecord) *logspb.LogRecord {
	return &logspb.LogRecord{
		TimeUnixNano:           r.GetTimeUnixNano(),
		ObservedTimeUnixNano:   r.GetObservedTimeUnixNano(),
		SeverityNumber:         r.GetSeverityNumber(),
		SeverityText:           r.GetSeverityText(),
		Body:                   r.GetBody(),
		Attributes:             slices.Clone(r.GetAttributes()),
		DroppedAttributesCount: r.GetDroppedAttributesCount(),
		Flags:                  r.GetFlags(),
		TraceId:                r.GetTraceId(),
		SpanId:                 r.GetSpanId(),
		EventName:              r.GetEventName(),
	}
}

func copySpan(s *tracepb.Span) *tracepb.Span {
	return &tracepb.Span{
		TraceId:                s.GetTraceId(),
		SpanId:                 s.GetSpanId(),
		TraceState:             s.GetTraceState(),
		ParentSpanId:           s.GetParentSpanId(),
		Flags:                  s.GetFlags(),
		Name:                   s.GetName(),
		Kind:                   s.GetKind(),
		StartTimeUnixNano:      s.GetStartTimeUnixNano(),
		EndTimeUnixNano:        s.GetEndTimeUnixNano(),
		Attributes:             slices.Clone(s.GetAttributes()),
		DroppedAttributesCount: s.GetDroppedAttributesCount(),
		Events:                 s.GetEvents(),
		DroppedEventsCount:     s.GetDroppedEventsCount(),
		Links:                  s.GetLinks(),
		DroppedLinksCount:      s.GetDroppedLinksCount(),
		Status:                 s.GetStatus(),
	}
}

// copyMetric copies m without its data, which the caller sets.
func copyMetric(m *metricspb.Metric) *metricspb.Metric {
	return &metricspb.Metric{
		Name:        m.GetName(),
		Description: m.GetDescription(),
		Unit:        m.GetUnit(),
		Metadata:    m.GetMetadata(),
	}
}

func copyNumberDataPoint(dp *metricspb.NumberDataPoint) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		Attributes:        slices.Clone(dp.GetAttributes()),
		StartTimeUnixNano: dp.GetStartTimeUnixNano(),
		TimeUnixNano:      dp.GetTimeUnixNano(),
		Value:             dp.GetValue(),
		Exemplars:         dp.GetExemplars(),
		Flags:             dp.GetFlags(),
	}
}

func copyHistogramDataPoint(dp *metricspb.HistogramDataPoint) *metricspb.HistogramDataPoint {
	return &metricspb.HistogramDataPoint{
		Attributes:        slices.Clone(dp.GetAttributes()),
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

func copyExponentialHistogramDataPoint(
	dp *metricspb.ExponentialHistogramDataPoint,
) *metricspb.ExponentialHistogramDataPoint {
	return &metricspb.ExponentialHistogramDataPoint{
		Attributes:        slices.Clone(dp.GetAttributes()),
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

func copySummaryDataPoint(dp *metricspb.SummaryDataPoint) *metricspb.SummaryDataPoint {
	return &metricspb.SummaryDataPoint{
		Attributes:        slices.Clone(dp.GetAttributes()),
		StartTimeUnixNano: dp.GetStartTimeUnixNano(),
		TimeUnixNano:      dp.GetTimeUnixNano(),
		Count:             dp.GetCount(),
		Sum:               dp.GetSum(),
		QuantileValues:    dp.GetQuantileValues(),
		Flags:             dp.GetFlags(),
	}
}
