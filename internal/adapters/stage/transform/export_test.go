// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package transform

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// The shallow copies, for TestCopiesKeepEveryField.

func CopyResource(m *resourcepb.Resource) *resourcepb.Resource { return copyResource(m) }

func CopyScope(m *commonpb.InstrumentationScope) *commonpb.InstrumentationScope { return copyScope(m) }

func CopyLogRecord(m *logspb.LogRecord) *logspb.LogRecord { return copyLogRecord(m) }

func CopySpan(m *tracepb.Span) *tracepb.Span { return copySpan(m) }

func CopyMetric(m *metricspb.Metric) *metricspb.Metric { return copyMetric(m) }

func CopyNumberDataPoint(m *metricspb.NumberDataPoint) *metricspb.NumberDataPoint {
	return copyNumberDataPoint(m)
}

func CopyHistogramDataPoint(m *metricspb.HistogramDataPoint) *metricspb.HistogramDataPoint {
	return copyHistogramDataPoint(m)
}

func CopyExponentialHistogramDataPoint(
	m *metricspb.ExponentialHistogramDataPoint,
) *metricspb.ExponentialHistogramDataPoint {
	return copyExponentialHistogramDataPoint(m)
}

func CopySummaryDataPoint(m *metricspb.SummaryDataPoint) *metricspb.SummaryDataPoint {
	return copySummaryDataPoint(m)
}
