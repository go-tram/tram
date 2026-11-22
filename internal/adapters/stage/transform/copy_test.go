// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package transform_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/go-tram/tram/internal/adapters/stage/transform"
)

// fill sets every field of m, the first field of each oneof, to a value
// other than its default, so a copy that misses a field differs.
func fill(m protoreflect.Message) {
	fields := m.Descriptor().Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if oneof := fd.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() && oneof.Fields().Get(0) != fd {
			continue
		}
		switch {
		case fd.IsList():
			list := m.Mutable(fd).List()
			list.Append(element(list.NewElement(), fd))
		case fd.IsMap():
			continue
		case fd.Kind() == protoreflect.MessageKind:
			fill(m.Mutable(fd).Message())
		default:
			m.Set(fd, scalar(fd))
		}
	}
}

func element(v protoreflect.Value, fd protoreflect.FieldDescriptor) protoreflect.Value {
	if fd.Kind() == protoreflect.MessageKind {
		fill(v.Message())
		return v
	}
	return scalar(fd)
}

func scalar(fd protoreflect.FieldDescriptor) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(1)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(7)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(7)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(7)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(7)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(7)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(7)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("x")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{7})
	case protoreflect.MessageKind, protoreflect.GroupKind:
		panic("messages are filled, not set")
	default:
		panic("unexpected kind " + fd.Kind().String())
	}
}

func filled[M proto.Message](m M) M {
	fill(m.ProtoReflect())
	return m
}

// TestCopiesKeepEveryField fails when OTLP gains a field the copies miss,
// e.g. after updating go.opentelemetry.io/proto/otlp; add it to copy.go.
func TestCopiesKeepEveryField(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   proto.Message
		copy func(proto.Message) proto.Message
	}{
		{"Resource", filled(&resourcepb.Resource{}),
			func(m proto.Message) proto.Message { return transform.CopyResource(m.(*resourcepb.Resource)) }},
		{"InstrumentationScope", filled(&commonpb.InstrumentationScope{}),
			func(m proto.Message) proto.Message { return transform.CopyScope(m.(*commonpb.InstrumentationScope)) }},
		{"LogRecord", filled(&logspb.LogRecord{}),
			func(m proto.Message) proto.Message { return transform.CopyLogRecord(m.(*logspb.LogRecord)) }},
		{"Span", filled(&tracepb.Span{}),
			func(m proto.Message) proto.Message { return transform.CopySpan(m.(*tracepb.Span)) }},
		{"NumberDataPoint", filled(&metricspb.NumberDataPoint{}),
			func(m proto.Message) proto.Message {
				return transform.CopyNumberDataPoint(m.(*metricspb.NumberDataPoint))
			}},
		{"HistogramDataPoint", filled(&metricspb.HistogramDataPoint{}),
			func(m proto.Message) proto.Message {
				return transform.CopyHistogramDataPoint(m.(*metricspb.HistogramDataPoint))
			}},
		{"ExponentialHistogramDataPoint", filled(&metricspb.ExponentialHistogramDataPoint{}),
			func(m proto.Message) proto.Message {
				return transform.CopyExponentialHistogramDataPoint(m.(*metricspb.ExponentialHistogramDataPoint))
			}},
		{"SummaryDataPoint", filled(&metricspb.SummaryDataPoint{}),
			func(m proto.Message) proto.Message {
				return transform.CopySummaryDataPoint(m.(*metricspb.SummaryDataPoint))
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.True(t, proto.Equal(tc.in, tc.copy(tc.in)), "%s: a field is not copied", tc.name)
		})
	}

	// Metrics are copied without their data, which the stage rebuilds.
	m := filled(&metricspb.Metric{})
	c := transform.CopyMetric(m)
	c.Data = m.GetData()
	assert.True(t, proto.Equal(m, c), "Metric: a field is not copied")
}
