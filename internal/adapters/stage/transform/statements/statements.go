// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package statements is the contract between the transform stage and its
// drivers, e.g. cel, and holds what every driver shares: the target paths of
// statements and the operations on attributes.
//
// The stage copies an event's data, walks it and hands each resource, scope
// and entry (log record, metric data point or span) to the driver's program,
// which changes their attributes in place. When a statement fails, the
// program asks the stage what to do, and the stage drops entries or rejects
// the event as its spec says.
package statements

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Driver compiles the options under its name into a program.
type Driver interface {
	Compile(opts map[string]any) (Program, error)
}

// Program holds the compiled statements per signal. It is safe for
// concurrent use.
type Program interface {
	// Logs, Metrics and Traces return the statements of a signal, nil for a
	// signal without statements, whose data the stage passes on unchanged.
	Logs() SignalProgram
	Metrics() SignalProgram
	Traces() SignalProgram
}

// SignalProgram applies the statements of one signal. Each method runs the
// statements of its context in order and changes the attributes it is given.
// It returns Continue, or the action the stage's OnError returned for a
// failed statement other than Continue, which ends the statements of that
// context.
type SignalProgram interface {
	// Resource runs the statements on resource.attributes, once per resource.
	Resource(resource *resourcepb.Resource, onError OnError) Action
	// Scope runs the statements on scope.attributes, once per scope.
	Scope(resource *resourcepb.Resource, scope *commonpb.InstrumentationScope, onError OnError) Action
	// Entry runs the statements on attributes, once per log record, metric
	// data point or span.
	Entry(entry Entry, onError OnError) Action
}

// Entry is a log record, metric data point or span with what statements may
// read about it.
type Entry struct {
	Resource *resourcepb.Resource
	Scope    *commonpb.InstrumentationScope
	// Record is set for logs; Metric and DataPoint for metrics; Span for
	// traces.
	Record    *logspb.LogRecord
	Metric    *metricspb.Metric
	DataPoint proto.Message
	Span      *tracepb.Span
	// Attributes are the entry's own attributes, which statements change.
	Attributes *[]*commonpb.KeyValue
}

// Action is what happens after a statement failed on a resource, scope or
// entry.
type Action int

const (
	// Continue skips the failed statement and runs the next one.
	Continue Action = iota
	// Drop removes the entry, or the resource or scope with all its entries.
	Drop
	// Reject rejects the whole event.
	Reject
)

// OnError is called for every failed statement and decides what happens.
type OnError func(err error) Action
