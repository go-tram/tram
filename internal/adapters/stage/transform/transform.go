// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package transform is the transform stage: it changes the attributes of log
// records, metric data points and spans, and of their scopes and resources,
// with the statements of its driver, e.g. cel. The stage copies what it
// changes, since an event is shared with sibling branches, runs the
// statements once per resource, scope and entry, and handles failed
// statements as `on_error` says. The contract with the drivers is in package
// statements; only package drivers knows them.
package transform

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-playground/validator/v10"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/base"
	"github.com/go-tram/tram/internal/adapters/stage/transform/drivers"
	st "github.com/go-tram/tram/internal/adapters/stage/transform/statements"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/metrics"
)

const (
	Type      = "transform"
	meterName = "github.com/go-tram/tram/internal/adapters/stage/transform"

	// OnErrorSkip skips a failed statement and keeps the entry; the default.
	OnErrorSkip = "skip"
	// OnErrorDrop drops the entry, scope or resource a statement failed on.
	OnErrorDrop = "drop"
	// OnErrorReject rejects the event a statement failed on.
	OnErrorReject = "reject"

	// failureLogInterval bounds how often failed statements are logged, so a
	// statement that fails on every entry does not flood the log.
	failureLogInterval = time.Minute
)

// Spec is the spec of a transform stage. The driver's options are under the
// driver's name, e.g. `cel: {logs: [...]}`, and are passed to the driver.
type Spec struct {
	// Driver selects the language of the statements, e.g. cel.
	Driver string `json:"driver" validate:"required"`
	// OnError is what happens when a statement fails on an entry: skip the
	// statement (default), drop the entry, or reject the event.
	OnError string `json:"on_error,omitempty" validate:"omitempty,oneof=skip drop reject"`
}

type Transform struct {
	*base.Stage[Spec]

	program st.Program
	action  st.Action
	logger  *slog.Logger

	failures metric.Int64Counter
	// failureOptions holds the add options per signal: stream, node, signal.
	failureOptions map[domain.Signal][]metric.AddOption
	lastLog        atomic.Int64
}

// New creates the transform stage with the driver its spec names.
func New(
	stage domain.StageSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	provider metric.MeterProvider,
	stream string,
) (*Transform, error) {
	return newTransform(stage, validator, logger, provider, stream, drivers.Factory{})
}

func newTransform(
	stage domain.StageSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	provider metric.MeterProvider,
	stream string,
	driverFactory driver.Factory[st.Driver],
) (*Transform, error) {
	generic, opts, drv, err := driver.Split(stage.Spec, driverFactory)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](stage.Name, stage.From, generic, validator)
	if err != nil {
		return nil, err
	}
	program, err := drv.Compile(opts)
	if err != nil {
		return nil, err
	}
	t := &Transform{
		Stage:   b,
		program: program,
		action:  action(b.Spec().OnError),
		logger:  logger.With(slog.String("stage", stage.Name)),
	}
	t.failures, t.failureOptions = newFailureMetric(provider, stream, stage.Name)
	return t, nil
}

func action(onError string) st.Action {
	switch onError {
	case OnErrorDrop:
		return st.Drop
	case OnErrorReject:
		return st.Reject
	default:
		return st.Continue
	}
}

func newFailureMetric(
	provider metric.MeterProvider,
	stream, node string,
) (metric.Int64Counter, map[domain.Signal][]metric.AddOption) {
	counter, err := provider.Meter(meterName).Int64Counter(
		metrics.Name("stage", Type, "failed"),
		metric.WithUnit("{statement}"),
		metric.WithDescription("Statements that failed on a resource, scope or entry, whatever on_error then did"),
	)
	if err != nil {
		return nil, nil
	}
	options := map[domain.Signal][]metric.AddOption{
		domain.SignalLogs: nil, domain.SignalMetrics: nil, domain.SignalTraces: nil,
	}
	for _, signal := range []domain.Signal{domain.SignalLogs, domain.SignalMetrics, domain.SignalTraces} {
		options[signal] = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(
			attribute.String(metrics.LabelStream, stream),
			attribute.String(metrics.LabelNode, node),
			attribute.String(metrics.LabelSignal, string(signal)),
		))}
	}
	return counter, options
}

// rejected ends the walk of an event whose statement failed with on_error
// reject.
type rejected struct{ err error }

// Notify passes on a copy of the event with the statements applied. Signals
// without statements are passed on as they are.
func (t *Transform) Notify(ctx context.Context, event *domain.Event) error {
	out := *event
	run := &walk{t: t}

	if sp := t.program.Logs(); sp != nil && event.Logs != nil {
		run.signal, run.program = domain.SignalLogs, sp
		out.Logs = run.logs(event.Logs)
	}
	if sp := t.program.Metrics(); sp != nil && event.Metrics != nil && run.rejected == nil {
		run.signal, run.program = domain.SignalMetrics, sp
		out.Metrics = run.metrics(event.Metrics)
	}
	if sp := t.program.Traces(); sp != nil && event.Traces != nil && run.rejected == nil {
		run.signal, run.program = domain.SignalTraces, sp
		out.Traces = run.traces(event.Traces)
	}
	t.recordFailures(ctx, run)

	if run.rejected != nil {
		return fmt.Errorf("%w: %w", domain.ErrEventRejected, run.rejected.err)
	}
	if out.RecordCount() == 0 && event.RecordCount() > 0 {
		return nil
	}
	return t.NotifySubscribers(ctx, &out)
}

func (t *Transform) recordFailures(ctx context.Context, run *walk) {
	if t.failures == nil {
		return
	}
	for signal, n := range run.failed {
		t.failures.Add(ctx, n, t.failureOptions[signal]...)
	}
}

// walk applies the statements to one event, signal by signal.
type walk struct {
	t        *Transform
	signal   domain.Signal
	program  st.SignalProgram
	failed   map[domain.Signal]int64
	rejected *rejected
}

func (w *walk) onError(err error) st.Action {
	if w.failed == nil {
		w.failed = make(map[domain.Signal]int64, 1)
	}
	w.failed[w.signal]++
	w.logFailure(err)
	if w.t.action == st.Reject && w.rejected == nil {
		w.rejected = &rejected{err: err}
	}
	return w.t.action
}

// logFailure logs a failed statement at most once per failureLogInterval.
func (w *walk) logFailure(err error) {
	now := time.Now().UnixNano()
	last := w.t.lastLog.Load()
	if now-last < int64(failureLogInterval) || !w.t.lastLog.CompareAndSwap(last, now) {
		return
	}
	w.t.logger.Warn("Transform statement failed; see the stage's failed metric for how often",
		slog.String("signal", string(w.signal)), slog.String("on_error", onErrorName(w.t.action)),
		slog.Any("error", err))
}

func onErrorName(a st.Action) string {
	switch a {
	case st.Drop:
		return OnErrorDrop
	case st.Reject:
		return OnErrorReject
	case st.Continue:
		return OnErrorSkip
	default:
		return OnErrorSkip
	}
}

// keep tells whether a resource, scope or entry stays after its statements
// ran; it is false once the event is rejected, so the walk stops early.
func (w *walk) keep(a st.Action) bool {
	return w.rejected == nil && a != st.Drop
}

// resource returns a copy of r with the resource statements applied, and
// whether it stays. A missing resource is created for the statements and
// left out again if they set nothing on it.
func (w *walk) resource(r *resourcepb.Resource) (*resourcepb.Resource, bool) {
	out := copyResource(r)
	if out == nil {
		out = &resourcepb.Resource{}
	}
	ok := w.keep(w.program.Resource(out, w.onError))
	if r == nil && len(out.GetAttributes()) == 0 {
		out = nil
	}
	return out, ok
}

func (w *walk) scope(r *resourcepb.Resource, s *commonpb.InstrumentationScope) (*commonpb.InstrumentationScope, bool) {
	out := copyScope(s)
	if out == nil {
		out = &commonpb.InstrumentationScope{}
	}
	ok := w.keep(w.program.Scope(orEmptyResource(r), out, w.onError))
	if s == nil && len(out.GetAttributes()) == 0 {
		out = nil
	}
	return out, ok
}

func orEmptyResource(r *resourcepb.Resource) *resourcepb.Resource {
	if r == nil {
		return &resourcepb.Resource{}
	}
	return r
}

func orEmptyScope(s *commonpb.InstrumentationScope) *commonpb.InstrumentationScope {
	if s == nil {
		return &commonpb.InstrumentationScope{}
	}
	return s
}

// logs returns a copy of data with the statements applied. Scopes and
// resources whose entries were all dropped are left out.
//
//nolint:dupl // Same shape for another OTLP type; the shared logic is in resource, scope and keep.
func (w *walk) logs(data *logspb.LogsData) *logspb.LogsData {
	out := &logspb.LogsData{ResourceLogs: make([]*logspb.ResourceLogs, 0, len(data.GetResourceLogs()))}
	for _, rl := range data.GetResourceLogs() {
		resource, ok := w.resource(rl.GetResource())
		if !ok {
			continue
		}
		nrl := &logspb.ResourceLogs{Resource: resource, SchemaUrl: rl.GetSchemaUrl(),
			ScopeLogs: make([]*logspb.ScopeLogs, 0, len(rl.GetScopeLogs()))}
		for _, sl := range rl.GetScopeLogs() {
			if nsl, keep := w.scopeLogs(resource, sl); keep {
				nrl.ScopeLogs = append(nrl.ScopeLogs, nsl)
			}
		}
		if len(rl.GetScopeLogs()) == 0 || len(nrl.GetScopeLogs()) > 0 {
			out.ResourceLogs = append(out.ResourceLogs, nrl)
		}
	}
	return out
}

// scopeLogs returns a copy of sl with the statements applied, and whether it
// stays: it does unless its scope or all its records were dropped.
func (w *walk) scopeLogs(resource *resourcepb.Resource, sl *logspb.ScopeLogs) (*logspb.ScopeLogs, bool) {
	scope, ok := w.scope(resource, sl.GetScope())
	if !ok {
		return nil, false
	}
	nsl := &logspb.ScopeLogs{Scope: scope, SchemaUrl: sl.GetSchemaUrl(),
		LogRecords: make([]*logspb.LogRecord, 0, len(sl.GetLogRecords()))}
	for _, r := range sl.GetLogRecords() {
		record := copyLogRecord(r)
		if w.keep(w.program.Entry(st.Entry{
			Resource: orEmptyResource(resource), Scope: orEmptyScope(scope),
			Record: record, Attributes: &record.Attributes,
		}, w.onError)) {
			nsl.LogRecords = append(nsl.LogRecords, record)
		}
	}
	return nsl, len(sl.GetLogRecords()) == 0 || len(nsl.GetLogRecords()) > 0
}

//nolint:dupl // Same shape for another OTLP type; the shared logic is in resource, scope and keep.
func (w *walk) traces(data *tracepb.TracesData) *tracepb.TracesData {
	out := &tracepb.TracesData{ResourceSpans: make([]*tracepb.ResourceSpans, 0, len(data.GetResourceSpans()))}
	for _, rs := range data.GetResourceSpans() {
		resource, ok := w.resource(rs.GetResource())
		if !ok {
			continue
		}
		nrs := &tracepb.ResourceSpans{Resource: resource, SchemaUrl: rs.GetSchemaUrl(),
			ScopeSpans: make([]*tracepb.ScopeSpans, 0, len(rs.GetScopeSpans()))}
		for _, ss := range rs.GetScopeSpans() {
			if nss, keep := w.scopeSpans(resource, ss); keep {
				nrs.ScopeSpans = append(nrs.ScopeSpans, nss)
			}
		}
		if len(rs.GetScopeSpans()) == 0 || len(nrs.GetScopeSpans()) > 0 {
			out.ResourceSpans = append(out.ResourceSpans, nrs)
		}
	}
	return out
}

// scopeSpans returns a copy of ss with the statements applied, and whether
// it stays: it does unless its scope or all its spans were dropped.
func (w *walk) scopeSpans(resource *resourcepb.Resource, ss *tracepb.ScopeSpans) (*tracepb.ScopeSpans, bool) {
	scope, ok := w.scope(resource, ss.GetScope())
	if !ok {
		return nil, false
	}
	nss := &tracepb.ScopeSpans{Scope: scope, SchemaUrl: ss.GetSchemaUrl(),
		Spans: make([]*tracepb.Span, 0, len(ss.GetSpans()))}
	for _, s := range ss.GetSpans() {
		span := copySpan(s)
		if w.keep(w.program.Entry(st.Entry{
			Resource: orEmptyResource(resource), Scope: orEmptyScope(scope),
			Span: span, Attributes: &span.Attributes,
		}, w.onError)) {
			nss.Spans = append(nss.Spans, span)
		}
	}
	return nss, len(ss.GetSpans()) == 0 || len(nss.GetSpans()) > 0
}

//nolint:dupl // Same shape for another OTLP type; the shared logic is in resource, scope and keep.
func (w *walk) metrics(data *metricspb.MetricsData) *metricspb.MetricsData {
	out := &metricspb.MetricsData{
		ResourceMetrics: make([]*metricspb.ResourceMetrics, 0, len(data.GetResourceMetrics())),
	}
	for _, rm := range data.GetResourceMetrics() {
		resource, ok := w.resource(rm.GetResource())
		if !ok {
			continue
		}
		nrm := &metricspb.ResourceMetrics{Resource: resource, SchemaUrl: rm.GetSchemaUrl(),
			ScopeMetrics: make([]*metricspb.ScopeMetrics, 0, len(rm.GetScopeMetrics()))}
		for _, sm := range rm.GetScopeMetrics() {
			if nsm, keep := w.scopeMetrics(resource, sm); keep {
				nrm.ScopeMetrics = append(nrm.ScopeMetrics, nsm)
			}
		}
		if len(rm.GetScopeMetrics()) == 0 || len(nrm.GetScopeMetrics()) > 0 {
			out.ResourceMetrics = append(out.ResourceMetrics, nrm)
		}
	}
	return out
}

// scopeMetrics returns a copy of sm with the statements applied, and whether
// it stays: it does unless its scope was dropped or all its metrics lost
// their data points.
func (w *walk) scopeMetrics(
	resource *resourcepb.Resource,
	sm *metricspb.ScopeMetrics,
) (*metricspb.ScopeMetrics, bool) {
	scope, ok := w.scope(resource, sm.GetScope())
	if !ok {
		return nil, false
	}
	nsm := &metricspb.ScopeMetrics{Scope: scope, SchemaUrl: sm.GetSchemaUrl(),
		Metrics: make([]*metricspb.Metric, 0, len(sm.GetMetrics()))}
	for _, m := range sm.GetMetrics() {
		if metric, keep := w.metric(orEmptyResource(resource), orEmptyScope(scope), m); keep {
			nsm.Metrics = append(nsm.Metrics, metric)
		}
	}
	return nsm, len(sm.GetMetrics()) == 0 || len(nsm.GetMetrics()) > 0
}

// metric returns a copy of m with the entry statements applied to its data
// points, and whether it stays: it does unless all its data points were
// dropped.
func (w *walk) metric(
	r *resourcepb.Resource,
	s *commonpb.InstrumentationScope,
	m *metricspb.Metric,
) (*metricspb.Metric, bool) {
	out := copyMetric(m)
	keep := func(dp proto.Message, attrs *[]*commonpb.KeyValue) bool {
		return w.keep(w.program.Entry(st.Entry{
			Resource: r, Scope: s, Metric: out, DataPoint: dp, Attributes: attrs,
		}, w.onError))
	}
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		points, ok := points(data.Gauge.GetDataPoints(), copyNumberDataPoint,
			func(dp *metricspb.NumberDataPoint) bool { return keep(dp, &dp.Attributes) })
		out.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: points}}
		return out, ok
	case *metricspb.Metric_Sum:
		points, ok := points(data.Sum.GetDataPoints(), copyNumberDataPoint,
			func(dp *metricspb.NumberDataPoint) bool { return keep(dp, &dp.Attributes) })
		out.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			DataPoints:             points,
			AggregationTemporality: data.Sum.GetAggregationTemporality(),
			IsMonotonic:            data.Sum.GetIsMonotonic(),
		}}
		return out, ok
	case *metricspb.Metric_Histogram:
		points, ok := points(data.Histogram.GetDataPoints(), copyHistogramDataPoint,
			func(dp *metricspb.HistogramDataPoint) bool { return keep(dp, &dp.Attributes) })
		out.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints:             points,
			AggregationTemporality: data.Histogram.GetAggregationTemporality(),
		}}
		return out, ok
	case *metricspb.Metric_ExponentialHistogram:
		points, ok := points(data.ExponentialHistogram.GetDataPoints(), copyExponentialHistogramDataPoint,
			func(dp *metricspb.ExponentialHistogramDataPoint) bool { return keep(dp, &dp.Attributes) })
		out.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints:             points,
			AggregationTemporality: data.ExponentialHistogram.GetAggregationTemporality(),
		}}
		return out, ok
	case *metricspb.Metric_Summary:
		points, ok := points(data.Summary.GetDataPoints(), copySummaryDataPoint,
			func(dp *metricspb.SummaryDataPoint) bool { return keep(dp, &dp.Attributes) })
		out.Data = &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: points}}
		return out, ok
	}
	return m, true
}

// points copies the data points, runs the statements on each copy and keeps
// those keep reports. It tells whether the metric stays: it does unless all
// of its data points were dropped.
func points[T any](in []T, clone func(T) T, keep func(T) bool) ([]T, bool) {
	out := make([]T, 0, len(in))
	for _, dp := range in {
		c := clone(dp)
		if keep(c) {
			out = append(out, c)
		}
	}
	return out, len(in) == 0 || len(out) > 0
}
