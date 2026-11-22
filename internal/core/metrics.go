// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"context"
	"errors"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/go-tram/tram/internal/core/domain"
	metricnames "github.com/go-tram/tram/internal/metrics"
)

const meterName = "github.com/go-tram/tram/internal/core"

const (
	outcomeOK        = "ok"
	outcomeRejected  = "rejected"
	outcomeCancelled = "cancelled"
	outcomeError     = "error"
)

type metrics struct {
	records     metric.Int64Counter
	recordsOut  metric.Int64Counter
	duration    metric.Float64Histogram
	requestSize metric.Int64Histogram
}

func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	meter := provider.Meter(meterName)

	records, recordsErr := meter.Int64Counter(metricnames.Name("node", "records"),
		metric.WithDescription("Records a node received: log records, metric data points and spans"),
		metric.WithUnit("{record}"),
	)
	recordsOut, recordsOutErr := meter.Int64Counter(metricnames.Name("node", "records", "out"),
		metric.WithDescription(
			"Records a source or stage passed on: log records, metric data points and spans, "+
				"each counted once however many children it has",
		),
		metric.WithUnit("{record}"),
	)
	duration, durationErr := meter.Float64Histogram(
		metricnames.Name("node", "duration"),
		metric.WithDescription("Time a node took to handle an event, including the nodes below it"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets()...),
	)
	requestSize, requestSizeErr := meter.Int64Histogram(
		metricnames.Name("source", "request", "size"),
		metric.WithDescription(
			"Size of the requests a source received, as received and after decoding the content encoding",
		),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(requestSizeBuckets()...),
	)
	return &metrics{records: records, recordsOut: recordsOut, duration: duration, requestSize: requestSize},
		errors.Join(recordsErr, recordsOutErr, durationErr, requestSizeErr)
}

// Ingresses commonly limit requests to 1 or 2 MB, and the OTLP source accepts
// up to 20 MB decoded, so those limits are bounds of their own.
func requestSizeBuckets() []float64 {
	return []float64{
		1 << 10, 4 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10,
		1 << 20, 2 << 20, 4 << 20, 8 << 20, 16 << 20, 20 << 20, 32 << 20,
	}
}

// Finer in seconds: a batch waits just over its timeout, usually a round
// number of seconds, and coarse buckets would push its quantiles towards the
// next bound.
func durationBuckets() []float64 {
	return []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		0.75, 1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 6, 7.5, 10, 15, 30,
	}
}

func nodeAttributes(stream string, node *domain.Node) func(key, value string) metric.MeasurementOption {
	base := []attribute.KeyValue{
		attribute.String(metricnames.LabelStream, stream),
		attribute.String(metricnames.LabelNode, node.Name),
		attribute.String(metricnames.LabelKind, string(node.Kind)),
	}
	return func(key, value string) metric.MeasurementOption {
		return metric.WithAttributeSet(attribute.NewSet(append(base, attribute.String(key, value))...))
	}
}

type recordCounter struct {
	counter                 metric.Int64Counter
	logs, dataPoints, spans []metric.AddOption
}

func newRecordCounter(
	counter metric.Int64Counter,
	with func(key, value string) metric.MeasurementOption,
) recordCounter {
	add := func(signal domain.Signal) []metric.AddOption {
		return []metric.AddOption{with(metricnames.LabelSignal, string(signal))}
	}
	return recordCounter{
		counter:    counter,
		logs:       add(domain.SignalLogs),
		dataPoints: add(domain.SignalMetrics),
		spans:      add(domain.SignalTraces),
	}
}

// count does nothing for the zero recordCounter, the one of nodes whose stream
// leaves out their records.
func (c *recordCounter) count(ctx context.Context, event *domain.Event) {
	if c.counter == nil {
		return
	}
	logs, dataPoints, spans := event.RecordCounts()
	c.add(ctx, logs, c.logs)
	c.add(ctx, dataPoints, c.dataPoints)
	c.add(ctx, spans, c.spans)
}

func (c *recordCounter) add(ctx context.Context, records int, signal []metric.AddOption) {
	if records > 0 {
		c.counter.Add(ctx, int64(records), signal...)
	}
}

func (m *metrics) output(spec domain.StreamSpec, node *domain.Node) recordCounter {
	if !spec.Metrics.Records.Enabled(node.Kind) {
		return recordCounter{}
	}
	return newRecordCounter(m.recordsOut, nodeAttributes(spec.Name, node))
}

// instrument returns the subscriber unchanged if its stream leaves out every
// metric of the node.
func (m *metrics) instrument(
	spec domain.StreamSpec,
	node *domain.Node,
	subscriber domain.Subscriber,
) domain.Subscriber {
	records := spec.Metrics.Records.Enabled(node.Kind)
	duration := spec.Metrics.Duration.Enabled(node.Kind)
	requests := node.Kind == domain.KindSource && spec.Metrics.RequestSizeEnabled()
	if !records && !duration && !requests {
		return subscriber
	}

	with := nodeAttributes(spec.Name, node)
	s := &instrumentedSubscriber{Subscriber: subscriber}
	if records {
		s.records = newRecordCounter(m.records, with)
	}
	if duration {
		record := func(outcome string) []metric.RecordOption {
			return []metric.RecordOption{with(metricnames.LabelOutcome, outcome)}
		}
		s.duration = m.duration
		s.ok = record(outcomeOK)
		s.rejected = record(outcomeRejected)
		s.cancelled = record(outcomeCancelled)
		s.failed = record(outcomeError)
	}
	if requests {
		s.requests = newRequestSizes(m.requestSize, spec.Name, node)
	}
	return s
}

// requestSizes records the sizes of the requests a source received.
type requestSizes struct {
	histogram metric.Int64Histogram
	base      []attribute.KeyValue
	// byEncoding holds the options of the encodings sources accept.
	byEncoding map[string]sizeOptions
}

type sizeOptions struct {
	received, decoded metric.RecordOption
}

func newRequestSizes(histogram metric.Int64Histogram, stream string, node *domain.Node) *requestSizes {
	r := &requestSizes{histogram: histogram, base: []attribute.KeyValue{
		attribute.String(metricnames.LabelStream, stream),
		attribute.String(metricnames.LabelNode, node.Name),
		attribute.String(metricnames.LabelKind, string(node.Kind)),
	}}
	r.byEncoding = map[string]sizeOptions{"identity": r.options("identity"), "gzip": r.options("gzip")}
	return r
}

func (r *requestSizes) options(encoding string) sizeOptions {
	with := func(body string) metric.RecordOption {
		return metric.WithAttributeSet(attribute.NewSet(append(slices.Clone(r.base),
			attribute.String(metricnames.LabelEncoding, encoding),
			attribute.String(metricnames.LabelBody, body))...))
	}
	return sizeOptions{received: with("received"), decoded: with("decoded")}
}

func (r *requestSizes) record(ctx context.Context, request *domain.Request) {
	opts, ok := r.byEncoding[request.Encoding]
	if !ok {
		opts = r.options(request.Encoding)
	}
	r.histogram.Record(ctx, int64(request.WireBytes), opts.received)
	r.histogram.Record(ctx, int64(request.DecodedBytes), opts.decoded)
}

type instrumentedSubscriber struct {
	domain.Subscriber

	records recordCounter
	// duration is nil if the stream leaves out the duration of the node.
	duration                        metric.Float64Histogram
	ok, rejected, cancelled, failed []metric.RecordOption
	// requests is set for sources only.
	requests *requestSizes
}

func (s *instrumentedSubscriber) Notify(ctx context.Context, event *domain.Event) error {
	s.records.count(ctx, event)
	if s.requests != nil && event.Request != nil {
		s.requests.record(ctx, event.Request)
	}
	if s.duration == nil {
		return s.Subscriber.Notify(ctx, event)
	}

	start := time.Now()
	err := s.Subscriber.Notify(ctx, event)

	outcome := s.ok
	switch {
	case errors.Is(err, domain.ErrEventRejected):
		outcome = s.rejected
	case errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled):
		outcome = s.cancelled
	case err != nil:
		outcome = s.failed
	}
	s.duration.Record(ctx, time.Since(start).Seconds(), outcome...)
	return err
}
