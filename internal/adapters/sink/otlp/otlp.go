// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"cel.dev/cel-go/cel"
	"github.com/go-playground/validator/v10"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"golang.org/x/sync/errgroup"

	"github.com/go-tram/tram/internal/adapters/sink/base"
	"github.com/go-tram/tram/internal/adapters/sink/celentry"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	Type = "otlp"

	defaultTimeout = 10 * time.Second

	maxRedirects = 10

	maxErrorBodyBytes = 1024

	// Bounds the requests of one event that are sent at the same time, e.g.
	// to many destinations from a url_expression.
	maxConcurrentRequests = 16
)

type OTLP struct {
	*base.Sink[Spec]

	// Nil for signals without a URL expression.
	urls celentry.Programs

	logger *slog.Logger
	client *http.Client
}

// request is the data of one signal for one destination.
type request struct {
	url    string
	signal domain.Signal
	data   []byte
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*OTLP, error) {
	baseSink, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}

	timeout := defaultTimeout
	if raw := baseSink.Spec().Timeout; raw != "" {
		timeout, err = time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %w", err)
		}
		if timeout <= 0 {
			return nil, fmt.Errorf("invalid timeout: %s is not positive", raw)
		}
	}

	urls, err := compileURLs(baseSink.Spec().URLExpression)
	if err != nil {
		return nil, err
	}

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport is not an *http.Transport")
	}
	transport = transport.Clone()
	transport.MaxIdleConnsPerHost = transport.MaxIdleConns
	transport.TLSClientConfig = &tls.Config{
		//nolint:gosec // Opt-in through the insecure option, for endpoints with self-signed certificates.
		InsecureSkipVerify: baseSink.Spec().Insecure,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("stopped after max redirects")
			}
			return nil
		},
	}

	return &OTLP{
		Sink:   baseSink,
		urls:   urls,
		logger: logger,
		client: client,
	}, nil
}

// compileURLs compiles the URL expressions. Signals without one get a nil
// program.
func compileURLs(spec *URLExpressionSpec) (celentry.Programs, error) {
	if spec == nil {
		return celentry.Programs{}, nil
	}
	programs, err := celentry.Compile(celentry.Expressions{Logs: spec.Logs, Metrics: spec.Metrics, Traces: spec.Traces})
	if err != nil {
		return celentry.Programs{}, fmt.Errorf("invalid url_expression: %w", err)
	}
	if spec.Logs == "" {
		programs.Logs = nil
	}
	if spec.Metrics == "" {
		programs.Metrics = nil
	}
	if spec.Traces == "" {
		programs.Traces = nil
	}
	return programs, nil
}

func (o *OTLP) Notify(ctx context.Context, event *domain.Event) error {
	if event == nil {
		return nil
	}

	requests, err := o.requests(event)
	if err != nil {
		return err
	}
	return o.sendAll(ctx, requests)
}

func (o *OTLP) requests(event *domain.Event) ([]request, error) {
	if o.Spec().URLExpression == nil {
		payloads, err := event.MarshalOTLPPayloads()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal event: %w", err)
		}
		requests := make([]request, len(payloads))
		for i, p := range payloads {
			requests[i] = request{url: o.Spec().URL, signal: p.Signal, data: p.Data}
		}
		return requests, nil
	}

	var requests []request
	var err error
	if event.Logs != nil {
		requests, err = appendRequests(requests, o.urls.Logs, o.Spec().URL, &domain.Event{Logs: event.Logs},
			func(key domain.LogKey) ([]domain.Part[*logspb.LogsData], error) {
				return domain.PartitionLogs(event.Logs, key)
			}, celentry.LogKey,
			func(d *logspb.LogsData) *domain.Event { return &domain.Event{Logs: d} })
		if err != nil {
			return nil, fmt.Errorf("logs: %w", err)
		}
	}
	if event.Metrics != nil {
		requests, err = appendRequests(requests, o.urls.Metrics, o.Spec().URL, &domain.Event{Metrics: event.Metrics},
			func(key domain.MetricKey) ([]domain.Part[*metricspb.MetricsData], error) {
				return domain.PartitionMetrics(event.Metrics, key)
			}, celentry.MetricKey,
			func(d *metricspb.MetricsData) *domain.Event { return &domain.Event{Metrics: d} })
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
	}
	if event.Traces != nil {
		requests, err = appendRequests(requests, o.urls.Traces, o.Spec().URL, &domain.Event{Traces: event.Traces},
			func(key domain.SpanKey) ([]domain.Part[*tracepb.TracesData], error) {
				return domain.PartitionSpans(event.Traces, key)
			}, celentry.SpanKey,
			func(d *tracepb.TracesData) *domain.Event { return &domain.Event{Traces: d} })
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
	}
	return requests, nil
}

// appendRequests appends the requests for one signal: one per URL its entries
// resolve to, or one to fallback for a signal without a URL expression. A
// signal with neither is not sent.
func appendRequests[K any, D any](
	requests []request,
	program cel.Program,
	fallback string,
	whole *domain.Event,
	partition func(K) ([]domain.Part[D], error),
	key func(cel.Program, celentry.Resolve) K,
	event func(D) *domain.Event,
) ([]request, error) {
	if program == nil {
		if fallback == "" {
			return requests, nil
		}
		return appendRequest(requests, fallback, whole)
	}

	parts, err := partition(key(program, urlOf()))
	if err != nil {
		// The same data resolves the same way again.
		return nil, fmt.Errorf("%w: could not resolve URL: %w", domain.ErrEventRejected, err)
	}
	for _, part := range parts {
		if requests, err = appendRequest(requests, part.Key, event(part.Data)); err != nil {
			return nil, err
		}
	}
	return requests, nil
}

func appendRequest(requests []request, url string, event *domain.Event) ([]request, error) {
	signal, data, err := event.MarshalOTLPData()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}
	return append(requests, request{url: url, signal: signal, data: data}), nil
}

// urlOf resolves the URL of an entry and checks it once per event.
func urlOf() celentry.Resolve {
	checked := map[string]bool{}
	return func(program cel.Program, vars *celentry.Vars) (string, error) {
		raw, err := celentry.String(program, vars)
		if err != nil {
			return "", err
		}
		if !checked[raw] {
			if err = validURL(raw); err != nil {
				return "", err
			}
			checked[raw] = true
		}
		return raw, nil
	}
}

func validURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("URL %q is not http or https", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("URL %q has no host", raw)
	}
	return nil
}

// sendAll sends the requests at the same time. If any fails in a way that may
// go away, the event fails retriably, so the source retries all of it: the
// failed requests may succeed then, while a request rejected for good would be
// rejected again either way.
func (o *OTLP) sendAll(ctx context.Context, requests []request) error {
	if len(requests) == 1 {
		return o.send(ctx, requests[0])
	}

	var mu sync.Mutex
	var transient, rejected []error
	var group errgroup.Group
	group.SetLimit(maxConcurrentRequests)
	for _, r := range requests {
		group.Go(func() error {
			if err := o.send(ctx, r); err != nil {
				mu.Lock()
				defer mu.Unlock()
				if errors.Is(err, domain.ErrEventRejected) {
					rejected = append(rejected, err)
				} else {
					transient = append(transient, err)
				}
			}
			return nil
		})
	}
	_ = group.Wait()

	if len(transient) > 0 {
		if len(rejected) > 0 {
			o.logger.WarnContext(ctx, "Destinations rejected data; retrying the event because others failed",
				slog.String("sink", o.Name()), slog.Any("rejected", errors.Join(rejected...)))
		}
		return errors.Join(transient...)
	}
	return errors.Join(rejected...)
}

func (o *OTLP) send(ctx context.Context, r request) error {
	signal, data := r.signal, r.data
	endpoint := strings.TrimRight(r.url, "/") + o.getEndpointPath(signal)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	for key, value := range o.Spec().Headers {
		req.Header.Set(key, value)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		// The error of the client names the URL, but without user info.
		return fmt.Errorf("failed to send OTLP data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		err = fmt.Errorf("OTLP server %s returned status %d: %s", req.URL.Redacted(), resp.StatusCode, string(body))
		if permanent(resp.StatusCode) {
			// The same data gets the same answer again, so it is rejected
			// instead of retried, e.g. by a broker source having it redelivered.
			return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
		}
		return err
	}

	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		o.logger.WarnContext(ctx, "Failed to read response body",
			slog.String("sink", o.Name()),
			slog.String("signal", string(signal)),
			slog.Any("error", err))
	}

	o.logger.DebugContext(ctx, "OTLP sink sent data",
		slog.String("sink", o.Name()),
		slog.String("signal", string(signal)),
		slog.Int("bytes", len(data)),
		slog.Int("status", resp.StatusCode))

	return nil
}

// permanent reports whether a status refuses the data itself: a client error
// other than 408 Request Timeout and 429 Too Many Requests, which OTLP
// clients retry. Server errors may go away and are retried as well.
func permanent(status int) bool {
	return status >= 400 && status < 500 &&
		status != http.StatusRequestTimeout && status != http.StatusTooManyRequests
}

func (o *OTLP) getEndpointPath(signal domain.Signal) string {
	switch signal {
	case domain.SignalLogs:
		return "/v1/logs"
	case domain.SignalMetrics:
		return "/v1/metrics"
	case domain.SignalTraces:
		return "/v1/traces"
	default:
		return ""
	}
}

func (o *OTLP) Stop() error {
	o.client.CloseIdleConnections()
	return nil
}
