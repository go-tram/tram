// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

type tapOptions struct {
	component string
	rate      float64
	output    string
	limit     int
}

func newTapCmd() *cobra.Command {
	var opts tapOptions
	var target targetFlags
	cmd := &cobra.Command{
		Use:   "tap <stream>",
		Short: "Show the events passing a running stream",
		Long: `Show the events passing a running stream of a TRAM server.

The server hands out copies of the events at the source, at every stage and
at every sink, or at the one component --component names: what a source or
stage passed on, and what a sink received and whether it took it. The stream
is not changed and never waits for the tap: events the tap cannot take in
time, or beyond --rate, are dropped and reported.

The tap keeps running when the stream is replaced, and ends when the stream
is deleted or the tapped component is removed. It talks to the admin API of
the server (tram server --admin-endpoint, localhost:50052 by default).

In Kubernetes, --kube taps every pod of the Engines the stream's Stream
resource names, from the kubeconfig's current context; --engine or
--selector name the pods instead. Every pod runs the stream with its share
of the data. The CLI forwards a port to each pod, as kubectl port-forward
does, follows scaling and rollouts, and shows the pod of every event.
--rate and --limit count for all pods together.`,
		Example: `  tram tap my-stream
  tram tap my-stream --component loki --rate 10
  tram tap my-stream --output json --limit 100 > events.ndjson
  tram tap observability.logs --kube         # the Stream resource logs in observability
  tram tap my-stream --engine telemetry -n observability`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.output != outputText && opts.output != outputJSON {
				return fmt.Errorf("invalid --output %q: use %s or %s", opts.output, outputText, outputJSON)
			}
			t, err := target.resolve()
			if err != nil {
				return err
			}
			// From here on, errors come from the server, not from the usage.
			cmd.SilenceUsage = true
			c, err := t.connect(cmd.Context(), args[0], cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			return runTap(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), opts, args[0], c, t)
		},
	}
	addTargetFlags(cmd, &target)
	flags := cmd.Flags()
	flags.StringVar(&opts.component, "component", "", "Source, stage or sink to tap (default all)")
	flags.Float64Var(&opts.rate, "rate", 0, "Most events per second to show; more are dropped (default no limit)")
	flags.StringVarP(&opts.output, "output", "o", outputText, "Output: text, or json with one event per line")
	flags.IntVar(&opts.limit, "limit", 0, "Stop after this many events (default no limit)")
	return cmd
}

// tapOutput writes the events of all taps one after another and counts them
// against the limit.
type tapOutput struct {
	mu     sync.Mutex
	out    io.Writer
	errOut io.Writer
	opts   tapOptions
	shown  int
	// limiter holds --rate for all taps together; nil without a rate.
	limiter *rate.Limiter
	dropped uint64
	// done is called once the limit is reached.
	done func()
}

func (o *tapOutput) write(pod string, resp *v1.TapStreamResponse) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.opts.limit > 0 && o.shown >= o.opts.limit {
		return nil
	}
	if o.limiter != nil && !o.limiter.Allow() {
		o.dropped += 1 + resp.GetDropped()
		return nil
	}
	// Drops are counted for all pods together, like --rate.
	if dropped := o.dropped + resp.GetDropped(); dropped > 0 {
		_, _ = fmt.Fprintf(o.errOut, "dropped %d events\n", dropped)
		o.dropped = 0
	}
	var err error
	if o.opts.output == outputJSON {
		err = writeJSON(o.out, pod, resp)
	} else {
		err = writeText(o.out, pod, resp)
	}
	o.shown++
	if o.opts.limit > 0 && o.shown >= o.opts.limit {
		o.done()
	}
	return err
}

func runTap(
	ctx context.Context,
	out, errOut io.Writer,
	opts tapOptions,
	name string,
	c connector,
	t target,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	output := &tapOutput{out: out, errOut: errOut, opts: opts, done: cancel}
	if opts.rate > 0 && c.many() {
		// Every pod sends up to the rate; the output keeps it for all of
		// them together.
		output.limiter = rate.NewLimiter(rate.Limit(opts.rate), max(1, int(opts.rate)))
	}
	req := &v1.TapStreamRequest{Name: name, Component: opts.component, Rate: opts.rate}

	return c.each(ctx, func(ctx context.Context, in instance) error {
		where := t.address
		if in.name != "" {
			where = "pod " + in.name
		}
		return tapInstance(ctx, in, where, req, output)
	})
}

// tapInstance taps one instance and writes its events until the tap ends.
func tapInstance(
	ctx context.Context,
	in instance,
	where string,
	req *v1.TapStreamRequest,
	output *tapOutput,
) error {
	stream, err := v1.NewTapServiceClient(in.conn).TapStream(ctx, req)
	if err != nil {
		return tapError(where, err)
	}
	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			return tapEnd(ctx, in, where, recvErr, output.errOut)
		}
		if err = output.write(in.name, resp); err != nil {
			return err
		}
	}
}

// tapEnd is how the tap of an instance ended: nil if the CLI ended it or a
// pod went away, e.g. in a rollout, which ends the tap of that pod only.
func tapEnd(ctx context.Context, in instance, where string, err error, errOut io.Writer) error {
	code := status.Code(err)
	if ctx.Err() != nil || errors.Is(err, io.EOF) || code == codes.Canceled {
		return nil //nolint:nilerr // The CLI ended the tap.
	}
	if in.name != "" && code == codes.Unavailable {
		_, _ = fmt.Fprintf(errOut, "%s: %s\n", where, status.Convert(err).Message())
		return nil
	}
	return tapError(where, err)
}

func tapError(where string, err error) error {
	code := status.Code(err)
	if code == codes.Unavailable {
		return fmt.Errorf("admin API at %s is not available: %s", where, status.Convert(err).Message())
	}
	if code == codes.Unimplemented {
		return fmt.Errorf("%s serves no taps; is it the admin API (tram server --admin-endpoint)?", where)
	}
	if code == codes.NotFound || code == codes.InvalidArgument {
		return errors.New(status.Convert(err).Message())
	}
	return err
}

// writeJSON writes resp as one line of JSON, with the field "pod" in front
// for a pod.
func writeJSON(out io.Writer, pod string, resp *v1.TapStreamResponse) error {
	data, err := protojson.MarshalOptions{Multiline: false}.Marshal(resp)
	if err != nil {
		return err
	}
	if pod != "" {
		var fields map[string]json.RawMessage
		if err = json.Unmarshal(data, &fields); err != nil {
			return err
		}
		podJSON, _ := json.Marshal(pod)
		fields["pod"] = podJSON
		if data, err = json.Marshal(fields); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(out, "%s\n", data)
	return err
}

// writeText writes a line about the event, with its pod for a pod, then a
// line per log record, metric or span.
func writeText(out io.Writer, pod string, resp *v1.TapStreamResponse) error {
	var b strings.Builder
	kind := strings.ToLower(strings.TrimPrefix(resp.GetKind().String(), "COMPONENT_KIND_"))
	b.WriteString(resp.GetTime().AsTime().Local().Format("15:04:05.000"))
	if pod != "" {
		b.WriteString("  " + pod)
	}
	fmt.Fprintf(&b, "  %-6s %s", kind, resp.GetComponent())
	signal, count := summary(resp)
	fmt.Fprintf(&b, "  %s %d", signal, count)
	if sink := resp.GetSink(); sink != nil {
		duration := sink.GetDuration().AsDuration().Round(time.Microsecond)
		if sink.GetError() == "" {
			fmt.Fprintf(&b, "  ok %s", duration)
		} else {
			fmt.Fprintf(&b, "  error %s: %s", duration, sink.GetError())
		}
	}
	b.WriteByte('\n')
	writeLogs(&b, resp.GetLogs())
	writeMetrics(&b, resp.GetMetrics())
	writeSpans(&b, resp.GetTraces())
	_, err := io.WriteString(out, b.String())
	return err
}

func writeLogs(b *strings.Builder, logs *logspb.LogsData) {
	for _, resource := range logs.GetResourceLogs() {
		for _, scope := range resource.GetScopeLogs() {
			for _, record := range scope.GetLogRecords() {
				severity := record.GetSeverityText()
				if severity == "" {
					severity = "-"
				}
				fmt.Fprintf(b, "    %-5s %s%s\n",
					severity, value(record.GetBody()), attributes(record.GetAttributes()))
			}
		}
	}
}

func writeMetrics(b *strings.Builder, metrics *metricspb.MetricsData) {
	for _, resource := range metrics.GetResourceMetrics() {
		for _, scope := range resource.GetScopeMetrics() {
			for _, metric := range scope.GetMetrics() {
				fmt.Fprintf(b, "    %s\n", metric.GetName())
			}
		}
	}
}

func writeSpans(b *strings.Builder, traces *tracepb.TracesData) {
	for _, resource := range traces.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			for _, span := range scope.GetSpans() {
				fmt.Fprintf(b, "    %s%s\n", span.GetName(), attributes(span.GetAttributes()))
			}
		}
	}
}

// summary returns the signal of the event and its number of log records,
// metrics or spans.
func summary(resp *v1.TapStreamResponse) (string, int) {
	var logs, metrics, spans int
	for _, resource := range resp.GetLogs().GetResourceLogs() {
		for _, scope := range resource.GetScopeLogs() {
			logs += len(scope.GetLogRecords())
		}
	}
	for _, resource := range resp.GetMetrics().GetResourceMetrics() {
		for _, scope := range resource.GetScopeMetrics() {
			metrics += len(scope.GetMetrics())
		}
	}
	for _, resource := range resp.GetTraces().GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			spans += len(scope.GetSpans())
		}
	}
	switch {
	case resp.GetMetrics() != nil:
		return "metrics", metrics
	case resp.GetTraces() != nil:
		return "spans", spans
	default:
		return "logs", logs
	}
}

func attributes(attrs []*commonpb.KeyValue) string {
	if len(attrs) == 0 {
		return ""
	}
	var b strings.Builder
	for _, attr := range attrs {
		fmt.Fprintf(&b, " %s=%s", attr.GetKey(), value(attr.GetValue()))
	}
	return b.String()
}

func value(v *commonpb.AnyValue) string {
	switch v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(v.GetIntValue(), 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(v.GetDoubleValue(), 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(v.GetBoolValue())
	case nil:
		return ""
	default:
		data, err := protojson.Marshal(v)
		if err != nil {
			return v.String()
		}
		return string(data)
	}
}
