// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package broker

import (
	"errors"
	"fmt"

	"cel.dev/cel-go/cel"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracespb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/go-tram/tram/internal/adapters/broker/partition"
	"github.com/go-tram/tram/internal/adapters/sink/celentry"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	logsName    = "logs"
	metricsName = "metrics"
	tracesName  = "traces"

	partitionVariable = "partition"
)

var ErrInvalidPartition = errors.New("invalid partition")

// programs holds an expression per signal, nil for a signal not sent.
type programs struct {
	logs    cel.Program
	metrics cel.Program
	traces  cel.Program
}

// partitions is nil without partitioning.
type partitions struct {
	count int
	keys  programs
}

// message is one group of an event: the records of one signal that go to one
// destination.
type message struct {
	destination string
	event       *domain.Event
}

func compile(spec *Spec) (programs, *partitions, error) {
	if err := checkPartitioning(spec); err != nil {
		return programs{}, nil, err
	}
	var extra []cel.EnvOption
	if spec.Partition != nil {
		extra = append(extra, cel.Variable(partitionVariable, cel.IntType))
	}
	destinations, err := compilePrograms(
		celentry.Expressions{
			Logs:    spec.Destination.Logs,
			Metrics: spec.Destination.Metrics,
			Traces:  spec.Destination.Traces,
		},
		extra...)
	if err != nil || spec.Partition == nil {
		return destinations, nil, err
	}
	p := spec.Partition
	keys, err := compilePrograms(celentry.Expressions{Logs: p.Logs, Metrics: p.Metrics, Traces: p.Traces})
	if err != nil {
		return programs{}, nil, fmt.Errorf("%w: key: %w", ErrInvalidPartition, err)
	}
	return destinations, &partitions{count: p.Count, keys: keys}, nil
}

func compilePrograms(e celentry.Expressions, extra ...cel.EnvOption) (programs, error) {
	compiled, err := celentry.Compile(e, extra...)
	if err != nil {
		return programs{}, err
	}
	return programs{logs: compiled.Logs, metrics: compiled.Metrics, traces: compiled.Traces}, nil
}

// checkPartitioning checks that every sent signal has a key and no key is set
// for a signal that is not sent.
func checkPartitioning(spec *Spec) error {
	p := spec.Partition
	if p == nil {
		return nil
	}
	for _, signal := range []struct{ name, destination, key string }{
		{logsName, spec.Destination.Logs, p.Logs},
		{metricsName, spec.Destination.Metrics, p.Metrics},
		{tracesName, spec.Destination.Traces, p.Traces},
	} {
		switch {
		case signal.destination != "" && signal.key == "":
			return fmt.Errorf("%w: %s are sent, but partition.%s is not set",
				ErrInvalidPartition, signal.name, signal.name)
		case signal.destination == "" && signal.key != "":
			return fmt.Errorf("%w: partition.%s is set, but %s are not sent",
				ErrInvalidPartition, signal.name, signal.name)
		}
	}
	return nil
}

// group splits the event into one message per signal and destination. check
// refuses a destination no message can be sent to; it is called once per
// destination.
func (s *Sink) group(event *domain.Event, check func(string) error) ([]message, error) {
	var messages []message
	var keys programs
	if s.partitions != nil {
		keys = s.partitions.keys
	}
	spec := s.Spec().Destination

	if event.Logs != nil && spec.Logs != "" {
		parts, err := domain.PartitionLogs(event.Logs, celentry.LogKey(s.destinations.logs, s.resolver(keys.logs)))
		if err != nil {
			return nil, fmt.Errorf("logs: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *logspb.LogsData) *domain.Event {
			return &domain.Event{Logs: d}
		})
	}
	if event.Metrics != nil && spec.Metrics != "" {
		parts, err := domain.PartitionMetrics(event.Metrics,
			celentry.MetricKey(s.destinations.metrics, s.resolver(keys.metrics)))
		if err != nil {
			return nil, fmt.Errorf("metrics: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *metricspb.MetricsData) *domain.Event {
			return &domain.Event{Metrics: d}
		})
	}
	if event.Traces != nil && spec.Traces != "" {
		parts, err := domain.PartitionSpans(
			event.Traces,
			celentry.SpanKey(s.destinations.traces, s.resolver(keys.traces)),
		)
		if err != nil {
			return nil, fmt.Errorf("traces: %w", err)
		}
		messages = appendMessages(messages, parts, func(d *tracespb.TracesData) *domain.Event {
			return &domain.Event{Traces: d}
		})
	}

	// Checked once per destination rather than for every record.
	if err := checkDestinations(messages, check); err != nil {
		return nil, err
	}
	return messages, nil
}

func checkDestinations(messages []message, check func(string) error) error {
	for _, m := range messages {
		if err := check(m.destination); err != nil {
			return err
		}
	}
	return nil
}

func appendMessages[D any](messages []message, parts []domain.Part[D], event func(D) *domain.Event) []message {
	for _, part := range parts {
		messages = append(messages, message{destination: part.Key, event: event(part.Data)})
	}
	return messages
}

// resolver evaluates the destination of an entry. With partitioning, it sets
// the entry's partition first, from the key that keyProgram returns.
func (s *Sink) resolver(keyProgram cel.Program) celentry.Resolve {
	if s.partitions == nil {
		return celentry.String
	}
	count := s.partitions.count
	return func(program cel.Program, vars *celentry.Vars) (string, error) {
		key, err := celentry.String(keyProgram, vars)
		if err != nil {
			return "", fmt.Errorf("partition key: %w", err)
		}
		vars.Set(partitionVariable, int64(partition.Of(key, count)))
		return celentry.String(program, vars)
	}
}
