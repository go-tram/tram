// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package broker is the broker sink: it sends OTLP data to a message broker
// through the driver its spec names. Each record goes to the destination its
// CEL expression returns; the records of one signal and destination become
// one message with a plain OTLP body.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	contract "github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/broker/drivers"
	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/sink/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	Type = "broker"

	// Bounds the messages a sink has in flight, so a fast sink cannot make
	// the broker buffer without bounds. A sink at the bound waits instead,
	// which slows its source down.
	defaultMaxPending = 256
)

type Sink struct {
	*base.Sink[Spec]

	destinations programs
	// Nil without partitioning.
	partitions *partitions

	publisher contract.Publisher
	logger    *slog.Logger
}

func New(sink domain.SinkSpec, validator *validator.Validate, logger *slog.Logger) (*Sink, error) {
	return newSink(sink, validator, logger, drivers.NewPublisherFactory(validator))
}

// newSink is [New] with the drivers to resolve the spec's `driver` from, so
// tests can inject the in-memory driver; see export_test.go.
func newSink(
	sink domain.SinkSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[contract.PublisherDriver],
) (*Sink, error) {
	generic, opts, drv, err := driver.Split(sink.Spec, driverFactory)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](sink.Name, sink.From, generic, validator)
	if err != nil {
		return nil, err
	}
	destinations, parts, err := compile(b.Spec())
	if err != nil {
		return nil, err
	}
	logger = logger.With(slog.String("sink", sink.Name))
	publisher, err := drv.NewPublisher(opts, contract.PublisherConfig{
		Name:       "sink " + sink.Name,
		MaxPending: maxPending(b.Spec()),
	}, logger)
	if err != nil {
		return nil, err
	}
	return &Sink{Sink: b, destinations: destinations, partitions: parts, publisher: publisher, logger: logger}, nil
}

// Check validates sink like New, without connecting to the broker.
func Check(sink domain.SinkSpec, validator *validator.Validate) error {
	generic, opts, drv, err := driver.Split(sink.Spec, drivers.NewPublisherFactory(validator))
	if err != nil {
		return err
	}
	b, err := base.New[Spec](sink.Name, sink.From, generic, validator)
	if err != nil {
		return err
	}
	if _, _, err = compile(b.Spec()); err != nil {
		return err
	}
	return drv.CheckPublisher(opts)
}

func maxPending(spec *Spec) int {
	if spec.MaxPending != 0 {
		return spec.MaxPending
	}
	return defaultMaxPending
}

func (s *Sink) Notify(ctx context.Context, event *domain.Event) error {
	messages, err := s.group(event, s.publisher.CheckDestination)
	if err != nil {
		// Resolving the destinations of the same event fails the same way
		// again, so the event is rejected instead of retried. It must not
		// count as delivered either, which would lose it silently.
		s.logger.WarnContext(ctx, "Could not resolve destination, rejecting event", slog.Any("error", err))
		return fmt.Errorf("%w: could not resolve destination: %w", domain.ErrEventRejected, err)
	}
	if err = s.send(ctx, messages); err != nil {
		if errors.Is(err, contract.ErrMessageTooLarge) {
			// The same message is too large on every try, so retrying would
			// never end.
			s.logger.WarnContext(ctx, "Message too large for the broker, rejecting event", slog.Any("error", err))
			return fmt.Errorf("%w: %w", domain.ErrEventRejected, err)
		}
		return fmt.Errorf("failed to send event: %w", err)
	}
	return nil
}

// send encodes and publishes the messages one after another without waiting
// in between, so the broker stores the first ones while the rest are still
// being encoded, and then waits for every acknowledgement. A message that
// cannot be published stops the rest of the event; those already published
// stay.
func (s *Sink) send(ctx context.Context, messages []message) error {
	acks := make([]contract.Ack, 0, len(messages))
	var err error
	for _, m := range messages {
		_, body, marshalErr := m.event.MarshalOTLPData()
		if marshalErr != nil {
			err = fmt.Errorf("failed to marshal event: %w", marshalErr)
			break
		}
		ack, publishErr := s.publisher.Publish(ctx, contract.Message{Destination: m.destination, Body: body})
		if publishErr != nil {
			err = publishErr
			break
		}
		acks = append(acks, ack)
	}
	for _, ack := range acks {
		if ackErr := ack.Wait(ctx); ackErr != nil {
			err = errors.Join(err, ackErr)
			if ctx.Err() != nil {
				return err
			}
		}
	}
	return err
}

func (s *Sink) Stop() error {
	return s.publisher.Close()
}
