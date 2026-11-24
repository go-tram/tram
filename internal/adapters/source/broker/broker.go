// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package broker is the broker source: it receives OTLP data from a message
// broker through the driver its spec names. Every message is one serialized
// OTLP payload of the signal the spec names.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-playground/validator/v10"

	contract "github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/broker/consume"
	"github.com/go-tram/tram/internal/adapters/broker/drivers"
	"github.com/go-tram/tram/internal/adapters/broker/partition"
	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	Type = "broker"

	redeliveryDelay = 5 * time.Second
)

var ErrInvalidPartition = errors.New("invalid partition")

type Source struct {
	*base.Source[Spec]

	consumer *consume.Consumer
}

func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*Source, error) {
	return newSource(source, validator, logger, drivers.NewSubscriberFactory(validator))
}

// Check validates source like New, without connecting to the broker.
func Check(source domain.SourceSpec, validator *validator.Validate) error {
	generic, opts, drv, err := driver.Split(source.Spec, drivers.NewSubscriberFactory(validator))
	if err != nil {
		return err
	}
	b, err := base.New[Spec](source.Name, generic, validator)
	if err != nil {
		return err
	}
	if opts, err = expandPartition(b.Spec(), opts); err != nil {
		return err
	}
	return drv.CheckSubscriber(opts)
}

// newSource is [New] with the drivers to resolve the spec's `driver` from, so
// tests can inject the in-memory driver; see export_test.go.
func newSource(
	source domain.SourceSpec,
	validator *validator.Validate,
	logger *slog.Logger,
	driverFactory driver.Factory[contract.SubscriberDriver],
) (*Source, error) {
	generic, opts, drv, err := driver.Split(source.Spec, driverFactory)
	if err != nil {
		return nil, err
	}
	b, err := base.New[Spec](source.Name, generic, validator)
	if err != nil {
		return nil, err
	}
	if opts, err = expandPartition(b.Spec(), opts); err != nil {
		return nil, err
	}
	logger = logger.With(slog.String("source", source.Name))
	subscriber, err := drv.NewSubscriber(opts, contract.SubscriberConfig{
		Name:     "source " + source.Name,
		Capacity: workersOf(b.Spec()),
	}, logger)
	if err != nil {
		return nil, err
	}
	s := &Source{Source: b}
	s.consumer = consume.New(consume.Config{
		Subscriber: subscriber,
		Workers:    workersOf(b.Spec()),
		Signal:     domain.Signal(b.Spec().Signal),
		Notify:     s.NotifySubscribers,
		RetryDelay: func(contract.Delivery) time.Duration { return redeliveryDelay },
		Logger:     logger,
	})
	return s, nil
}

// expandPartition replaces {partition} in the driver's options with the
// partition of the key. Without partitioning the placeholder must not appear,
// and with it, it must: otherwise every key would read the same data.
func expandPartition(spec *Spec, opts map[string]any) (map[string]any, error) {
	p := spec.Partition
	part := 0
	if p != nil {
		part = partition.Of(p.Key, p.Count)
	}
	expanded, found := partition.ExpandAll(opts, part)
	switch {
	case p == nil && found:
		return nil, fmt.Errorf("%w: the driver's options contain %s, but partition is not set",
			ErrInvalidPartition, partition.Placeholder)
	case p != nil && !found:
		return nil, fmt.Errorf("%w: the driver's options need %s to read the key's partition",
			ErrInvalidPartition, partition.Placeholder)
	case p == nil:
		return opts, nil
	}
	out, _ := expanded.(map[string]any)
	return out, nil
}

func workersOf(spec *Spec) int {
	return int(max(spec.Workers, 1))
}

// Run consumes until Stop. Deliveries use ctx, not the stop context, so
// messages being handled when the source stops are still acknowledged.
func (s *Source) Run(ctx context.Context) error { return s.consumer.Run(ctx) }

func (s *Source) Stop() error { return s.consumer.Stop() }
