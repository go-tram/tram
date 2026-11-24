// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package nats is the broker driver for NATS JetStream. Destinations are
// subjects; the streams that store them are set up outside TRAM.
package nats

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/core/domain"
)

// Name is the driver's name, the `driver` field of a broker spec and the key
// of its options.
const Name = "nats"

type Driver struct {
	validator *validator.Validate
}

var _ broker.Driver = (*Driver)(nil)

func New(validator *validator.Validate) *Driver {
	return &Driver{validator: validator}
}

func (d *Driver) NewPublisher(
	opts map[string]any,
	cfg broker.PublisherConfig,
	logger *slog.Logger,
) (broker.Publisher, error) {
	spec, err := parse(d.validator, opts, &PublisherSpec{})
	if err != nil {
		return nil, err
	}
	return newPublisher(spec, cfg, logger)
}

func (d *Driver) NewSubscriber(
	opts map[string]any,
	cfg broker.SubscriberConfig,
	logger *slog.Logger,
) (broker.Subscriber, error) {
	spec, err := parse(d.validator, opts, &SubscriberSpec{})
	if err != nil {
		return nil, err
	}
	return newSubscriber(spec, cfg, logger)
}

func (d *Driver) CheckPublisher(opts map[string]any) error {
	_, err := parse(d.validator, opts, &PublisherSpec{})
	return err
}

func (d *Driver) CheckSubscriber(opts map[string]any) error {
	_, err := parse(d.validator, opts, &SubscriberSpec{})
	return err
}

func parse[T any](v *validator.Validate, opts map[string]any, spec *T) (*T, error) {
	spec, err := domain.ParseSpec(opts, spec)
	if err != nil {
		return nil, fmt.Errorf("nats options: %w", err)
	}
	if err = v.Struct(spec); err != nil {
		return nil, fmt.Errorf("nats options: %w", err)
	}
	return spec, nil
}
