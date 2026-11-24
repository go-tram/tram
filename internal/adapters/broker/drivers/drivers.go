// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package drivers is the one place that knows the concrete broker drivers. The
// broker sink creates a [PublisherFactory] and the broker source a
// [SubscriberFactory] to resolve the `driver` of their spec; their tests inject
// another [driver.Factory] instead.
package drivers

import (
	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/broker/amqp"
	"github.com/go-tram/tram/internal/adapters/broker/kafka"
	"github.com/go-tram/tram/internal/adapters/broker/nats"
	"github.com/go-tram/tram/internal/adapters/driver"
)

// PublisherFactory returns the publishing side of the driver a spec names, for
// the broker sink.
type PublisherFactory struct {
	validator *validator.Validate
}

var _ driver.Factory[broker.PublisherDriver] = (*PublisherFactory)(nil)

func NewPublisherFactory(validator *validator.Validate) *PublisherFactory {
	return &PublisherFactory{validator: validator}
}

// Driver returns the publishing side of the driver called name.
func (f *PublisherFactory) Driver(name string) (broker.PublisherDriver, error) {
	return newDriver(f.validator, name)
}

// SubscriberFactory returns the subscribing side of the driver a spec names,
// for the broker source.
type SubscriberFactory struct {
	validator *validator.Validate
}

var _ driver.Factory[broker.SubscriberDriver] = (*SubscriberFactory)(nil)

func NewSubscriberFactory(validator *validator.Validate) *SubscriberFactory {
	return &SubscriberFactory{validator: validator}
}

// Driver returns the subscribing side of the driver called name.
func (f *SubscriberFactory) Driver(name string) (broker.SubscriberDriver, error) {
	return newDriver(f.validator, name)
}

// newDriver creates the driver called name. Drivers are stateless, so it creates
// one per call; a driver that holds state, such as shared connections, would be
// cached by the factories and closed by their owner.
func newDriver(validator *validator.Validate, name string) (broker.Driver, error) {
	switch name {
	case amqp.Name:
		return amqp.New(validator), nil
	case kafka.Name:
		return kafka.New(validator), nil
	case nats.Name:
		return nats.New(validator), nil
	default:
		return nil, driver.Unknown(name, Names()...)
	}
}

// Names are the names of the drivers.
func Names() []string {
	return []string{amqp.Name, kafka.Name, nats.Name}
}
