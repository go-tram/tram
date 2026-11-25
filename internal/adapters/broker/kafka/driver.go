// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package kafka is the broker driver for Apache Kafka and compatible brokers.
// Destinations are topics; they are set up outside TRAM.
package kafka

import (
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/core/domain"
)

// Name is the driver's name, the `driver` field of a broker spec and the key
// of its options.
const Name = "kafka"

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
		return nil, fmt.Errorf("kafka options: %w", err)
	}
	if err = v.Struct(spec); err != nil {
		return nil, fmt.Errorf("kafka options: %w", err)
	}
	return spec, nil
}

// connectionOptions are the client options of spec. name identifies the
// client in the broker's logs and quotas.
func connectionOptions(spec ConnectionSpec, name string) []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(spec.Brokers...), kgo.ClientID("tram " + name)}
	if spec.TLS != nil {
		//nolint:gosec // Skipping the verification is an explicit option for tests.
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: spec.TLS.InsecureSkipVerify,
		}))
	}
	if spec.SASL != nil {
		opts = append(opts, kgo.SASL(mechanism(spec.SASL)))
	}
	return opts
}

func mechanism(spec *SASLSpec) sasl.Mechanism {
	switch spec.Mechanism {
	case "scram-sha-256":
		return scram.Auth{User: spec.Username, Pass: spec.Password}.AsSha256Mechanism()
	case "scram-sha-512":
		return scram.Auth{User: spec.Username, Pass: spec.Password}.AsSha512Mechanism()
	default:
		return plain.Auth{User: spec.Username, Pass: spec.Password}.AsMechanism()
	}
}
