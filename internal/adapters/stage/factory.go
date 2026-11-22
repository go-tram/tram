// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package stage

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/go-tram/tram/internal/adapters/stage/batch"
	"github.com/go-tram/tram/internal/adapters/stage/decryption"
	"github.com/go-tram/tram/internal/adapters/stage/delay"
	"github.com/go-tram/tram/internal/adapters/stage/encryption"
	"github.com/go-tram/tram/internal/adapters/stage/filter"
	"github.com/go-tram/tram/internal/adapters/stage/noop"
	"github.com/go-tram/tram/internal/adapters/stage/transform"
	"github.com/go-tram/tram/internal/adapters/stage/validation"
	"github.com/go-tram/tram/internal/adapters/stage/valuemap"
	"github.com/go-tram/tram/internal/core/domain"
)

// Factory creates the stages by their type. Stages with drivers resolve them
// themselves, each through its own drivers package.
type Factory struct {
	validator *validator.Validate
	logger    *slog.Logger
	provider  metric.MeterProvider
}

// Option configures the factory.
type Option func(*Factory)

// WithMeterProvider is the provider the stages that record metrics of their
// own create their meters from, like the lookup outcomes of the value map
// stage. Without it, the globally configured provider serves them.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(f *Factory) { f.provider = provider }
}

func NewFactory(logger *slog.Logger, opts ...Option) *Factory {
	f := &Factory{
		validator: validator.New(validator.WithRequiredStructEnabled()),
		logger:    logger,
		provider:  otel.GetMeterProvider(),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (f *Factory) New(spec domain.StageSpec, stream string) (domain.Stage, error) {
	switch spec.Type {
	case noop.Type:
		return noop.New(spec, f.validator, f.logger)
	case validation.Type:
		return validation.New(spec, f.validator, f.logger)
	case filter.Type:
		return filter.New(spec, f.validator, f.logger)
	case decryption.Type:
		return decryption.New(spec, f.validator, f.logger)
	case delay.Type:
		return delay.New(spec, f.validator, f.logger)
	case encryption.Type:
		return encryption.New(spec, f.validator, f.logger)
	case batch.Type:
		return batch.New(spec, f.validator, f.logger)
	case valuemap.Type:
		return valuemap.New(spec, f.validator, f.logger, f.provider, stream)
	case transform.Type:
		return transform.New(spec, f.validator, f.logger, f.provider, stream)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidStageType, spec.Type)
	}
}
