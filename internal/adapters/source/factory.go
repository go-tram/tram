// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package source

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/source/broker"
	"github.com/go-tram/tram/internal/adapters/source/buffer"
	"github.com/go-tram/tram/internal/adapters/source/generator"
	"github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/core/domain"
)

// Factory creates the sources by their type. Sources with drivers resolve
// them themselves, each through its own drivers package.
type Factory struct {
	validator *validator.Validate
	logger    *slog.Logger
	dataDir   string
}

// Option configures the factory.
type Option func(*Factory)

// WithDataDir is the data directory of the instance, where buffer sources
// read their queues from. Without it, buffer sources fail to build.
func WithDataDir(dir string) Option {
	return func(f *Factory) { f.dataDir = dir }
}

func NewFactory(logger *slog.Logger, opts ...Option) *Factory {
	f := &Factory{
		validator: validator.New(validator.WithRequiredStructEnabled()),
		logger:    logger,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// New creates the source of spec. It ignores the stream it belongs to:
// sources record no metrics of their own.
func (f *Factory) New(spec domain.SourceSpec, _ string) (domain.Source, error) {
	switch spec.Type {
	case broker.Type:
		return broker.New(spec, f.validator, f.logger)
	case buffer.Type:
		return buffer.New(spec, f.validator, f.logger, f.dataDir)
	case generator.Type:
		return generator.New(spec, f.validator)
	case otlp.Type:
		return otlp.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSourceType, spec.Type)
	}
}

// Check validates spec like New, without contacting external systems or
// opening ports: a broker source only checks its options, an OTLP source does
// not listen, every other source is created and stopped again. A buffer
// source opens its queue in the factory's data directory, so check with a
// data directory of its own.
func (f *Factory) Check(spec domain.SourceSpec) error {
	switch spec.Type {
	case broker.Type:
		return broker.Check(spec, f.validator)
	case otlp.Type:
		return otlp.Check(spec, f.validator, f.logger)
	}
	s, err := f.New(spec, "")
	if err != nil {
		return err
	}
	return s.Stop()
}
