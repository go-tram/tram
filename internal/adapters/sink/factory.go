// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package sink

import (
	"fmt"
	"log/slog"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/sink/broker"
	"github.com/go-tram/tram/internal/adapters/sink/buffer"
	"github.com/go-tram/tram/internal/adapters/sink/debug"
	"github.com/go-tram/tram/internal/adapters/sink/noop"
	"github.com/go-tram/tram/internal/adapters/sink/otlp"
	"github.com/go-tram/tram/internal/core/domain"
)

// Factory creates the sinks by their type. Sinks with drivers resolve them
// themselves, each through its own drivers package.
type Factory struct {
	validator *validator.Validate
	logger    *slog.Logger
	dataDir   string
}

// Option configures the factory.
type Option func(*Factory)

// WithDataDir is the data directory of the instance, where buffer sinks keep
// their queues. Without it, buffer sinks fail to build.
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

// New creates the sink of spec. It ignores the stream it belongs to:
// sinks record no metrics of their own.
func (f *Factory) New(spec domain.SinkSpec, _ string) (domain.Sink, error) {
	switch spec.Type {
	case broker.Type:
		return broker.New(spec, f.validator, f.logger)
	case buffer.Type:
		return buffer.New(spec, f.validator, f.logger, f.dataDir)
	case noop.Type:
		return noop.New(spec, f.validator, f.logger)
	case debug.Type:
		return debug.New(spec, f.validator, f.logger)
	case otlp.Type:
		return otlp.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSinkType, spec.Type)
	}
}

// Check validates spec like New, without contacting external systems: a
// broker sink only checks its options, every other sink is created and
// stopped again. A buffer sink opens its queue in the factory's data
// directory, so check with a data directory of its own.
func (f *Factory) Check(spec domain.SinkSpec) error {
	if spec.Type == broker.Type {
		return broker.Check(spec, f.validator)
	}
	s, err := f.New(spec, "")
	if err != nil {
		return err
	}
	return s.Stop()
}
