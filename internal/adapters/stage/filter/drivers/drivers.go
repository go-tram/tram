// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package drivers is the one place that knows the concrete drivers of the
// filter stage. The filter stage creates a [Factory] to resolve the `driver`
// of its spec; its tests may inject another [driver.Factory] instead.
package drivers

import (
	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/filter/cel"
	"github.com/go-tram/tram/internal/adapters/stage/filter/rules"
)

// Factory returns the driver a filter spec names.
type Factory struct {
	validator *validator.Validate
}

var _ driver.Factory[rules.Driver] = (*Factory)(nil)

func NewFactory(validator *validator.Validate) *Factory {
	return &Factory{validator: validator}
}

// Driver returns the driver called name.
func (f *Factory) Driver(name string) (rules.Driver, error) {
	switch name {
	case cel.Name:
		return cel.New(f.validator), nil
	default:
		return nil, driver.Unknown(name, Names()...)
	}
}

// Names are the names of the drivers.
func Names() []string {
	return []string{cel.Name}
}
