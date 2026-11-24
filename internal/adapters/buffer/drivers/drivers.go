// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package drivers is the one place that knows the concrete drivers of the
// buffer sink and source. They create a [Factory] to resolve the `driver` of
// their spec and to list the queue files of the data directory; their tests
// may inject another factory instead.
package drivers

import (
	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/adapters/buffer/sqlite"
	"github.com/go-tram/tram/internal/adapters/driver"
)

// Factory returns the driver a buffer spec names, keeping its queues in the
// data directory of the instance.
type Factory struct {
	validator *validator.Validate
	dataDir   string
}

// NewFactory returns the drivers for queue files in dataDir; without one, no
// buffer runs on the instance.
func NewFactory(validator *validator.Validate, dataDir string) *Factory {
	return &Factory{validator: validator, dataDir: dataDir}
}

// Driver returns the driver called name.
func (f *Factory) Driver(name string) (buffer.Driver, error) {
	switch name {
	case sqlite.Name:
		return sqlite.New(f.validator, f.dataDir), nil
	default:
		return nil, driver.Unknown(name, Names()...)
	}
}

// All returns every driver, e.g. to list all queue files of the data
// directory.
func (f *Factory) All() []buffer.Driver {
	return []buffer.Driver{sqlite.New(f.validator, f.dataDir)}
}

// Names are the names of the drivers.
func Names() []string {
	return []string{sqlite.Name}
}
