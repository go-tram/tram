// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package drivers is the one place that knows the concrete crypto drivers.
// The encryption stage resolves the `driver` of its spec with the factory
// this package provides; its tests can inject another one.
package drivers

import (
	"github.com/go-tram/tram/internal/adapters/crypto"
	"github.com/go-tram/tram/internal/adapters/crypto/local"
	"github.com/go-tram/tram/internal/adapters/crypto/stackitkms"
	"github.com/go-tram/tram/internal/adapters/driver"
)

// Factory resolves crypto drivers by name.
type Factory struct{}

var _ driver.Factory[crypto.Driver] = Factory{}

// NewFactory returns the factory of all crypto drivers.
func NewFactory() *Factory { return &Factory{} }

// Driver returns the driver called name.
func (Factory) Driver(name string) (crypto.Driver, error) {
	switch name {
	case local.Name:
		return local.New(), nil
	case stackitkms.Name:
		return stackitkms.New(), nil
	default:
		return nil, driver.Unknown(name, Names()...)
	}
}

// Names are the names of the drivers.
func Names() []string {
	return []string{local.Name, stackitkms.Name}
}
