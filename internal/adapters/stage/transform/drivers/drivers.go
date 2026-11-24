// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package drivers is the one place that knows the concrete drivers of the
// transform stage. The stage creates a [Factory] to resolve the `driver` of
// its spec; its tests may inject another [driver.Factory] instead.
package drivers

import (
	"github.com/go-tram/tram/internal/adapters/driver"
	"github.com/go-tram/tram/internal/adapters/stage/transform/cel"
	"github.com/go-tram/tram/internal/adapters/stage/transform/statements"
)

// Factory returns the driver a transform spec names.
type Factory struct{}

var _ driver.Factory[statements.Driver] = Factory{}

// Driver returns the driver called name.
func (Factory) Driver(name string) (statements.Driver, error) {
	switch name {
	case cel.Name:
		return cel.New(), nil
	default:
		return nil, driver.Unknown(name, Names()...)
	}
}

// Names are the names of the drivers.
func Names() []string {
	return []string{cel.Name}
}
