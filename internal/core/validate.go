// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

// Checks validate the specs of sources and sinks without contacting external
// systems or opening ports.
type Checks struct {
	Source func(domain.SourceSpec) error
	Sink   func(domain.SinkSpec) error
}

// ValidateStream validates spec without running it: its name, that it is a
// tree, and the spec of every component. Sources and sinks are checked with
// checks; stages are built with stageFactory and stopped again, as in a test
// run. Every invalid component is reported, not only the first.
func ValidateStream(
	spec domain.StreamSpec,
	checks Checks,
	stageFactory ports.Factory[domain.StageSpec, domain.Stage],
) error {
	if err := domain.ValidateStreamName(spec.Name); err != nil {
		return err
	}
	if _, err := newTree(spec); err != nil {
		return err
	}

	var errs []error
	if err := checks.Source(spec.Source); err != nil {
		errs = append(errs, invalidSpec(spec.Source.Name, err))
	}
	for _, stageSpec := range spec.Stages {
		stage, err := stageFactory.New(stageSpec, spec.Name)
		if err == nil {
			err = stage.Stop()
		}
		if err != nil {
			errs = append(errs, invalidSpec(stageSpec.Name, err))
		}
	}
	for _, sinkSpec := range spec.Sinks {
		if err := checks.Sink(sinkSpec); err != nil {
			errs = append(errs, invalidSpec(sinkSpec.Name, err))
		}
	}
	return errors.Join(errs...)
}
