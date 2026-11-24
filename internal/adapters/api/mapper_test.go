// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/api"
	"github.com/go-tram/tram/internal/core/domain"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

func TestMapperMapsMetrics(t *testing.T) {
	t.Parallel()

	stream := &v1.Stream{
		Name:   "s",
		Source: &v1.Source{Name: "in", Type: "otlp"},
		Metrics: &v1.Metrics{
			Records:     &v1.NodeMetrics{Stages: new(true)},
			Duration:    &v1.NodeMetrics{Sinks: new(false)},
			RequestSize: new(false),
		},
	}

	spec := api.PBStreamToStreamSpec(stream)
	assert.Equal(t, domain.MetricsSpec{
		Records:     domain.NodeMetrics{Stages: new(true)},
		Duration:    domain.NodeMetrics{Sinks: new(false)},
		RequestSize: new(false),
	}, spec.Metrics)
	assert.True(t, proto.Equal(stream.GetMetrics(), api.StreamSpecToPBStream(spec).GetMetrics()),
		"a response shows the metrics the stream sets")
}

func TestMapperLeavesOutDefaultMetrics(t *testing.T) {
	t.Parallel()

	spec := api.PBStreamToStreamSpec(&v1.Stream{Name: "s", Source: &v1.Source{Name: "in", Type: "otlp"}})
	assert.Equal(t, domain.MetricsSpec{}, spec.Metrics)
	assert.Nil(t, api.StreamSpecToPBStream(spec).GetMetrics())
}

func TestMapperMapsSuspended(t *testing.T) {
	t.Parallel()

	stream := &v1.Stream{
		Name:      "s",
		Source:    &v1.Source{Name: "in", Type: "otlp"},
		Suspended: new(true),
	}

	spec := api.PBStreamToStreamSpec(stream)
	assert.True(t, spec.Suspended)
	assert.True(t, api.StreamSpecToPBStream(spec).GetSuspended(),
		"a response shows the stream as suspended")

	spec = api.PBStreamToStreamSpec(&v1.Stream{Name: "s", Source: &v1.Source{Name: "in", Type: "otlp"}})
	assert.False(t, spec.Suspended)
	assert.False(t, api.StreamSpecToPBStream(spec).GetSuspended())
}
