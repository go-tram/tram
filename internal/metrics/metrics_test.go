// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package metrics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/go-tram/tram/internal/metrics"
)

func TestName(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		parts []string
		want  string
	}{
		{parts: []string{"node", "records"}, want: "tram.node.records"},
		{parts: []string{"node", "records", "out"}, want: "tram.node.records.out"},
		{parts: []string{"source", "request", "size"}, want: "tram.source.request.size"},
		{parts: []string{"stage", "value_map", "mapped"}, want: "tram.stage.value_map.mapped"},
		{nil, "tram"},
		{parts: []string{"loopback"}, want: "tram.loopback"},
	} {
		assert.Equal(t, c.want, metrics.Name(c.parts...), "metric name of %v", c.parts)
	}
}

func TestContract(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "tram", metrics.Prefix, "the prefix never changes; dashboards depend on it")
	assert.Equal(t,
		[]string{metrics.LabelStream, metrics.LabelNode, metrics.LabelKind, metrics.LabelSignal,
			metrics.LabelOutcome, metrics.LabelEncoding, metrics.LabelBody},
		[]string{"stream", "node", "kind", "signal", "outcome", "encoding", "body"},
		"attribute keys never change; renaming one splits its series")
	assert.Equal(t,
		[]string{metrics.KindSource, metrics.KindStage, metrics.KindSink},
		[]string{"source", "stage", "sink"},
		"the kind values never change; a rename in domain breaks dashboards")
}
