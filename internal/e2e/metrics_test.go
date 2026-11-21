// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/go-tram/tram/internal/core/domain"
	metricnames "github.com/go-tram/tram/internal/metrics"
)

// TestMetricsFollowTheNamingContract sends one event through a pipeline whose
// value map stage records metrics of its own next to the core's node metrics,
// and asserts every instrument the instance exported conforms to the naming
// contract in internal/metrics: the name schema, the label keys, a set stream
// label, and a description and unit. A component that breaks the contract
// fails this test instead of a dashboard.
func TestMetricsFollowTheNamingContract(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	// The global provider carries the test's reader; tests that start later
	// record into it too, and everything they record has to conform as well.
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	p := newPipeline(t,
		[]domain.StageSpec{{
			Name: "mapper",
			Type: "value_map",
			From: "in",
			Spec: map[string]any{
				"source_attr": "team",
				"target_attr": "team.id",
				"default":     "unknown",
				"table":       map[string]any{"team-0": "cart"},
			},
		}},
		[]domain.SinkSpec{{Name: "out", Type: captureType, From: "mapper"}},
	)

	require.Equal(t, 200, p.post(t, teamRequest(), 10*time.Second))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.NotEmpty(t, rm.ScopeMetrics, "nothing was measured")

	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			assert.Regexpf(t, `^tram\.(node|source|stage|sink)(\.[a-z0-9_]+)+$`,
				m.Name, "%s: metric names follow the schema tram.<kind>[.<type>].<metric>", m.Name)
			assert.NotEmptyf(t, m.Description, "%s: every metric describes itself", m.Name)
			assert.NotEmptyf(t, m.Unit, "%s: every metric names its unit", m.Name)
			names = append(names, m.Name)

			for _, set := range attributeSets(t, m.Data) {
				labels := map[string]string{}
				for it := set.Iter(); it.Next(); {
					kv := it.Attribute()
					labels[string(kv.Key)] = kv.Value.String()
				}
				for key := range labels {
					assert.Containsf(t, allowedLabelKeys(), key,
						"%s: every label key comes from the naming contract", m.Name)
				}
				assert.Equalf(t, "e2e", labels[metricnames.LabelStream],
					"%s: every series names its stream", m.Name)
			}
		}
	}

	// The core measured the pipeline and the stage measured its lookups;
	// a test that sees only one of the two layers proves nothing.
	assert.Contains(t, names, "tram.node.records")
	assert.Contains(t, names, "tram.stage.value_map.mapped")
}

// teamRequest is one log record whose team attribute the value map maps.
func teamRequest() *logspb.LogsData {
	sl := &logspb.ScopeLogs{Scope: nil}
	sl.LogRecords = []*logspb.LogRecord{{
		Attributes: []*commonpb.KeyValue{stringAttr("team", "team-0")},
	}}
	return &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{sl}}}}
}

func allowedLabelKeys() []string {
	return []string{
		metricnames.LabelStream,
		metricnames.LabelNode,
		metricnames.LabelKind,
		metricnames.LabelSignal,
		metricnames.LabelOutcome,
		metricnames.LabelEncoding,
		metricnames.LabelBody,
	}
}

// attributeSets returns the attribute sets of an aggregation's data points.
func attributeSets(t *testing.T, data metricdata.Aggregation) []attribute.Set {
	t.Helper()
	switch data := data.(type) {
	case metricdata.Sum[int64]:
		sets := make([]attribute.Set, 0, len(data.DataPoints))
		for _, dp := range data.DataPoints {
			sets = append(sets, dp.Attributes)
		}
		return sets
	case metricdata.Histogram[int64]:
		sets := make([]attribute.Set, 0, len(data.DataPoints))
		for _, dp := range data.DataPoints {
			sets = append(sets, dp.Attributes)
		}
		return sets
	case metricdata.Histogram[float64]:
		sets := make([]attribute.Set, 0, len(data.DataPoints))
		for _, dp := range data.DataPoints {
			sets = append(sets, dp.Attributes)
		}
		return sets
	default:
		require.Failf(t, "unexpected aggregation", "%T", data)
		return nil
	}
}
