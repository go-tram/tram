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
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/go-tram/tram/internal/core/domain"
)

// A transform stage changes the records of its branch only: the sibling sink
// below the same source receives them as they were sent.
func TestTransformChangesOnlyItsBranch(t *testing.T) {
	t.Parallel()
	p := newPipeline(t,
		[]domain.StageSpec{{Name: "normalize", Type: "transform", From: "in", Spec: map[string]any{
			"driver": "cel",
			"cel": map[string]any{"logs": []any{
				map[string]any{"set": "resource.attributes['env']",
					"value": "attr(resource.attributes, 'k8s.namespace.name').split('-')[1]"},
				map[string]any{"delete": "attributes['password']"},
				map[string]any{"rename": "attributes['user']", "to": "user.name"},
			}},
		}}},
		[]domain.SinkSpec{captureSinkSpec("normalized", "normalize"), captureSinkSpec("raw", "in")},
	)
	logs := &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("k8s.namespace.name", "shop-prod")}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Attributes: []*commonpb.KeyValue{stringAttr("user", "ann"), stringAttr("password", "s3cret")},
		}}}},
	}}}

	require.Equal(t, 200, p.post(t, logs, 10*time.Second))

	normalized := p.sinks.sink(t, "normalized").received()
	require.Len(t, normalized, 1)
	rl := normalized[0].Logs.GetResourceLogs()[0]
	assert.Equal(t, map[string]string{"k8s.namespace.name": "shop-prod", "env": "prod"},
		stringAttrs(rl.GetResource().GetAttributes()))
	assert.Equal(t, map[string]string{"user.name": "ann"},
		stringAttrs(rl.GetScopeLogs()[0].GetLogRecords()[0].GetAttributes()))

	raw := p.sinks.sink(t, "raw").received()
	require.Len(t, raw, 1)
	assert.Equal(t, map[string]string{"user": "ann", "password": "s3cret"},
		stringAttrs(raw[0].Logs.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()[0].GetAttributes()),
		"the sibling branch gets the records as they were sent")
}

// With on_error reject, a failed statement answers the client 422.
func TestTransformRejectsWithOnErrorReject(t *testing.T) {
	t.Parallel()
	p := newPipeline(t,
		[]domain.StageSpec{{Name: "strict", Type: "transform", From: "in", Spec: map[string]any{
			"driver":   "cel",
			"on_error": "reject",
			"cel": map[string]any{"logs": []any{
				map[string]any{"set": "attributes['team.id']", "value": "attr(record.attributes, 'team')"},
			}},
		}}},
		[]domain.SinkSpec{captureSinkSpec("out", "strict")},
	)

	status := p.post(t, request("checkout", rec{id: "no team", customer: "A"}), 10*time.Second)

	assert.Equal(t, 422, status)
	assert.Empty(t, p.sinks.sink(t, "out").received())
}

func stringAttrs(list []*commonpb.KeyValue) map[string]string {
	out := map[string]string{}
	for _, a := range list {
		out[a.GetKey()] = a.GetValue().GetStringValue()
	}
	return out
}
