// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package statements_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	st "github.com/go-tram/tram/internal/adapters/stage/transform/statements"
)

func TestParsePath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw  string
		want st.Path
		err  bool
	}{
		{raw: "attributes['user.id']", want: st.Path{Context: st.ContextEntry, Key: "user.id"}},
		{raw: `attributes["user.id"]`, want: st.Path{Context: st.ContextEntry, Key: "user.id"}},
		{raw: "scope.attributes['lib']", want: st.Path{Context: st.ContextScope, Key: "lib"}},
		{raw: "resource.attributes['service.name']", want: st.Path{Context: st.ContextResource, Key: "service.name"}},
		{raw: "body", err: true},
		{raw: "attributes['']", err: true},
		{raw: "attributes.user", err: true},
		{raw: "span.attributes['x']", err: true},
		{raw: " attributes['x']", err: true},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()
			got, err := st.ParsePath(tc.raw)
			if tc.err {
				require.ErrorIs(t, err, st.ErrInvalidPath)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func str(v string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
}

func kv(key, value string) *commonpb.KeyValue { return &commonpb.KeyValue{Key: key, Value: str(value)} }

func keys(attrs []*commonpb.KeyValue) map[string]string {
	out := map[string]string{}
	for _, a := range attrs {
		out[a.GetKey()] = a.GetValue().GetStringValue()
	}
	return out
}

func TestAttributeOperations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		op   func(*[]*commonpb.KeyValue)
		want map[string]string
	}{
		{name: "set adds", op: func(a *[]*commonpb.KeyValue) { st.Set(a, "env", str("prod")) },
			want: map[string]string{"user": "ann", "note": "a long note", "env": "prod"}},
		{name: "set replaces", op: func(a *[]*commonpb.KeyValue) { st.Set(a, "user", str("bob")) },
			want: map[string]string{"user": "bob", "note": "a long note"}},
		{name: "delete", op: func(a *[]*commonpb.KeyValue) { st.Delete(a, "user") },
			want: map[string]string{"note": "a long note"}},
		{name: "delete missing", op: func(a *[]*commonpb.KeyValue) { st.Delete(a, "nope") },
			want: map[string]string{"user": "ann", "note": "a long note"}},
		{name: "rename", op: func(a *[]*commonpb.KeyValue) { st.Rename(a, "user", "user.name") },
			want: map[string]string{"user.name": "ann", "note": "a long note"}},
		{
			name: "rename onto an existing key replaces it",
			op:   func(a *[]*commonpb.KeyValue) { st.Rename(a, "user", "note") },
			want: map[string]string{"note": "ann"},
		},
		{name: "rename missing", op: func(a *[]*commonpb.KeyValue) { st.Rename(a, "nope", "x") },
			want: map[string]string{"user": "ann", "note": "a long note"}},
		{name: "truncate", op: func(a *[]*commonpb.KeyValue) { st.Truncate(a, "note", 6) },
			want: map[string]string{"user": "ann", "note": "a long"}},
		{name: "truncate shorter value", op: func(a *[]*commonpb.KeyValue) { st.Truncate(a, "user", 6) },
			want: map[string]string{"user": "ann", "note": "a long note"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			attrs := []*commonpb.KeyValue{kv("user", "ann"), kv("note", "a long note")}
			tc.op(&attrs)
			assert.Equal(t, tc.want, keys(attrs))
		})
	}
}

func TestTruncateCountsCharacters(t *testing.T) {
	t.Parallel()
	attrs := []*commonpb.KeyValue{kv("city", "Düsseldorf")}
	st.Truncate(&attrs, "city", 2)
	assert.Equal(t, "Dü", attrs[0].GetValue().GetStringValue(), "multi-byte characters stay whole")

	ints := []*commonpb.KeyValue{
		{Key: "n", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 12345}}},
	}
	st.Truncate(&ints, "n", 2)
	assert.Equal(t, int64(12345), ints[0].GetValue().GetIntValue(), "only strings are truncated")
}
