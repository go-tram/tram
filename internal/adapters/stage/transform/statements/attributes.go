// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package statements

import (
	"slices"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// The operations on attributes change the slice they are given, which has to
// be a copy the stage owns. A key appears at most once afterwards.

// Set sets key to value, replacing an existing value.
func Set(attributes *[]*commonpb.KeyValue, key string, value *commonpb.AnyValue) {
	if i := index(*attributes, key); i >= 0 {
		(*attributes)[i] = &commonpb.KeyValue{Key: key, Value: value}
		return
	}
	*attributes = append(*attributes, &commonpb.KeyValue{Key: key, Value: value})
}

// Delete removes key; a missing key is no error.
func Delete(attributes *[]*commonpb.KeyValue, key string) {
	*attributes = slices.DeleteFunc(*attributes, func(kv *commonpb.KeyValue) bool { return kv.GetKey() == key })
}

// Rename moves the value of key to the key to, replacing a value there. A
// missing key is no error and changes nothing.
func Rename(attributes *[]*commonpb.KeyValue, key, to string) {
	i := index(*attributes, key)
	if i < 0 || key == to {
		return
	}
	value := (*attributes)[i].GetValue()
	Delete(attributes, key)
	Set(attributes, to, value)
}

// Truncate shortens the string value of key to at most length characters.
// Values of other types and missing keys are left as they are.
func Truncate(attributes *[]*commonpb.KeyValue, key string, length int) {
	i := index(*attributes, key)
	if i < 0 {
		return
	}
	s, ok := (*attributes)[i].GetValue().GetValue().(*commonpb.AnyValue_StringValue)
	if !ok || utf8.RuneCountInString(s.StringValue) <= length {
		return
	}
	runes := []rune(s.StringValue)
	(*attributes)[i] = &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{StringValue: string(runes[:length])},
	}}
}

func index(attributes []*commonpb.KeyValue, key string) int {
	return slices.IndexFunc(attributes, func(kv *commonpb.KeyValue) bool { return kv.GetKey() == key })
}
