// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines

import (
	"encoding/json"
	"time"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
)

const (
	// bufferType is the type of the buffer sink and source, which keep their
	// queues in the data directory of the instance.
	bufferType = "buffer"
	// defaultBufferMaxAge is the max_age of buffer sinks that set none.
	defaultBufferMaxAge = 24 * time.Hour
)

// bufferMaxAges returns the max_age of every buffer sink of a stream: the
// queue's retention is set by the sink that writes it. Only the field this
// needs is read from the sink's untyped spec; a spec the instances would
// refuse counts with the default.
func bufferMaxAges(stream *tramv1.Stream) []time.Duration {
	var ages []time.Duration
	for _, sink := range stream.Spec.Sinks {
		if sink.Type != bufferType {
			continue
		}
		age := defaultBufferMaxAge
		if sink.Spec != nil {
			var spec struct {
				MaxAge string `json:"max_age"`
			}
			if json.Unmarshal(sink.Spec.Raw, &spec) == nil && spec.MaxAge != "" {
				if parsed, err := time.ParseDuration(spec.MaxAge); err == nil && parsed > 0 {
					age = parsed
				}
			}
		}
		ages = append(ages, age)
	}
	return ages
}

// writesBuffer reports whether a stream is active and writes to a buffer: its
// writes would land on volumes that go away when the Engine loses its
// storage. A suspended stream writes nothing.
func writesBuffer(stream *tramv1.Stream) bool {
	suspended := stream.Spec.Suspended != nil && *stream.Spec.Suspended
	return !suspended && len(bufferMaxAges(stream)) > 0
}

// usesBuffer reports whether a stream writes to or reads from a buffer, so
// it needs the storage of its Engine.
func usesBuffer(stream *tramv1.Stream) bool {
	return stream.Spec.Source.Type == bufferType || len(bufferMaxAges(stream)) > 0
}

// maxBufferAge is the largest max_age of the buffer sinks of streams; the
// default if they have none, since a volume may hold queues of streams that
// left.
func maxBufferAge(streams []tramv1.Stream) time.Duration {
	largest := defaultBufferMaxAge
	for i := range streams {
		for _, age := range bufferMaxAges(&streams[i]) {
			largest = max(largest, age)
		}
	}
	return largest
}
