// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package metrics is the naming contract of TRAM's metrics. Two layers
// record them: the core measures every node of a stream from outside, and
// components measure what only they see inside, like the lookup outcomes
// of the value map stage. Both build their metric names, attribute keys and
// kind values from here, so their series fit together on one dashboard.
//
// The name schema is
//
//	tram.<kind>[.<component type>].<metric>
//
// like tram.node.duration or tram.stage.value_map.mapped. Name builds it
// from the prefix and its parts. An OpenTelemetry reader spells the parts
// with underscores, tram_node_records_total.
package metrics

import (
	"strings"

	"github.com/go-tram/tram/internal/core/domain"
)

// Prefix is the namespace every metric name starts with.
const Prefix = "tram"

// Keys of the attributes every layer may attach to its metrics. The core
// uses Stream, Node, Kind and Signal on all of them; Outcome, Encoding and
// Body are specific to a metric, and components use Stream, Node and Signal.
const (
	LabelStream   = "stream"
	LabelNode     = "node"
	LabelKind     = "kind"
	LabelSignal   = "signal"
	LabelOutcome  = "outcome"
	LabelEncoding = "encoding"
	LabelBody     = "body"
)

// Values of the kind attribute: the kind of node the metric is about, bound
// to domain.NodeKind so they cannot differ from it.
const (
	KindSource = string(domain.KindSource)
	KindStage  = string(domain.KindStage)
	KindSink   = string(domain.KindSink)
)

// Name builds a metric name from the prefix and parts:
// Name("node", "records") is "tram.node.records".
func Name(parts ...string) string {
	return strings.Join(append([]string{Prefix}, parts...), ".")
}
