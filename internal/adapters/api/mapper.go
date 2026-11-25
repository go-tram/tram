// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/go-tram/tram/internal/core/domain"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

func pbStreamToStreamSpec(req *v1.Stream) domain.StreamSpec {
	source := pbSourceToSourceSpec(req.GetSource())
	return domain.StreamSpec{
		Name:      req.GetName(),
		Source:    source,
		Stages:    mmap(req.GetStages(), pbStageToStageSpec),
		Sinks:     mmap(req.GetSinks(), pbSinkToSinkSpec),
		Metrics:   pbMetricsToMetricsSpec(req.GetMetrics()),
		Suspended: req.GetSuspended(),
	}
}

// The optional fields are pointers, so an unset field keeps its default.
func pbMetricsToMetricsSpec(req *v1.Metrics) domain.MetricsSpec {
	if req == nil {
		return domain.MetricsSpec{}
	}
	return domain.MetricsSpec{
		Records:     pbNodeMetricsToNodeMetrics(req.GetRecords()),
		Duration:    pbNodeMetricsToNodeMetrics(req.GetDuration()),
		RequestSize: req.RequestSize,
	}
}

func pbNodeMetricsToNodeMetrics(req *v1.NodeMetrics) domain.NodeMetrics {
	if req == nil {
		return domain.NodeMetrics{}
	}
	return domain.NodeMetrics{Source: req.Source, Stages: req.Stages, Sinks: req.Sinks}
}

func pbSourceToSourceSpec(req *v1.Source) domain.SourceSpec {
	return domain.SourceSpec{
		Name: req.GetName(),
		Type: req.GetType(),
		Spec: req.GetSpec().AsMap(),
	}
}

func pbStageToStageSpec(req *v1.Stage) domain.StageSpec {
	return domain.StageSpec{
		Name: req.GetName(),
		Type: req.GetType(),
		From: req.GetFrom(),
		Spec: req.GetSpec().AsMap(),
	}
}

func pbSinkToSinkSpec(req *v1.Sink) domain.SinkSpec {
	return domain.SinkSpec{
		Name: req.GetName(),
		Type: req.GetType(),
		From: req.GetFrom(),
		Spec: req.GetSpec().AsMap(),
	}
}

func streamSpecToPBStream(spec domain.StreamSpec) *v1.Stream {
	return &v1.Stream{
		Name:      spec.Name,
		Source:    sourceSpecToPBSource(spec.Source),
		Stages:    mmap(spec.Stages, stageSpecToPBStage),
		Sinks:     mmap(spec.Sinks, sinkSpecToPBSink),
		Metrics:   metricsSpecToPBMetrics(spec.Metrics),
		Suspended: &spec.Suspended,
	}
}

// Unset settings stay unset, so responses show what the stream sets.
func metricsSpecToPBMetrics(spec domain.MetricsSpec) *v1.Metrics {
	records := nodeMetricsToPBNodeMetrics(spec.Records)
	duration := nodeMetricsToPBNodeMetrics(spec.Duration)
	if records == nil && duration == nil && spec.RequestSize == nil {
		return nil
	}
	return &v1.Metrics{Records: records, Duration: duration, RequestSize: spec.RequestSize}
}

func nodeMetricsToPBNodeMetrics(spec domain.NodeMetrics) *v1.NodeMetrics {
	if spec.Source == nil && spec.Stages == nil && spec.Sinks == nil {
		return nil
	}
	return &v1.NodeMetrics{Source: spec.Source, Stages: spec.Stages, Sinks: spec.Sinks}
}

func sourceSpecToPBSource(spec domain.SourceSpec) *v1.Source {
	pbSpec, _ := structpb.NewValue(spec.Spec)
	return &v1.Source{
		Name: spec.Name,
		Type: spec.Type,
		Spec: pbSpec.GetStructValue(),
	}
}

func stageSpecToPBStage(spec domain.StageSpec) *v1.Stage {
	pbSpec, _ := structpb.NewValue(spec.Spec)
	return &v1.Stage{
		Name: spec.Name,
		Type: spec.Type,
		From: spec.From,
		Spec: pbSpec.GetStructValue(),
	}
}

func sinkSpecToPBSink(spec domain.SinkSpec) *v1.Sink {
	pbSpec, _ := structpb.NewValue(spec.Spec)
	return &v1.Sink{
		Name: spec.Name,
		Type: spec.Type,
		From: spec.From,
		Spec: pbSpec.GetStructValue(),
	}
}

func mmap[T, S any](in []T, f func(T) S) []S {
	out := make([]S, len(in))
	for i, v := range in {
		out[i] = f(v)
	}
	return out
}

func streamPlanToPB(plan *domain.StreamPlan) *v1.StreamPlan {
	if plan == nil {
		return nil
	}
	return &v1.StreamPlan{
		Action:         streamPlanActionToPB(plan.Action),
		SourceReplaced: plan.SourceReplaced,
		CreatedNodes:   plan.CreatedNodes,
		StoppedNodes:   plan.StoppedNodes,
	}
}

func streamPlanActionToPB(action domain.StreamPlanAction) v1.StreamPlanAction {
	switch action {
	case domain.StreamPlanActionNoop:
		return v1.StreamPlanAction_STREAM_PLAN_ACTION_NOOP
	case domain.StreamPlanActionHotSwap:
		return v1.StreamPlanAction_STREAM_PLAN_ACTION_HOT_SWAP
	case domain.StreamPlanActionRebuild:
		return v1.StreamPlanAction_STREAM_PLAN_ACTION_REBUILD
	default:
		return v1.StreamPlanAction_STREAM_PLAN_ACTION_UNSPECIFIED
	}
}

// StreamToPB is the stream of the API for spec, e.g. for a client.
func StreamToPB(spec domain.StreamSpec) *v1.Stream { return streamSpecToPBStream(spec) }

// StreamFromPB is the spec of the stream of the API.
func StreamFromPB(stream *v1.Stream) domain.StreamSpec { return pbStreamToStreamSpec(stream) }
