// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"context"
	"errors"
	"log/slog"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

type tapServer struct {
	v1.UnimplementedTapServiceServer

	tapper ports.Tapper
	logger *slog.Logger
	// shutdown ends when the API shuts down. Taps never end on their own, so
	// they are ended then instead of holding up the graceful stop.
	shutdown context.Context
}

func (s tapServer) TapStream(req *v1.TapStreamRequest, stream grpc.ServerStreamingServer[v1.TapStreamResponse]) error {
	if err := protovalidate.Validate(req); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	stop := context.AfterFunc(s.shutdown, cancel)
	defer stop()

	tap, err := s.tapper.Tap(ctx, req.GetName(), domain.TapOptions{
		Component: req.GetComponent(),
		Rate:      req.GetRate(),
	})
	if err != nil {
		return s.statusOf(ctx, err)
	}
	for {
		event, nextErr := tap.Next(ctx)
		if nextErr != nil {
			if s.shutdown.Err() != nil {
				return status.Error(codes.Unavailable, "the server is shutting down")
			}
			if ctx.Err() != nil {
				// The client is gone.
				return nil
			}
			return s.statusOf(ctx, nextErr)
		}
		if err = stream.Send(tapEventToPB(event)); err != nil {
			return err
		}
	}
}

func (s tapServer) statusOf(ctx context.Context, err error) error {
	var st error
	switch {
	case errors.Is(err, domain.ErrComponentNotFound):
		st = status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrInvalidTapOptions):
		st = status.Error(codes.InvalidArgument, err.Error())
	default:
		st = toStatus(err)
	}
	if status.Code(st) == codes.Internal {
		s.logger.ErrorContext(ctx, "Could not tap stream", slog.Any("error", err))
	}
	return st
}

func tapEventToPB(event domain.TapEvent) *v1.TapStreamResponse {
	resp := &v1.TapStreamResponse{
		Component: event.Component,
		Kind:      componentKindToPB(event.Kind),
		Time:      timestamppb.New(event.Time),
		Dropped:   event.Dropped,
	}
	if event.Event != nil {
		resp.Logs = event.Event.Logs
		resp.Metrics = event.Event.Metrics
		resp.Traces = event.Event.Traces
	}
	if event.Kind == domain.KindSink {
		resp.Sink = &v1.SinkResult{Duration: durationpb.New(event.SinkDuration)}
		if event.SinkErr != nil {
			resp.Sink.Error = event.SinkErr.Error()
		}
	}
	return resp
}

func componentKindToPB(kind domain.NodeKind) v1.ComponentKind {
	switch kind {
	case domain.KindSource:
		return v1.ComponentKind_COMPONENT_KIND_SOURCE
	case domain.KindStage:
		return v1.ComponentKind_COMPONENT_KIND_STAGE
	case domain.KindSink:
		return v1.ComponentKind_COMPONENT_KIND_SINK
	default:
		return v1.ComponentKind_COMPONENT_KIND_UNSPECIFIED
	}
}
