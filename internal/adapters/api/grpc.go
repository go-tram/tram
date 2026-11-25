// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

type streamServer struct {
	v1.UnimplementedStreamServiceServer

	core   ports.Core
	logger *slog.Logger
}

func (s streamServer) PutStream(ctx context.Context, req *v1.PutStreamRequest) (*v1.PutStreamResponse, error) {
	if err := protovalidate.Validate(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	streamSpec := pbStreamToStreamSpec(req.GetStream())

	if req.GetDryRun() {
		plan, err := s.core.PlanStream(ctx, streamSpec, streamSpec)
		if err != nil {
			return nil, s.statusOf(ctx, err)
		}
		return &v1.PutStreamResponse{
			Stream: streamSpecToPBStream(streamSpec),
			Plan:   streamPlanToPB(plan),
		}, nil
	}

	stream, err := s.core.UpsertStream(ctx, streamSpec)
	if err != nil {
		return nil, s.statusOf(ctx, err)
	}

	return &v1.PutStreamResponse{
		Stream: streamSpecToPBStream(*stream),
	}, nil
}
func (s streamServer) GetStream(ctx context.Context, req *v1.GetStreamRequest) (*v1.GetStreamResponse, error) {
	stream, err := s.core.GetStream(ctx, req.GetName())
	if err != nil {
		return nil, s.statusOf(ctx, err)
	}
	return &v1.GetStreamResponse{
		Stream: streamSpecToPBStream(*stream),
	}, nil
}

func (s streamServer) DeleteStream(ctx context.Context, req *v1.DeleteStreamRequest) (*v1.DeleteStreamResponse, error) {
	if err := s.core.DeleteStream(ctx, req.GetName()); err != nil {
		return nil, s.statusOf(ctx, err)
	}
	return &v1.DeleteStreamResponse{}, nil
}

func (s streamServer) ListStreams(ctx context.Context, _ *v1.ListStreamsRequest) (*v1.ListStreamsResponse, error) {
	streams, err := s.core.GetStreams(ctx)
	if err != nil {
		return nil, s.statusOf(ctx, err)
	}
	slices.SortFunc(streams, func(a, b domain.StreamSpec) int { return strings.Compare(a.Name, b.Name) })
	return &v1.ListStreamsResponse{Streams: mmap(streams, streamSpecToPBStream)}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrStreamNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrStreamBuildFailed):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrCoreClosed):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s streamServer) statusOf(ctx context.Context, err error) error {
	st := toStatus(err)
	if status.Code(st) == codes.Internal {
		s.logger.ErrorContext(ctx, "Could not handle API request", slog.Any("error", err))
	}
	return st
}
