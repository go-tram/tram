// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

import (
	"net/http"

	"time"

	"google.golang.org/grpc"

	"github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	MaxBodyBytes           = maxBodyBytes
	DefaultDeliveryTimeout = defaultDeliveryTimeout
	MaxPairwiseKeys        = maxPairwiseKeys
)

func DuplicateKey(event *domain.Event) (string, bool) {
	return duplicateKey(event)
}

func NewWithoutListener(src *base.Source[Spec], deliveryTimeout time.Duration) *OTLP {
	return &OTLP{
		Source:               src,
		deliveryTimeout:      deliveryTimeout,
		responseWriteTimeout: responseWriteTimeout,
		retryAfter:           retryAfterHeader(defaultRetryAfter),
	}
}

func RetryAfter(o *OTLP) string {
	return o.retryAfter
}

func SetResponseWriteTimeout(o *OTLP, timeout time.Duration) {
	o.responseWriteTimeout = timeout
}

func HandleLogs(o *OTLP, w http.ResponseWriter, r *http.Request) {
	o.handleLogs(w, r)
}

// NewGRPCServer is the gRPC server of o, for benchmarks that serve it on a
// listener of their own.
func NewGRPCServer(o *OTLP) *grpc.Server {
	return newGRPCServer(o)
}
