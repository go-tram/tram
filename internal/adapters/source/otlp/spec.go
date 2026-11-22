// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

// Spec configures an OTLP source. It listens for OTLP/HTTP on HTTPPort, for
// OTLP/gRPC on GRPCPort, or for both; at least one port is required.
type Spec struct {
	HTTPPort        int    `json:"http_port,omitempty"        validate:"required_without=GRPCPort,omitempty,min=1,max=65535"`
	GRPCPort        int    `json:"grpc_port,omitempty"        validate:"required_without=HTTPPort,omitempty,min=1,max=65535,nefield=HTTPPort"`
	DeliveryTimeout string `json:"delivery_timeout,omitempty"`
	RetryAfter      string `json:"retry_after,omitempty"`
}
