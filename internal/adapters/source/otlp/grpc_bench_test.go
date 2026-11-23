// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"context"
	"net"
	"testing"

	"github.com/go-playground/validator/v10"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

// Exports logs over gRPC through an in-memory connection, so the numbers
// compare with BenchmarkHandleLogs: decoding and handling a request, here
// with gRPC's HTTP/2 framing and the client's encoding, without a network.
func BenchmarkGRPCExportLogs(b *testing.B) {
	for _, shape := range otlpgen.Shapes() {
		src, err := base.New[otlp.Spec]("otlp", map[string]any{"grpc_port": 1}, validator.New())
		if err != nil {
			b.Fatal(err)
		}
		_ = src.Subscribe(discardSubscriber{})
		server := otlp.NewGRPCServer(otlp.NewWithoutListener(src, otlp.DefaultDeliveryTimeout))
		listener := bufconn.Listen(1 << 20)
		go func() { _ = server.Serve(listener) }()
		b.Cleanup(server.Stop)

		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = conn.Close() })
		client := collogspb.NewLogsServiceClient(conn)
		request := &collogspb.ExportLogsServiceRequest{ResourceLogs: otlpgen.Logs(shape).GetResourceLogs()}

		b.Run(shape.String(), func(b *testing.B) {
			b.ReportAllocs()
			cpu := benchcpu.Start()
			for b.Loop() {
				if _, exportErr := client.Export(b.Context(), request); exportErr != nil {
					b.Fatal(exportErr)
				}
			}
			cpu.Report(b)
			b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
