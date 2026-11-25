// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/go-tram/tram/internal/ports"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

// Admin serves the admin API, the API of the CLI: the StreamService, like the
// stream API, and the services that look into the running TRAM, today the
// TapService. It hands out the data of the streams, so it has an endpoint of
// its own, localhost by default, which is opened to others on the network
// level, e.g. with kubectl port-forward, and not together with the stream
// API.
//
// gRPC and its HTTP gateway share the endpoint: a request with a gRPC content
// type over HTTP/2 goes to gRPC, any other to the gateway.
type Admin struct {
	endpoint string
	core     ports.Core
	tapper   ports.Tapper
	logger   *slog.Logger
}

func NewAdmin(endpoint string, core ports.Core, tapper ports.Tapper, logger *slog.Logger) *Admin {
	return &Admin{endpoint: endpoint, core: core, tapper: tapper, logger: logger}
}

// Run serves the admin API until ctx ends.
func (a *Admin) Run(ctx context.Context) error {
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", a.endpoint)
	if err != nil {
		return fmt.Errorf("admin API failed to listen: %w", err)
	}
	return a.Serve(ctx, listener)
}

// Serve serves the admin API on listener until ctx ends.
func (a *Admin) Serve(ctx context.Context, listener net.Listener) error {
	grpcServer := grpc.NewServer()
	v1.RegisterStreamServiceServer(grpcServer, &streamServer{core: a.core, logger: a.logger})
	// Taps run until the client leaves, so they are ended when ctx ends.
	v1.RegisterTapServiceServer(grpcServer, &tapServer{tapper: a.tapper, logger: a.logger, shutdown: ctx})

	// The gateway calls the gRPC services through the endpoint itself: its
	// in-process calls do not support streaming.
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	endpoint := dialAddr(listener.Addr())
	for _, register := range []func(context.Context, *runtime.ServeMux, string, []grpc.DialOption) error{
		v1.RegisterStreamServiceHandlerFromEndpoint,
		v1.RegisterTapServiceHandlerFromEndpoint,
	} {
		if err := register(ctx, mux, endpoint, opts); err != nil {
			return errors.Join(fmt.Errorf("failed to register admin gateway handler: %w", err), listener.Close())
		}
	}

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
				grpcServer.ServeHTTP(w, r)
				return
			}
			mux.ServeHTTP(w, r)
		}),
		ReadHeaderTimeout: readHeaderTimeout,
		Protocols:         protocols,
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			a.logger.WarnContext(ctx, "Admin API calls did not finish in time", slog.Any("error", err))
			_ = server.Close()
		}
	}()

	a.logger.InfoContext(ctx, "Admin API listening", slog.String("address", listener.Addr().String()))
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("admin API failed: %w", err)
	}
	<-stopped
	return nil
}

// dialAddr is the address to reach addr, a listener's address, from the same
// host: a listener on all interfaces is reached on localhost.
func dialAddr(addr net.Addr) string {
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}
