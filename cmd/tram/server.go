// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/go-tram/tram/internal/adapters/api"
	"github.com/go-tram/tram/internal/adapters/configsync"
	"github.com/go-tram/tram/internal/adapters/k8scrd"
	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"

	"github.com/go-tram/tram/internal/core"
)

type serverOptions struct {
	grpcEndpoint      string
	httpEndpoint      string
	configDir         string
	enableK8sOperator bool
	kubeconfig        string
	watchNamespaces   []string
	engine            string
	enableFileSync    bool
	adminEndpoint     string
	pprofEndpoint     string
	dataDir           string
}

func newServerCmd(logger *slog.Logger) *cobra.Command {
	var opts serverOptions
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start the gRPC Gateway HTTP server",
		Long:  `Start the gRPC Gateway HTTP server that proxies requests to the gRPC backend.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServer(cmd.Context(), logger, opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.grpcEndpoint, "grpc-endpoint", "localhost:50051", "gRPC server endpoint")
	flags.StringVar(&opts.httpEndpoint, "http-endpoint", "localhost:8080", "HTTP server endpoint")
	flags.StringVarP(&opts.configDir, "config-dir", "c", "config", "Configuration directory")
	flags.BoolVar(&opts.enableK8sOperator, "enable-k8s-operator", false, "Enable the Kubernetes operator")
	flags.BoolVar(&opts.enableFileSync, "enable-file-sync", false, "Enable the file sync")
	flags.StringVar(&opts.kubeconfig, "kubeconfig", "", "Kubernetes config file")
	flags.StringSliceVar(&opts.watchNamespaces, "watch-namespace", nil,
		"Namespaces whose Stream resources the operator runs, comma-separated (default all)")
	flags.StringVar(&opts.engine, "engine", "", "Engine whose Stream resources the operator runs")
	flags.StringVar(&opts.adminEndpoint, "admin-endpoint", "localhost:50052",
		"Endpoint of the admin API (gRPC and HTTP), which shows the events of running streams, "+
			"e.g. to tram tap; empty disables it")
	flags.StringVar(&opts.pprofEndpoint, "pprof-endpoint", "",
		"Endpoint serving the Go profiles under /debug/pprof/, e.g. localhost:6060 (default disabled)")
	flags.StringVar(
		&opts.dataDir,
		"data-dir",
		"",
		"Directory the buffer sink and source keep their queues in, e.g. on a persistent volume (default none: streams with a buffer fail to build)",
	)
	return cmd
}

func runServer(ctx context.Context, logger *slog.Logger, opts serverOptions) (err error) {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metricsHandler, meterProvider, shutdownMetrics, err := setupMetrics()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, shutdownMetrics(context.WithoutCancel(ctx)))
	}()

	if err = checkDataDir(opts.dataDir); err != nil {
		return err
	}

	sourceFac := source.NewFactory(logger, source.WithDataDir(opts.dataDir))
	stageFac := stage.NewFactory(logger, stage.WithMeterProvider(meterProvider))
	sinkFac := sink.NewFactory(logger, sink.WithDataDir(opts.dataDir))

	c := core.New(ctx, logger, sourceFac, stageFac, sinkFac)
	defer func() {
		err = errors.Join(err, c.Close())
	}()

	a, err := api.New(c, opts.grpcEndpoint, opts.httpEndpoint, metricsHandler, logger)
	if err != nil {
		return err
	}

	eg, driversCtx := errgroup.WithContext(ctx)

	if opts.adminEndpoint != "" {
		eg.Go(func() error {
			return api.NewAdmin(opts.adminEndpoint, c, c, logger).Run(driversCtx)
		})
	}

	if opts.pprofEndpoint != "" {
		eg.Go(func() error {
			return runPprof(driversCtx, opts.pprofEndpoint, logger)
		})
	}

	if opts.enableFileSync {
		eg.Go(func() error {
			cs, syncErr := configsync.New(c, opts.configDir, logger)
			if syncErr != nil {
				return syncErr
			}
			return errors.Join(cs.Run(driversCtx), cs.Close())
		})
	}

	if opts.enableK8sOperator {
		eg.Go(func() error {
			op, opErr := k8scrd.New(c, k8scrd.Options{
				Kubeconfig: opts.kubeconfig,
				Namespaces: opts.watchNamespaces,
				Engine:     opts.engine,
				HasDataDir: opts.dataDir != "",
			}, logger)
			if opErr != nil {
				return opErr
			}
			return op.Run(driversCtx)
		})
	}

	eg.Go(func() error {
		return a.Run(driversCtx)
	})

	err = eg.Wait()
	logger.InfoContext(ctx, "Shutting down")
	return err
}

// checkDataDir fails early if the data directory is set but missing, instead
// of failing every stream with a buffer.
func checkDataDir(dir string) error {
	if dir == "" {
		return nil
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("data directory %s is not a directory", dir)
	}
	return nil
}
