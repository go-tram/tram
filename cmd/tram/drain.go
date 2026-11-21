// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"errors"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/go-tram/tram/internal/adapters/buffer/drivers"
	"github.com/go-tram/tram/internal/adapters/k8scrd"
	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
)

// drainPoll is how often drain checks whether it is done.
const drainPoll = 5 * time.Second

type drainOptions struct {
	kubeconfig      string
	watchNamespaces []string
	engine          string
	dataDir         string
}

func newDrainCmd(logger *slog.Logger) *cobra.Command {
	var opts drainOptions
	cmd := &cobra.Command{
		Use:   "drain",
		Short: "Deliver what the buffer queues of an Engine's volume hold, then exit",
		Long: `Drain runs only the Streams of an Engine that read from a buffer: their sources
deliver what the queue files in the data directory hold, to the sinks of those
streams. Every other Stream is treated as suspended, so nothing is written to
the volume any more. Drain exits once every queue file is empty or holds only
entries older than their max_age, so a volume without an instance can be
removed without losing data. tram manager runs it as a Job for such volumes.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDrain(cmd.Context(), logger, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.kubeconfig, "kubeconfig", "", "Kubernetes config file")
	flags.StringSliceVar(&opts.watchNamespaces, "watch-namespace", nil,
		"Namespaces whose Stream resources are drained, comma-separated (default all)")
	flags.StringVar(&opts.engine, "engine", "", "Engine whose Stream resources are drained")
	flags.StringVar(&opts.dataDir, "data-dir", "", "Directory holding the buffer queue files")
	_ = cmd.MarkFlagRequired("engine")
	_ = cmd.MarkFlagRequired("data-dir")
	return cmd
}

func runDrain(ctx context.Context, logger *slog.Logger, opts drainOptions) (err error) {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err = checkDataDir(opts.dataDir); err != nil {
		return err
	}

	c := core.New(ctx, logger,
		source.NewFactory(logger, source.WithDataDir(opts.dataDir)),
		stage.NewFactory(logger),
		sink.NewFactory(logger, sink.WithDataDir(opts.dataDir)))
	defer func() {
		err = errors.Join(err, c.Close())
	}()

	op, err := k8scrd.New(c, k8scrd.Options{
		Kubeconfig: opts.kubeconfig,
		Namespaces: opts.watchNamespaces,
		Engine:     opts.engine,
		HasDataDir: true,
		Drain:      true,
	}, logger)
	if err != nil {
		return err
	}
	opCtx, stopOperator := context.WithCancel(ctx)
	defer stopOperator()
	operatorDone := make(chan error, 1)
	go func() { operatorDone <- op.Run(opCtx) }()

	logger.InfoContext(
		ctx,
		"Draining buffer queues",
		slog.String("dir", opts.dataDir),
		slog.String("engine", opts.engine),
	)
	ticker := time.NewTicker(drainPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case opErr := <-operatorDone:
			return errors.Join(errors.New("operator stopped before the queues were drained"), opErr)
		case <-ticker.C:
		}
		pending, checkErr := drivers.Pending(ctx, opts.dataDir, logger)
		if checkErr != nil {
			return checkErr
		}
		if pending == 0 {
			logger.InfoContext(ctx, "Every buffer queue is empty or expired")
			return nil
		}
	}
}
