// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/go-tram/tram/internal/adapters/k8scrd"
)

type managerOptions struct {
	kubeconfig              string
	watchNamespaces         []string
	leaderElect             bool
	leaderElectionNamespace string
	engineImage             string
	engineClusterRole       string
}

func newManagerCmd(logger *slog.Logger) *cobra.Command {
	var opts managerOptions
	cmd := &cobra.Command{
		Use:   "manager",
		Short: "Run the Kubernetes manager of TRAM Engines",
		Long:  "Run the Kubernetes manager of Tram engines",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runManager(cmd.Context(), logger, opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.kubeconfig, "kubeconfig", "", "Kubernetes config file")
	flags.StringSliceVar(&opts.watchNamespaces, "watch-namespace", nil,
		"Namespaces whose Stream resources the manager labels, comma-separated (default all)")
	flags.BoolVar(&opts.leaderElect, "leader-elect", true,
		"Let only one of several manager replicas work at a time")
	flags.StringVar(&opts.leaderElectionNamespace, "leader-election-namespace", "",
		"Namespace of the leader election lease (default the pod's namespace; required outside a cluster)")
	flags.StringVar(&opts.engineImage, "engine-image", "",
		"Tram image of the Engines that set no spec.image, usually the manager's own image")
	flags.StringVar(&opts.engineClusterRole, "engine-cluster-role", k8scrd.DefaultEngineClusterRole,
		"ClusterRole bound to the service account of every Engine in its namespace; "+
			"the manager needs the bind verb on it")
	return cmd
}

func runManager(ctx context.Context, logger *slog.Logger, opts managerOptions) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m, err := k8scrd.NewManager(k8scrd.ManagerOptions{
		Kubeconfig:              opts.kubeconfig,
		Namespaces:              opts.watchNamespaces,
		LeaderElection:          opts.leaderElect,
		LeaderElectionNamespace: opts.leaderElectionNamespace,
		EngineImage:             opts.engineImage,
		EngineClusterRole:       opts.engineClusterRole,
	}, logger)
	if err != nil {
		return err
	}
	return m.Run(ctx)
}
