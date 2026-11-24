// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// logo is printed by commands that are not written yet.
//
//nolint:unused // Used later.
const logo = `
                                                                                                   :::
                                                                                                    :::
                                                                                                      :::
                               :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::  .::::::::::::::::
                              :::                                                                     :::        :    ::    ::::
                            :::                                                                     :::           ::   ::      ::
                           :::                                                                      :              ::   ::      ::.
                         :::                                                                                        .:    :      :::
                        :::     ::::::::::::::  :::::::::.           ::         ::          :::                       :    :      :::
                      :::       :::::::::::::   ::::::::::::       :::::      :::::        ::::                        :    :      :::
                     :::             :::                  :::     ::: :::     ::::::      :::::                         :    :      :::
::::::::::::::::::::::               :::       ::::      ::::    :::  ::::    :::::::    ::::::                          :    ::     :::
                  ::::               :::       :::::::::::::    ::::   ::::   :::: :::  ::: :::                           :::::::     :::
                     :::             :::       ::::    ::::    :::: ::::::::  ::::  ::::::  :::                                   ::::::::
                      :::            :::       ::::     ::::  :::: :::::::::: ::::   ::::   :::                                         :::
                        :::          :::       ::::       :::::::         ::::::::          :::                                          .::
                         :::                                                                                                               ::
                           :::                                                                      :        :::::::::::::::::::::::::::::::::
                            :::                                                                     :::                                   :::
                              :::                                                                     :::                                :::
                               .::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::  .::::::::::::::::::::::::::::::
                                                                                                      :::
                                                                                                    :::
                                                                                                   :::
`

func main() {
	level := &slog.LevelVar{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if err := newRootCmdFor(logger, level, isPlugin(os.Args[0])).Execute(); err != nil {
		logger.Error("Tram stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

// pluginPrefix starts the name of a kubectl plugin: kubectl runs
// kubectl-tram for kubectl tram.
const pluginPrefix = "kubectl-"

// isPlugin reports whether the binary runs as a kubectl plugin, by its name.
func isPlugin(arg0 string) bool {
	return strings.HasPrefix(filepath.Base(arg0), pluginPrefix)
}

func newRootCmd(logger *slog.Logger, level *slog.LevelVar) *cobra.Command {
	return newRootCmdFor(logger, level, false)
}

// newRootCmdFor is the root command; as a kubectl plugin, it has only the
// commands of the CLI, and Kubernetes is their default target.
func newRootCmdFor(logger *slog.Logger, level *slog.LevelVar, plugin bool) *cobra.Command {
	var logLevel string
	root := &cobra.Command{
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if err := level.UnmarshalText([]byte(logLevel)); err != nil {
				return fmt.Errorf("invalid --log-level %q: use debug, info, warn or error", logLevel)
			}
			return nil
		},
		Use:   "tram",
		Short: "Tram is a simple observability pipeline tool",
		Long: `Tram is a simple observability pipeline tool that allows you to
collect, process, and forward logs, metrics, and traces to various
backends.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	root.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn or error")
	if plugin {
		root.Annotations = map[string]string{cobra.CommandDisplayNameAnnotation: "kubectl tram"}
		// kubectl completes plugins through kubectl_complete-tram, which
		// calls the hidden __complete; a completion command would not work.
		root.CompletionOptions.DisableDefaultCmd = true
		root.Short = "Tap, check and inspect the TRAM streams of a Kubernetes cluster"
		root.Long = `Tap, check and inspect the TRAM streams of a Kubernetes cluster.

The commands talk to the TRAM pods that run a stream, with the kubeconfig and
the flags of kubectl, e.g. --context and -n. A stream of a Stream resource
is named <namespace>.<name>.`
	} else {
		root.AddCommand(newServerCmd(logger))
		root.AddCommand(newManagerCmd(logger))
		root.AddCommand(newDrainCmd(logger))
	}
	root.AddCommand(newTestCmd(logger))
	root.AddCommand(newTapCmd(plugin))
	root.AddCommand(newValidateCmd())
	root.AddCommand(newListCmd())
	root.AddCommand(newStreamCmd(plugin))
	return root
}
