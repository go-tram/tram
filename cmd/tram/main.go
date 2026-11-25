// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

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

	// Ctrl-C ends a command cleanly, e.g. a tap with its port forwards; a
	// second one ends the process right away.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	context.AfterFunc(ctx, stop)

	cmd, err := newRootCmdFor(logger, level, isPlugin(os.Args[0])).ExecuteContextC(ctx)
	stop()
	if err != nil {
		reportError(os.Stderr, cmd, err, logger)
		os.Exit(1)
	}
}

// reportError reports the error of cmd once: in the log for the commands
// that run TRAM, whose output is a log, and as a plain message for the
// others.
func reportError(out io.Writer, cmd *cobra.Command, err error, logger *slog.Logger) {
	if cmd != nil && cmd.Annotations[logsAnnotation] == "true" {
		logger.Error("TRAM stopped with an error", slog.Any("error", err))
		return
	}
	_, _ = fmt.Fprintln(out, "Error:", err)
}

// logsAnnotation marks the commands that run TRAM and report in their log.
const logsAnnotation = "tram.io/logs"

// The groups of the commands in the help.
const (
	groupRun     = "run"
	groupStreams = "streams"
	groupFiles   = "files"
)

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
		Short: "TRAM receives, processes and forwards OpenTelemetry logs, metrics and traces",
		Long: `TRAM receives, processes and forwards OpenTelemetry logs, metrics and traces
in streams: a source, stages and sinks, configured through an API, files or
Kubernetes resources.

The binary runs TRAM, and is the CLI to work with its streams: tap and
manage the streams of a running instance through its admin API, or check
stream files without one.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
		// main reports errors itself, once.
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn or error")
	if !plugin {
		root.AddGroup(&cobra.Group{ID: groupRun, Title: "Run TRAM:"})
	}
	root.AddGroup(
		&cobra.Group{ID: groupStreams, Title: "Work with the streams of a running instance:"},
		&cobra.Group{ID: groupFiles, Title: "Work with stream files:"},
	)
	if plugin {
		root.Annotations = map[string]string{cobra.CommandDisplayNameAnnotation: "kubectl tram"}
		// kubectl completes plugins through kubectl_complete-tram, which
		// calls the hidden __complete; a completion command would not work.
		root.CompletionOptions.DisableDefaultCmd = true
		root.Short = "Tap, check and inspect the TRAM streams of a Kubernetes cluster"
		root.Long = `Tap, check and inspect the TRAM streams of a Kubernetes cluster.

The commands talk to the TRAM pods that run a stream, with the kubeconfig and
the flags of kubectl, e.g. --context and -n. The stream of a Stream resource
is its name, in the namespace of -n, or <namespace>.<name>.`
	} else {
		for _, cmd := range []*cobra.Command{newServerCmd(logger), newManagerCmd(logger), newDrainCmd(logger)} {
			cmd.GroupID = groupRun
			if cmd.Annotations == nil {
				cmd.Annotations = map[string]string{}
			}
			cmd.Annotations[logsAnnotation] = "true"
			root.AddCommand(cmd)
		}
	}
	for _, cmd := range []*cobra.Command{newTapCmd(plugin), newStreamCmd(plugin)} {
		cmd.GroupID = groupStreams
		root.AddCommand(cmd)
	}
	for _, cmd := range []*cobra.Command{newValidateCmd(), newTestCmd(logger), newComponentsCmd()} {
		cmd.GroupID = groupFiles
		root.AddCommand(cmd)
	}
	if !plugin {
		root.AddCommand(newReferenceCmd())
	}
	return root
}
