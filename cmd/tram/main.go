// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"log/slog"
	"os"

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
	if err := newRootCmd(logger, level).Execute(); err != nil {
		logger.Error("Tram stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

func newRootCmd(logger *slog.Logger, level *slog.LevelVar) *cobra.Command {
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
	root.AddCommand(newServerCmd(logger))
	root.AddCommand(newManagerCmd(logger))
	root.AddCommand(newTestCmd(logger))
	root.AddCommand(newDrainCmd(logger))
	root.AddCommand(newTapCmd())
	root.AddCommand(newValidateCmd())
	root.AddCommand(newListCmd())
	root.AddCommand(newContextCmd())
	return root
}
