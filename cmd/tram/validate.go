// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/go-tram/tram/internal/adapters/sink"
	"github.com/go-tram/tram/internal/adapters/source"
	"github.com/go-tram/tram/internal/adapters/stage"
	"github.com/go-tram/tram/internal/core"
	"github.com/go-tram/tram/internal/testrun"
)

type validateOptions struct {
	secretsFile string
}

func newValidateCmd() *cobra.Command {
	var opts validateOptions
	cmd := &cobra.Command{
		Use:   "validate <file or directory>...",
		Short: "Check stream files without running them",
		Long: `Check stream files without running them: their format, the stream name,
that the stream is a tree, and the spec of every component.

Sources and sinks are checked without contacting anything: a broker
component checks its options but does not connect, an OTLP source opens no
port. Stages are built and stopped again, as in tram test, so an encryption
stage with the stackit-kms driver contacts the KMS. A directory is read like
the file sync reads it: its *.json files, not its subdirectories. Two files
with the same stream name are an error.

References like {"valueFrom": {"secretKeyRef": ...}} are resolved from the
secrets file, keyed "<name>/<key>"; without one, a reference fails the check.`,
		Example: `  tram validate config/
  tram validate stream.json --secrets secrets.json`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			files, err := streamFiles(args)
			if err != nil {
				return err
			}
			cmd.SilenceUsage = true
			return runValidate(cmd.OutOrStdout(), opts, files)
		},
	}
	cmd.Flags().StringVar(&opts.secretsFile, "secrets", "",
		"JSON file with secret values keyed \"<name>/<key>\" for references in component specs")
	return cmd
}

// streamFiles returns the files of paths: a file as it is, a directory's
// *.json files.
func streamFiles(paths []string) ([]string, error) {
	var files []string
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			files = append(files, path)
			continue
		}
		matches, err := filepath.Glob(filepath.Join(path, "*.json"))
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("%s holds no *.json files", path)
		}
		files = append(files, matches...)
	}
	slices.Sort(files)
	return slices.Compact(files), nil
}

func runValidate(out io.Writer, opts validateOptions, files []string) error {
	var secrets map[string]string
	if opts.secretsFile != "" {
		var err error
		if secrets, err = testrun.LoadSecrets(opts.secretsFile); err != nil {
			return err
		}
	}

	// Buffers open their queues in the data directory, so they get one of
	// their own that is removed afterwards.
	dataDir, err := os.MkdirTemp("", "tram-validate-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dataDir)

	logger := slog.New(slog.DiscardHandler)
	sources := source.NewFactory(logger, source.WithDataDir(dataDir))
	sinks := sink.NewFactory(logger, sink.WithDataDir(dataDir))
	checks := core.Checks{Source: sources.Check, Sink: sinks.Check}
	stages := stage.NewFactory(logger)

	invalid := 0
	byName := map[string]string{}
	for _, file := range files {
		name, fileErr := validateFile(file, secrets, checks, stages)
		if fileErr == nil {
			if other, ok := byName[name]; ok {
				fileErr = fmt.Errorf("stream %s is also defined in %s", name, other)
			}
			byName[name] = file
		}
		if fileErr != nil {
			invalid++
			_, _ = fmt.Fprintf(out, "FAIL %s\n%s\n", file, indent(fileErr.Error()))
			continue
		}
		_, _ = fmt.Fprintf(out, "ok   %s (%s)\n", file, name)
	}
	if invalid > 0 {
		return fmt.Errorf("%d of %d stream files are invalid", invalid, len(files))
	}
	return nil
}

func validateFile(
	file string,
	secrets map[string]string,
	checks core.Checks,
	stages *stage.Factory,
) (string, error) {
	spec, err := testrun.LoadStream(file)
	if err != nil {
		return "", err
	}
	prepared, err := testrun.PrepareAll(spec, secrets)
	if err != nil {
		return spec.Name, err
	}
	return spec.Name, core.ValidateStream(prepared, checks, stages)
}

func indent(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = "  " + line
	}
	return strings.Join(lines, "\n")
}
