// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

const configEnv = "TRAM_CONFIG"

// config is the tram config file: the contexts the CLI can talk to.
type config struct {
	CurrentContext string                 `json:"current-context,omitempty"`
	Contexts       map[string]contextSpec `json:"contexts,omitempty"`
}

// contextSpec is a target: the address of an admin API, or TRAM pods in
// Kubernetes.
type contextSpec struct {
	AdminEndpoint string      `json:"admin-endpoint,omitempty"`
	Kubernetes    *kubeTarget `json:"kubernetes,omitempty"`
}

func (c contextSpec) String() string {
	if c.Kubernetes != nil {
		return c.Kubernetes.String()
	}
	return c.AdminEndpoint
}

// configPath is $TRAM_CONFIG, or tram/config.yaml in the user's config
// directory, e.g. ~/.config/tram/config.yaml.
func configPath() string {
	if path := os.Getenv(configEnv); path != "" {
		return path
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return filepath.Join(".tram", "config.yaml")
	}
	return filepath.Join(dir, "tram", "config.yaml")
}

// loadConfig reads the config file; a missing file is an empty config.
func loadConfig() (config, error) {
	data, err := os.ReadFile(configPath())
	if errors.Is(err, fs.ErrNotExist) {
		return config{}, nil
	}
	if err != nil {
		return config{}, err
	}
	var cfg config
	if err = yaml.UnmarshalStrict(data, &cfg); err != nil {
		return config{}, fmt.Errorf("invalid config %s: %w", configPath(), err)
	}
	return cfg, nil
}

func saveConfig(cfg config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	path := configPath()
	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func newContextCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "context",
		Short: "Manage the contexts the CLI talks to",
		Long: `Manage the contexts of the tram config: named targets for the commands that
talk to a server, like tram tap and tram stream. A context is the address of
an admin API or TRAM pods in Kubernetes, an Engine or a label selector.

The config is ` + "`$TRAM_CONFIG`" + `, or tram/config.yaml in the user's config directory.
A command uses --context, then $TRAM_CONTEXT, then the current context; flags
like --admin-endpoint or --engine take precedence over all of them.`,
		Example: `  tram context add local --admin-endpoint localhost:50052
  tram context add prod --engine telemetry -n observability --kube-context prod
  tram context use prod
  tram tap logs                     # the pods of Engine telemetry in prod`,
	}
	cmd.AddCommand(newContextAddCmd(), newContextUseCmd(), newContextListCmd(), newContextRemoveCmd())
	return cmd
}

func newContextAddCmd() *cobra.Command {
	var f targetFlags
	cmd := &cobra.Command{
		Use:   "add <name>",
		Short: "Add or replace a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var c contextSpec
			switch {
			case f.kube.Engine != "" || f.kube.Selector != "":
				if f.adminEndpoint != "" {
					return errors.New("set --admin-endpoint or a target in Kubernetes, not both")
				}
				if f.kube.Engine != "" && f.kube.Selector != "" {
					return errors.New("set --engine or --selector, not both")
				}
				kube := f.kube
				c.Kubernetes = &kube
			case f.adminEndpoint != "":
				c.AdminEndpoint = f.adminEndpoint
			default:
				return errors.New("set --admin-endpoint, --engine or --selector")
			}
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if cfg.Contexts == nil {
				cfg.Contexts = map[string]contextSpec{}
			}
			cfg.Contexts[args[0]] = c
			if cfg.CurrentContext == "" {
				cfg.CurrentContext = args[0]
			}
			if err = saveConfig(cfg); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "context %s: %s\n", args[0], c)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&f.adminEndpoint, "admin-endpoint", "", "Admin API of the TRAM server")
	flags.StringVar(&f.kube.Engine, "engine", "", "Kubernetes: the Engine whose pods to talk to")
	flags.StringVarP(&f.kube.Selector, "selector", "l", "", "Kubernetes: label selector of the TRAM pods")
	flags.StringVarP(
		&f.kube.Namespace,
		"namespace",
		"n",
		"",
		"Kubernetes: namespace of the pods (default the kubeconfig's)",
	)
	flags.StringVar(
		&f.kube.Context,
		"kube-context",
		"",
		"Kubernetes: context of the kubeconfig (default its current one)",
	)
	flags.StringVar(&f.kube.Kubeconfig, "kubeconfig", "", "Kubernetes: kubeconfig file (default as kubectl)")
	flags.IntVar(&f.kube.AdminPort, "admin-port", 0, "Kubernetes: port of the admin API in the pods")
	return cmd
}

func newContextUseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Make a context the current one",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if _, ok := cfg.Contexts[args[0]]; !ok {
				return fmt.Errorf("no context %q in %s", args[0], configPath())
			}
			cfg.CurrentContext = args[0]
			if err = saveConfig(cfg); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "using context %s\n", args[0])
			return nil
		},
	}
}

func newContextListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List the contexts; * marks the current one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, tablePadding, ' ', 0)
			_, _ = fmt.Fprintln(w, "CURRENT\tNAME\tTARGET")
			for _, name := range slices.Sorted(maps.Keys(cfg.Contexts)) {
				current := ""
				if name == cfg.CurrentContext {
					current = "*"
				}
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", current, name, cfg.Contexts[name])
			}
			return w.Flush()
		},
	}
}

func newContextRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove a context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			if _, ok := cfg.Contexts[args[0]]; !ok {
				return fmt.Errorf("no context %q in %s", args[0], configPath())
			}
			delete(cfg.Contexts, args[0])
			if cfg.CurrentContext == args[0] {
				cfg.CurrentContext = ""
			}
			if err = saveConfig(cfg); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "removed context %s\n", args[0])
			return nil
		},
	}
}
