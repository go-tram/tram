// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// TestChartArgumentsAreManagerFlags renders the Helm chart with several values
// and parses the manager's arguments with the command tree of the binary, so
// the chart never passes a flag `tram manager` does not know.
func TestChartArgumentsAreManagerFlags(t *testing.T) {
	t.Parallel()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	variants := map[string][]string{
		"defaults":           nil,
		"watched namespaces": {"--set", "manager.watchNamespaces={telemetry,edge}"},
		"everything set": {
			"--set", "manager.engineImage=registry.example.com/tram:1.2.3",
			"--set", "manager.leaderElect=false",
			"--set", "manager.logLevel=debug",
			"--set", "manager.extraArgs={--engine-cluster-role=custom}",
		},
	}
	for name, values := range variants {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			args := renderedManagerArgs(t, helm, values)
			root := newRootCmd(slog.New(slog.DiscardHandler), &slog.LevelVar{})
			cmd, flags, findErr := root.Find(args)
			require.NoError(t, findErr)
			require.Equal(t, "manager", cmd.Name(), "arguments %v", args)
			// Parsing on the subcommand includes the flags it inherits.
			require.NoError(t, cmd.ParseFlags(flags), "arguments %v", args)
			assert.Empty(t, cmd.Flags().Args(), "no stray arguments")
		})
	}
}

// renderedManagerArgs renders the chart and returns the arguments of the
// manager's container.
func renderedManagerArgs(t *testing.T, helm string, values []string) []string {
	t.Helper()
	chart := filepath.Join("..", "..", "charts", "tram")
	out, err := exec.CommandContext(t.Context(), helm,
		append([]string{"template", "tram", chart, "--namespace", "tram-system"}, values...)...).Output()
	require.NoError(t, err)

	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		var obj struct {
			Kind string `json:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name string   `json:"name"`
							Args []string `json:"args"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		require.NoError(t, yaml.Unmarshal([]byte(doc), &obj))
		if obj.Kind != "Deployment" {
			continue
		}
		for _, c := range obj.Spec.Template.Spec.Containers {
			if c.Name == "manager" {
				return c.Args
			}
		}
	}
	t.Fatal("the chart renders no manager container")
	return nil
}

// TestChartRunsAsNumericNonRootUser renders the chart and checks that the
// manager's pod names its user by number: the image's user is named, and the
// kubelet refuses to start a pod with runAsNonRoot unless it can verify a
// numeric user. envtest has no kubelet, so the API server accepts it anyway.
func TestChartRunsAsNumericNonRootUser(t *testing.T) {
	t.Parallel()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	chart := filepath.Join("..", "..", "charts", "tram")
	out, err := exec.CommandContext(t.Context(), helm,
		"template", "tram", chart, "--namespace", "tram-system", "--set", "crds.install=false").Output()
	require.NoError(t, err)

	for doc := range strings.SplitSeq(string(out), "\n---\n") {
		var obj struct {
			Kind string `json:"kind"`
			Spec struct {
				Template struct {
					Spec struct {
						SecurityContext struct {
							RunAsNonRoot *bool  `json:"runAsNonRoot"`
							RunAsUser    *int64 `json:"runAsUser"`
						} `json:"securityContext"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		require.NoError(t, yaml.Unmarshal([]byte(doc), &obj))
		if obj.Kind != "Deployment" {
			continue
		}
		security := obj.Spec.Template.Spec.SecurityContext
		require.NotNil(t, security.RunAsNonRoot)
		require.True(t, *security.RunAsNonRoot)
		require.NotNil(t, security.RunAsUser, "runAsNonRoot needs a numeric runAsUser")
		assert.NotZero(t, *security.RunAsUser)
		return
	}
	t.Fatal("the chart renders no Deployment")
}

// TestChartSetsEngineImage checks that the manager knows the image of the
// Engines that set no spec.image: without --engine-image, every such Engine
// fails.
func TestChartSetsEngineImage(t *testing.T) {
	t.Parallel()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}

	args := renderedManagerArgs(t, helm, []string{"--set", "image.tag=1.2.3"})
	assert.Contains(t, args, "--engine-image=ghcr.io/go-tram/tram:1.2.3", "the manager's own image by default")

	args = renderedManagerArgs(t, helm, []string{"--set", "manager.engineImage=registry.example.com/tram:dev"})
	assert.Contains(t, args, "--engine-image=registry.example.com/tram:dev")
}
