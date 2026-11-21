// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/go-tram/tram/internal/adapters/k8scrd"
	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
)

const (
	routersLabel = "engine.tram.io/routers"
	onlyMapper   = "only-mapper"
)

func engineRefs(engines ...string) []tramv1.EngineReference {
	refs := make([]tramv1.EngineReference, len(engines))
	for i, engine := range engines {
		refs[i] = tramv1.EngineReference{Name: engine}
	}
	return refs
}

func (c *cluster) createEngineStream(
	t *testing.T,
	namespace, name string,
	labels map[string]string,
	engines ...string,
) {
	t.Helper()
	stream := &tramv1.Stream{
		Namespace: namespace, Name: name, Labels: labels,
		Spec: tramv1.StreamSpec{
			EngineRefs: engineRefs(engines...),
			Source: tramv1.Spec{
				Name: "in",
				Type: "otlp",
				Spec: &runtime.RawExtension{Raw: []byte(`{"http_port":4318}`)},
			},
			Sinks: []tramv1.ReceiverSpec{{Name: "out", Type: "debug", From: "in"}},
		},
	}
	require.NoError(t, c.admin.Create(t.Context(), stream))
}

func (c *cluster) setEngineRefs(t *testing.T, namespace, name string, engines ...string) {
	t.Helper()
	var stream tramv1.Stream
	require.NoError(t, c.admin.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, &stream))
	base := stream.DeepCopy()
	stream.Spec.EngineRefs = engineRefs(engines...)
	require.NoError(t, c.admin.Patch(t.Context(), &stream, client.MergeFrom(base)))
}

func (c *cluster) requireLabels(t *testing.T, namespace, name string, want map[string]string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var stream tramv1.Stream
		key := types.NamespacedName{Namespace: namespace, Name: name}
		assert.NoError(collect, c.admin.Get(t.Context(), key, &stream))
		// The API server drops an empty label map.
		got := stream.Labels
		if got == nil {
			got = map[string]string{}
		}
		assert.Equal(collect, want, got)
	}, eventuallyTimeout, pollInterval)
}

func runManager(t *testing.T, opts k8scrd.ManagerOptions) {
	t.Helper()
	m, err := k8scrd.NewManager(k8scrd.ManagerWithoutNameValidation(opts), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
	})
}

func TestManagerDerivesEngineLabels(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	ns := c.newNamespace(t)
	c.createEngineStream(t, ns, "mapped",
		map[string]string{"team": "telemetry", "engine.tram.io/old": ""}, "mapper", "routers")
	c.createEngineStream(t, ns, "unassigned", map[string]string{mapperLabel: ""})

	runManager(t, k8scrd.ManagerOptions{
		Kubeconfig:              c.adminKubeconfig(t),
		LeaderElection:          true,
		LeaderElectionNamespace: ns,
	})

	c.requireLabels(t, ns, "mapped",
		map[string]string{"team": "telemetry", mapperLabel: "", routersLabel: ""})
	c.requireLabels(t, ns, "unassigned", map[string]string{})

	c.setEngineRefs(t, ns, "mapped", "routers")
	c.requireLabels(t, ns, "mapped", map[string]string{"team": "telemetry", routersLabel: ""})

	// An engine label set by hand is removed, other labels stay.
	c.setLabels(t, ns, "mapped",
		map[string]string{"team": "platform", routersLabel: "yes", "engine.tram.io/rogue": ""})
	c.requireLabels(t, ns, "mapped", map[string]string{"team": "platform", routersLabel: ""})
}

func TestEnginesRunTheStreamsThatReferToThem(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	ns := c.newNamespace(t)
	c.createEngineStream(t, ns, onlyMapper, nil, "mapper")
	c.createEngineStream(t, ns, "both", nil, "mapper", "routers")
	c.createEngineStream(t, ns, "none", nil)

	cfg := c.adminKubeconfig(t)
	runManager(t, k8scrd.ManagerOptions{Kubeconfig: cfg, Namespaces: []string{ns}})
	mapper, routers := newSyncCore(), newSyncCore()
	runOperator(t, mapper, k8scrd.Options{Kubeconfig: cfg, Namespaces: []string{ns}, Engine: "mapper"})
	runOperator(t, routers, k8scrd.Options{Kubeconfig: cfg, Namespaces: []string{ns}, Engine: "routers"})

	requireStreams(t, mapper, ns+".only-mapper", ns+".both")
	requireStreams(t, routers, ns+".both")
	c.requireActive(t, ns, onlyMapper)

	// Moving a Stream to another engine stops it on the first and starts it on
	// the second.
	c.setEngineRefs(t, ns, onlyMapper, "routers")
	requireStreams(t, mapper, ns+".both")
	requireStreams(t, routers, ns+".both", ns+".only-mapper")
}

func TestManagerNeedsOnlyItsClusterRole(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	content, err := os.ReadFile(manifest("tram-manager", "rbac", "manager", roleFile))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(content, &role))
	require.NoError(t, c.admin.Create(t.Context(), &role))

	const user = "tram-manager"
	require.NoError(t, c.admin.Create(t.Context(), &rbacv1.ClusterRoleBinding{
		Name:     "tram-manager",
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects: []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}))

	ns := c.newNamespace(t)
	c.createEngineStream(t, ns, "s", nil, "mapper")

	runManager(t, k8scrd.ManagerOptions{
		Kubeconfig:              c.kubeconfig(t, envtest.User{Name: user}),
		LeaderElection:          true,
		LeaderElectionNamespace: ns,
	})
	c.requireLabels(t, ns, "s", map[string]string{mapperLabel: ""})
}

func TestStreamRejectsInvalidEngineRefs(t *testing.T) {
	t.Parallel()
	c := startCluster(t)
	ns := c.newNamespace(t)

	tooMany := make([]string, 17)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("engine-%d", i)
	}
	tests := map[string][]string{
		"upper case":  {"Mapper"},
		"underscore":  {"mapper_v2"},
		"too long":    {strings.Repeat("a", 52)},
		"empty":       {""},
		"named twice": {"mapper", "mapper"},
		"too many":    tooMany,
	}
	i := 0
	for name, engines := range tests {
		i++
		stream := &tramv1.Stream{
			Namespace: ns, Name: fmt.Sprintf("s-%d", i),
			Spec: tramv1.StreamSpec{
				EngineRefs: engineRefs(engines...),
				Source:     tramv1.Spec{Name: "in", Type: "generator"},
				Sinks:      []tramv1.ReceiverSpec{{Name: "out", Type: "debug", From: "in"}},
			},
		}
		err := c.admin.Create(t.Context(), stream)
		assert.True(t, apierrors.IsInvalid(err), "%s: %v", name, err)
	}
}

// Without the manager, nobody corrects engine labels: an engine runs only the
// Streams whose spec.engineRefs name it, whatever their labels.
func TestEngineIgnoresEngineLabelsSetByHand(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	ns := c.newNamespace(t)
	label := map[string]string{mapperLabel: ""}
	c.createEngineStream(t, ns, "by-hand", label)
	c.createEngineStream(t, ns, "referred", label, "mapper")

	core := newSyncCore()
	runOperator(t, core, k8scrd.Options{Kubeconfig: c.adminKubeconfig(t), Namespaces: []string{ns}, Engine: "mapper"})
	requireStreams(t, core, ns+".referred")

	// The reference goes, the stale label stays: the stream stops.
	c.setEngineRefs(t, ns, "referred", "routers")
	requireStreams(t, core)
}
