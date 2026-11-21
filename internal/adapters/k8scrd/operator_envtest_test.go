// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/go-tram/tram/internal/adapters/k8scrd"
	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/core/domain"
)

const (
	roleFile     = "role.yaml"
	operatorName = "tram-operator"
)

// manifest returns the path of a file or directory of manifests/ in the
// repository, where controller-gen writes the CRDs and roles.
func manifest(parts ...string) string {
	return filepath.Join(append([]string{"..", "..", "..", "manifests"}, parts...)...)
}

const (
	eventuallyTimeout = 15 * time.Second
	pollInterval      = 50 * time.Millisecond
	settleTime        = time.Second
)

type cluster struct {
	env   *envtest.Environment
	admin client.Client
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run `task test:envtest`")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{manifest("crds")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, env.Stop()) })

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, tramv1.AddToScheme(scheme))
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	return &cluster{env: env, admin: admin}
}

func (c *cluster) kubeconfig(t *testing.T, user envtest.User) string {
	t.Helper()
	authUser, err := c.env.AddUser(user, nil)
	require.NoError(t, err)
	content, err := authUser.KubeConfig()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, content, 0o600))
	return path
}

func (c *cluster) adminKubeconfig(t *testing.T) string {
	t.Helper()
	return c.kubeconfig(t, envtest.User{Name: "admin", Groups: []string{"system:masters"}})
}

func (c *cluster) newNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{GenerateName: "tram-test-"}
	require.NoError(t, c.admin.Create(t.Context(), ns))
	return ns.Name
}

func (c *cluster) createStream(t *testing.T, namespace, name string, labels map[string]string) {
	t.Helper()
	stream := &tramv1.Stream{
		Namespace: namespace, Name: name, Labels: labels,
		Spec: tramv1.StreamSpec{
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

func (c *cluster) setLabels(t *testing.T, namespace, name string, labels map[string]string) {
	t.Helper()
	var stream tramv1.Stream
	require.NoError(t, c.admin.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, &stream))
	base := stream.DeepCopy()
	stream.Labels = labels
	require.NoError(t, c.admin.Patch(t.Context(), &stream, client.MergeFrom(base)))
}

func (c *cluster) requireActive(t *testing.T, namespace, name string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var stream tramv1.Stream
		key := types.NamespacedName{Namespace: namespace, Name: name}
		assert.NoError(collect, c.admin.Get(t.Context(), key, &stream))
		assert.True(collect, stream.Status.Active)
	}, eventuallyTimeout, pollInterval)
}

type syncCore struct {
	mu       sync.Mutex
	streams  map[string]domain.StreamSpec
	resolved map[string]domain.StreamSpec
}

func newSyncCore() *syncCore {
	return &syncCore{streams: map[string]domain.StreamSpec{}, resolved: map[string]domain.StreamSpec{}}
}

func (c *syncCore) UpsertStream(ctx context.Context, s domain.StreamSpec) (*domain.StreamSpec, error) {
	return c.UpsertResolvedStream(ctx, s, s)
}

func (c *syncCore) UpsertResolvedStream(_ context.Context, s, resolved domain.StreamSpec) (*domain.StreamSpec, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams[s.Name] = s
	c.resolved[s.Name] = resolved
	return &s, nil
}

// resolvedSinkSpec returns the spec the core runs the first sink of a stream
// with.
func (c *syncCore) resolvedSinkSpec(name string) map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.resolved[name]
	if !ok || len(s.Sinks) == 0 {
		return nil
	}
	return s.Sinks[0].Spec
}

func (c *syncCore) PlanStream(_ context.Context, s, _ domain.StreamSpec) (*domain.StreamPlan, error) {
	return &domain.StreamPlan{Stream: s.Name, Action: domain.StreamPlanActionRebuild}, nil
}

func (c *syncCore) DeleteStream(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.streams[name]; !ok {
		return domain.ErrStreamNotFound
	}
	delete(c.streams, name)
	return nil
}

func (c *syncCore) GetStream(_ context.Context, name string) (*domain.StreamSpec, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.streams[name]
	if !ok {
		return nil, domain.ErrStreamNotFound
	}
	return &s, nil
}

func (c *syncCore) GetStreams(context.Context) ([]domain.StreamSpec, error) { return nil, nil }

func (c *syncCore) Watch(context.Context, chan domain.StreamSpecWatchEvent) error { return nil }

func (c *syncCore) names() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.streams))
	for name := range c.streams {
		names = append(names, name)
	}
	return names
}

func runOperator(t *testing.T, core *syncCore, opts k8scrd.Options) {
	t.Helper()
	op, err := k8scrd.New(core, k8scrd.WithoutNameValidation(opts), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- op.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
	})
}

func requireStreams(t *testing.T, core *syncCore, want ...string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, want, core.names())
	}, eventuallyTimeout, pollInterval)
	require.Never(t, func() bool {
		return !assert.ElementsMatch(new(assert.CollectT), want, core.names())
	}, settleTime, pollInterval)
}

func TestOperatorRunsOnlySelectedStreams(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	watched, other := c.newNamespace(t), c.newNamespace(t)
	selected := map[string]string{tramv1.EngineLabel("validation"): ""}
	c.createEngineStream(t, watched, "selected", selected, "validation")
	c.createEngineStream(t, watched, "unlabeled", nil, "validation")
	c.createEngineStream(t, other, "selected", selected, "validation")

	core := newSyncCore()
	runOperator(t, core, k8scrd.Options{
		Kubeconfig: c.adminKubeconfig(t),
		Namespaces: []string{watched},
		Engine:     "validation",
	})

	requireStreams(t, core, watched+".selected")
	c.requireActive(t, watched, "selected")

	c.setLabels(t, watched, "selected", nil)
	c.setLabels(t, watched, "unlabeled", selected)
	requireStreams(t, core, watched+".unlabeled")
	c.requireActive(t, watched, "unlabeled")
}

func TestOperatorNeedsOnlyRoleBindingInWatchedNamespace(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	content, err := os.ReadFile(manifest("tram-manager", "rbac", "engine", roleFile))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(content, &role))
	require.NoError(t, c.admin.Create(t.Context(), &role))

	ns := c.newNamespace(t)
	const user = operatorName
	require.NoError(t, c.admin.Create(t.Context(), &rbacv1.RoleBinding{
		Namespace: ns, Name: "tram",
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects: []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}))
	c.createStream(t, ns, "s", nil)

	core := newSyncCore()
	runOperator(t, core, k8scrd.Options{
		Kubeconfig: c.kubeconfig(t, envtest.User{Name: user}),
		Namespaces: []string{ns},
	})

	requireStreams(t, core, ns+".s")
	c.requireActive(t, ns, "s")
}

// The operator reads secrets with a role bound in the watched namespace only,
// sees secrets without the engine's label, and applies a changed
// secret to the streams that refer to it.
func TestOperatorAppliesSecrets(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	content, err := os.ReadFile(manifest("tram-manager", "rbac", "engine", roleFile))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(content, &role))
	require.NoError(t, c.admin.Create(t.Context(), &role))

	ns := c.newNamespace(t)
	const user = operatorName
	require.NoError(t, c.admin.Create(t.Context(), &rbacv1.RoleBinding{
		Namespace: ns, Name: "tram",
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects: []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}))

	secret := &corev1.Secret{
		Namespace: ns, Name: "otlp-auth",
		Data: map[string][]byte{"token": []byte("old")},
	}
	require.NoError(t, c.admin.Create(t.Context(), secret))
	require.NoError(t, c.admin.Create(t.Context(), &tramv1.Stream{
		Namespace: ns, Name: "s", Labels: map[string]string{tramv1.EngineLabel("validation"): ""},
		Spec: tramv1.StreamSpec{
			EngineRefs: engineRefs("validation"),
			Source: tramv1.Spec{
				Name: "in",
				Type: "otlp",
				Spec: &runtime.RawExtension{Raw: []byte(`{"http_port":4318}`)},
			},
			Sinks: []tramv1.ReceiverSpec{
				{Name: "out", Type: "otlp", From: "in", Spec: &runtime.RawExtension{Raw: []byte(
					`{"url":"http://o:4318","headers":{"Authorization":` +
						`{"valueFrom":{"secretKeyRef":{"name":"otlp-auth","key":"token"}}}}}`)}},
			},
		},
	}))

	core := newSyncCore()
	runOperator(t, core, k8scrd.Options{
		Kubeconfig: c.kubeconfig(t, envtest.User{Name: user}),
		Namespaces: []string{ns},
		Engine:     "validation",
	})

	token := func() any {
		headers, _ := core.resolvedSinkSpec(ns + ".s")["headers"].(map[string]any)
		return headers["Authorization"]
	}
	require.Eventually(t, func() bool { return token() == "old" }, eventuallyTimeout, pollInterval)
	c.requireActive(t, ns, "s")

	secret.Data["token"] = []byte("new")
	require.NoError(t, c.admin.Update(t.Context(), secret))
	require.Eventually(t, func() bool { return token() == "new" }, eventuallyTimeout, pollInterval,
		"a changed secret is applied without a change to the stream")
}

// resolvedTable returns the table of the first stage a stream runs with.
func (c *syncCore) resolvedTable(name string) map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.resolved[name]
	if !ok || len(s.Stages) == 0 {
		return nil
	}
	table, _ := s.Stages[0].Spec["table"].(map[string]any)
	return table
}

// The operator merges the config maps a stream selects, with a role bound in
// the watched namespace only and without the engine's label on them,
// and applies created, changed, relabeled and conflicting config maps.
func TestOperatorAppliesConfigMaps(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	content, err := os.ReadFile(manifest("tram-manager", "rbac", "engine", roleFile))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(content, &role))
	require.NoError(t, c.admin.Create(t.Context(), &role))

	ns := c.newNamespace(t)
	const user = operatorName
	require.NoError(t, c.admin.Create(t.Context(), &rbacv1.RoleBinding{
		Namespace: ns, Name: "tram",
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects: []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}))

	routing := map[string]string{"tram.io/table": "routing"}
	a := &corev1.ConfigMap{Namespace: ns, Name: "routing-a", Labels: routing, Data: map[string]string{"t1": "r1"}}
	require.NoError(t, c.admin.Create(t.Context(), a))
	require.NoError(t, c.admin.Create(t.Context(), &tramv1.Stream{
		Namespace: ns, Name: "s", Labels: map[string]string{tramv1.EngineLabel("mapper"): ""},
		Spec: tramv1.StreamSpec{
			EngineRefs: engineRefs("mapper"),
			Source: tramv1.Spec{
				Name: "in",
				Type: "otlp",
				Spec: &runtime.RawExtension{Raw: []byte(`{"http_port":4318}`)},
			},
			Stages: []tramv1.ReceiverSpec{
				{Name: "map", Type: "value_map", From: "in", Spec: &runtime.RawExtension{Raw: []byte(
					`{"source_attr":"tenant","target_attr":"router","default":"none","table":` +
						`{"valueFrom":{"configMapSelector":{"matchLabels":{"tram.io/table":"routing"}}}}}`)}},
			},
			Sinks: []tramv1.ReceiverSpec{{Name: "out", Type: "debug", From: "map"}},
		},
	}))

	core := newSyncCore()
	runOperator(t, core, k8scrd.Options{
		Kubeconfig: c.kubeconfig(t, envtest.User{Name: user}),
		Namespaces: []string{ns},
		Engine:     "mapper",
	})
	requireTable := func(want map[string]any, msg string) {
		t.Helper()
		require.Eventually(t, func() bool { return assert.ObjectsAreEqual(want, core.resolvedTable(ns+".s")) },
			eventuallyTimeout, pollInterval, msg)
	}
	requireTable(map[string]any{"t1": "r1"}, "the stream runs with the selected config map")
	c.requireActive(t, ns, "s")

	b := &corev1.ConfigMap{Namespace: ns, Name: "routing-b", Labels: routing, Data: map[string]string{"t2": "r2"}}
	require.NoError(t, c.admin.Create(t.Context(), b))
	requireTable(map[string]any{"t1": "r1", "t2": "r2"}, "a created config map is merged")

	a.Data["t1"] = "r3"
	require.NoError(t, c.admin.Update(t.Context(), a))
	requireTable(map[string]any{"t1": "r3", "t2": "r2"}, "a changed config map is applied")

	b.Labels = nil
	require.NoError(t, c.admin.Update(t.Context(), b))
	requireTable(map[string]any{"t1": "r3"}, "a config map that no longer matches is left out")

	conflict := &corev1.ConfigMap{
		Namespace: ns,
		Name:      "routing-c",
		Labels:    routing,
		Data:      map[string]string{"t1": "r9"},
	}
	require.NoError(t, c.admin.Create(t.Context(), conflict))
	require.Eventually(t, func() bool {
		var s tramv1.Stream
		return c.admin.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: "s"}, &s) == nil && !s.Status.Active
	}, eventuallyTimeout, pollInterval, "conflicting config maps deactivate the stream")
	assert.Equal(t, map[string]any{"t1": "r3"}, core.resolvedTable(ns+".s"), "the stream keeps its last table")

	require.NoError(t, c.admin.Delete(t.Context(), conflict))
	c.requireActive(t, ns, "s")
}
