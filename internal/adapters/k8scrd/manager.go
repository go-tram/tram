// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
	"github.com/go-tram/tram/internal/adapters/k8scrd/internal/engines"
)

const (
	// leaderElectionID names the lease the replicas of the manager compete
	// for.
	leaderElectionID = "manager.tram.io"
	// DefaultEngineClusterRole is the role of a Tram instance with the
	// operator enabled, from manifests/tram-manager/rbac/engine/role.yaml.
	DefaultEngineClusterRole = "manager-role"
)

// ManagerOptions configure the manager of `tram manager`.
type ManagerOptions struct {
	Kubeconfig string
	// Namespaces limits the manager to the Stream resources of these
	// namespaces; all namespaces without any.
	Namespaces []string
	// LeaderElection lets only one of several manager replicas reconcile at a
	// time.
	LeaderElection bool
	// LeaderElectionNamespace holds the lease. In a pod it defaults to the
	// pod's namespace; outside a cluster it is required with LeaderElection.
	LeaderElectionNamespace string
	// EngineImage runs the Engines that set no spec.image.
	EngineImage string
	// EngineClusterRole is bound to the service account of every Engine in
	// its namespace; DefaultEngineClusterRole without it.
	EngineClusterRole string

	skipNameValidation bool
}

// Manager runs the controllers of `tram manager`.
type Manager struct {
	mgr    ctrl.Manager
	logger *slog.Logger
}

func NewManager(opts ManagerOptions, logger *slog.Logger) (*Manager, error) {
	setLogger(logger)
	cacheOpts := managerCacheOptions()
	for _, ns := range opts.Namespaces {
		// An empty name would stand for all namespaces and silently undo the
		// limit, e.g. after a trailing comma.
		if ns == "" {
			return nil, ErrEmptyNamespace
		}
		if cacheOpts.DefaultNamespaces == nil {
			cacheOpts.DefaultNamespaces = map[string]cache.Config{}
		}
		cacheOpts.DefaultNamespaces[ns] = cache.Config{}
	}

	cfg, err := ctrl.GetConfig()
	if opts.Kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", opts.Kubeconfig)
	}
	if err != nil {
		return nil, fmt.Errorf("get kube config: %w", err)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme, tramv1.AddToScheme, tramv1alpha1.AddToScheme,
	} {
		if err = add(scheme); err != nil {
			return nil, err
		}
	}

	logger.Info("Connecting to Kubernetes", slog.String("host", cfg.Host),
		slog.Any("namespaces", opts.Namespaces), slog.Bool("leader_election", opts.LeaderElection))

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                  scheme,
		Logger:                  logr.FromSlogHandler(logger.Handler()),
		Cache:                   cacheOpts,
		LeaderElection:          opts.LeaderElection,
		LeaderElectionID:        leaderElectionID,
		LeaderElectionNamespace: opts.LeaderElectionNamespace,
		// The process ends once the manager stops, so the lease can be handed
		// over at once instead of after it expires.
		LeaderElectionReleaseOnCancel: true,
		Controller:                    config.Controller{SkipNameValidation: &opts.skipNameValidation},
		Metrics: server.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return nil, err
	}

	if err = (&engines.StreamReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("set up stream controller: %w", err)
	}
	clusterRole := opts.EngineClusterRole
	if clusterRole == "" {
		clusterRole = DefaultEngineClusterRole
	}
	if err = (&engines.EngineReconciler{
		Client:      mgr.GetClient(),
		Image:       opts.EngineImage,
		ClusterRole: clusterRole,
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("set up engine controller: %w", err)
	}

	return &Manager{mgr: mgr, logger: logger}, nil
}

// managerCacheOptions caches only the resources the manager generates.
func managerCacheOptions() cache.Options {
	generated := cache.ByObject{Label: labels.SelectorFromSet(labels.Set{engines.ManagedByLabel: engines.FieldOwner})}
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&tramv1.Stream{}:                         {Transform: cache.TransformStripManagedFields()},
			&appsv1.Deployment{}:                     generated,
			&corev1.Service{}:                        generated,
			&corev1.ServiceAccount{}:                 generated,
			&rbacv1.RoleBinding{}:                    generated,
			&autoscalingv2.HorizontalPodAutoscaler{}: generated,
			&policyv1.PodDisruptionBudget{}:          generated,
			&appsv1.StatefulSet{}:                    generated,
			&batchv1.Job{}:                           generated,
			&corev1.PersistentVolumeClaim{}:          generated,
			&corev1.Pod{}:                            generated,
		},
	}
}

func (m *Manager) Run(ctx context.Context) error {
	m.logger.InfoContext(ctx, "Starting Kubernetes manager")
	err := m.mgr.Start(ctx)
	m.logger.InfoContext(ctx, "Kubernetes manager stopped", slog.Any("error", err))
	return err
}
