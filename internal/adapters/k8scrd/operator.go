// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

//go:generate controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
// The CRDs and roles go to manifests/ of the repository and, the same, to the
// templates of the Helm chart in charts/tram.
//go:generate controller-gen crd paths="./..." output:crd:dir=../../../manifests/crds
//go:generate controller-gen rbac:roleName=manager-role paths="./internal/controller/..." output:rbac:dir=../../../manifests/tram-manager/rbac/engine
//go:generate controller-gen rbac:roleName=tram-manager paths="./internal/engines/..." output:rbac:dir=../../../manifests/tram-manager/rbac/manager
//go:generate controller-gen crd paths="./..." output:crd:dir=../../../charts/tram/templates/crds
//go:generate controller-gen rbac:roleName=manager-role paths="./internal/controller/..." output:rbac:dir=../../../charts/tram/templates/tram-manager/rbac/engine
//go:generate controller-gen rbac:roleName=tram-manager paths="./internal/engines/..." output:rbac:dir=../../../charts/tram/templates/tram-manager/rbac/manager

package k8scrd

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	"github.com/go-tram/tram/internal/adapters/k8scrd/internal/controller"
	"github.com/go-tram/tram/internal/ports"
)

type Operator struct {
	mgr    ctrl.Manager
	core   ports.Core
	logger *slog.Logger
}

type Options struct {
	Kubeconfig string
	Namespaces []string
	Engine     string
	// Drain runs only the Streams that read from a buffer, for `tram drain`.
	Drain              bool
	skipNameValidation bool
}

// setLogger sets the global logger of controller-runtime. Its package-level
// loggers, like those of the client config, cache and sources, use it instead
// of the manager's logger.
func setLogger(logger *slog.Logger) {
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))
}

func New(core ports.Core, opts Options, logger *slog.Logger) (*Operator, error) {
	setLogger(logger)
	cacheOpts, err := cacheOptions(opts)
	if err != nil {
		return nil, err
	}

	cfg, err := ctrl.GetConfig()

	if opts.Kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", opts.Kubeconfig)
	}

	if err != nil {
		return nil, fmt.Errorf("get kube config: %w", err)
	}

	scheme := runtime.NewScheme()
	if err = clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err = tramv1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	logger.Info("Connecting to Kubernetes", slog.String("host", cfg.Host),
		slog.Any("namespaces", opts.Namespaces), slog.String("engine", opts.Engine))

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Logger:         logr.FromSlogHandler(logger.Handler()),
		Cache:          cacheOpts,
		LeaderElection: false,
		Controller:     config.Controller{SkipNameValidation: &opts.skipNameValidation},
		Metrics: server.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return nil, err
	}

	if err = (&controller.StreamReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Core:       core,
		References: mgr.GetAPIReader(),
		Engine:     opts.Engine,
		Drain:      opts.Drain,
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("set up controller: %w", err)
	}

	return &Operator{mgr: mgr, core: core, logger: logger}, nil
}

// cacheOptions limits the cache to the selected Stream resources. The
// reconciler reads through the cache, so a resource that leaves the selection,
// e.g. because its label was removed, is reconciled as deleted and its stream
// stops.
func cacheOptions(opts Options) (cache.Options, error) {
	var cacheOpts cache.Options

	if len(opts.Namespaces) > 0 {
		cacheOpts.DefaultNamespaces = make(map[string]cache.Config, len(opts.Namespaces))
		for _, ns := range opts.Namespaces {
			// An empty name would stand for all namespaces and silently undo the
			// limit, e.g. after a trailing comma.
			if ns == "" {
				return cache.Options{}, ErrEmptyNamespace
			}
			cacheOpts.DefaultNamespaces[ns] = cache.Config{}
		}
	}

	if opts.Engine != "" {
		selector, err := engineSelector(opts.Engine)
		if err != nil {
			return cache.Options{}, err
		}
		cacheOpts.DefaultLabelSelector = selector
		// The selector chooses Stream resources, not the secrets and config
		// maps they refer to, whose metadata is watched to notice changes.
		cacheOpts.ByObject = map[client.Object]cache.ByObject{
			&metav1.PartialObjectMetadata{APIVersion: "v1", Kind: "Secret"}: {
				Label: labels.Everything(),
			},
			&metav1.PartialObjectMetadata{APIVersion: "v1", Kind: "ConfigMap"}: {
				Label: labels.Everything(),
			},
		}
	}

	return cacheOpts, nil
}

// engineSelector selects the Streams that carry the label of engine, so the
// operator runs only the Streams whose spec.engineRefs name the engine.
func engineSelector(engine string) (labels.Selector, error) {
	if errs := validation.IsDNS1123Label(engine); len(errs) > 0 {
		return nil, fmt.Errorf("%w %q: %s", ErrInvalidEngine, engine, strings.Join(errs, "; "))
	}
	exists, err := labels.NewRequirement(tramv1.EngineLabel(engine), selection.Exists, nil)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %w", ErrInvalidEngine, engine, err)
	}
	return labels.NewSelector().Add(*exists), nil
}

func (o *Operator) Run(ctx context.Context) error {
	o.logger.InfoContext(ctx, "Starting Kubernetes operator")
	err := o.mgr.Start(ctx)
	o.logger.InfoContext(ctx, "Kubernetes operator stopped", slog.Any("error", err))
	return err
}
