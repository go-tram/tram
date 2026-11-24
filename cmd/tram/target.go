// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultAdminEndpoint = "localhost:50052"
	adminEndpointEnv     = "TRAM_ADMIN_ENDPOINT"
	contextEnv           = "TRAM_CONTEXT"
)

// targetFlags are the flags of the commands that talk to a server: where its
// admin API is, directly, through a context, or as pods in Kubernetes.
type targetFlags struct {
	adminEndpoint string
	context       string
	kube          kubeTarget
}

func addTargetFlags(cmd *cobra.Command, f *targetFlags) {
	flags := cmd.PersistentFlags()
	flags.StringVar(&f.adminEndpoint, "admin-endpoint", "",
		"Admin API of the TRAM server (env "+adminEndpointEnv+", default "+defaultAdminEndpoint+")")
	flags.StringVar(&f.context, "context", "", "Context of the tram config to use (env "+contextEnv+")")
	flags.StringVar(&f.kube.Engine, "engine", "", "Kubernetes: the Engine whose pods to talk to")
	flags.StringVarP(&f.kube.Selector, "selector", "l", "",
		"Kubernetes: label selector of the TRAM pods to talk to, instead of an Engine")
	flags.StringVarP(
		&f.kube.Namespace,
		"namespace",
		"n",
		"",
		"Kubernetes: namespace of the pods (default the kubeconfig's)",
	)
	flags.StringVar(&f.kube.Context, "kube-context", "", "Kubernetes: context of the kubeconfig to use")
	flags.StringVar(&f.kube.Kubeconfig, "kubeconfig", "", "Kubernetes: kubeconfig file (default as kubectl)")
	flags.IntVar(&f.kube.AdminPort, "admin-port", 0,
		"Kubernetes: port of the admin API in the pods (default the Engine's, or 50052)")
}

// target is where a command goes: an address or pods in Kubernetes.
type target struct {
	address string
	kube    *kubeTarget
}

// resolve finds the target. Flags come first, then the environment, then the
// context: --engine or --selector, --admin-endpoint, --context,
// TRAM_ADMIN_ENDPOINT, TRAM_CONTEXT, the current context of the tram config,
// and at last localhost:50052. The other Kubernetes flags refine a target in
// Kubernetes, e.g. -n the namespace of the context's Engine.
func (f *targetFlags) resolve() (target, error) {
	t, err := f.base()
	if err != nil {
		return target{}, err
	}
	if t.kube != nil {
		kube := t.kube.with(f.kube)
		t.kube = &kube
	} else if f.kube.Namespace != "" || f.kube.Context != "" || f.kube.Kubeconfig != "" || f.kube.AdminPort != 0 {
		return target{}, errors.New("-n, --kube-context, --kubeconfig and --admin-port need a target in Kubernetes: " +
			"set --engine or --selector, or use a context with one")
	}
	return t, nil
}

func (f *targetFlags) base() (target, error) {
	if f.kube.Engine != "" || f.kube.Selector != "" {
		if f.kube.Engine != "" && f.kube.Selector != "" {
			return target{}, errors.New("set --engine or --selector, not both")
		}
		return target{kube: &kubeTarget{Engine: f.kube.Engine, Selector: f.kube.Selector}}, nil
	}
	if f.adminEndpoint != "" {
		return target{address: f.adminEndpoint}, nil
	}
	if f.context != "" {
		return contextTarget(f.context)
	}
	if endpoint := os.Getenv(adminEndpointEnv); endpoint != "" {
		return target{address: endpoint}, nil
	}
	if name := os.Getenv(contextEnv); name != "" {
		return contextTarget(name)
	}
	cfg, err := loadConfig()
	if err != nil {
		return target{}, err
	}
	if cfg.CurrentContext != "" {
		return contextTarget(cfg.CurrentContext)
	}
	return target{address: defaultAdminEndpoint}, nil
}

func contextTarget(name string) (target, error) {
	cfg, err := loadConfig()
	if err != nil {
		return target{}, err
	}
	c, ok := cfg.Contexts[name]
	if !ok {
		return target{}, fmt.Errorf("no context %q in %s; see tram context list", name, configPath())
	}
	if c.Kubernetes != nil {
		kube := *c.Kubernetes
		return target{kube: &kube}, nil
	}
	return target{address: c.AdminEndpoint}, nil
}

// instance is one TRAM server of a target; name is its pod, empty for an
// address.
type instance struct {
	name string
	conn *grpc.ClientConn
}

// connector reaches the instances of a target.
type connector interface {
	// one connects to one instance, any of them.
	one(ctx context.Context) (instance, func(), error)
	// each runs fn for every instance until ctx ends, for an instance that
	// appears later too, and cancels the context of fn when its instance
	// goes away. An error of fn ends all of them and is returned.
	each(ctx context.Context, fn func(context.Context, instance) error) error
	// many reports whether the target can have several instances.
	many() bool
}

func (t target) connect(ctx context.Context, errOut io.Writer) (connector, error) {
	if t.kube != nil {
		return newKubeConnector(ctx, *t.kube, errOut)
	}
	return addressConnector(t.address), nil
}

// addressConnector reaches the one instance at an address.
type addressConnector string

func (a addressConnector) one(context.Context) (instance, func(), error) {
	conn, err := dial(string(a))
	if err != nil {
		return instance{}, nil, err
	}
	return instance{conn: conn}, func() { _ = conn.Close() }, nil
}

func (a addressConnector) each(ctx context.Context, fn func(context.Context, instance) error) error {
	in, closeConn, err := a.one(ctx)
	if err != nil {
		return err
	}
	defer closeConn()
	return fn(ctx, in)
}

func (addressConnector) many() bool { return false }

func dial(address string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", address, err)
	}
	return conn, nil
}
