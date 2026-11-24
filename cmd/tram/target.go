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
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultAdminEndpoint = "localhost:50052"
	adminEndpointEnv     = "TRAM_ADMIN_ENDPOINT"
	kubeEnv              = "TRAM_KUBE"
)

// targetFlags are the flags of the commands that talk to a server: the
// address of its admin API, or TRAM pods in Kubernetes.
type targetFlags struct {
	adminEndpoint string
	kubeOn        bool
	kube          kubeTarget
	// config holds the flags of kubectl for the kubeconfig, e.g. --context
	// and -n; nil in tests.
	config *kubeconfigFlags
	// plugin makes Kubernetes the default: the CLI runs as kubectl tram.
	plugin bool
}

func addTargetFlags(cmd *cobra.Command, f *targetFlags, plugin bool) {
	f.plugin = plugin
	flags := cmd.PersistentFlags()
	flags.StringVar(&f.adminEndpoint, "admin-endpoint", "",
		"Admin API of the TRAM server (env "+adminEndpointEnv+", default "+defaultAdminEndpoint+")")
	if !plugin {
		flags.BoolVar(&f.kubeOn, "kube", false,
			"Talk to the TRAM pods in Kubernetes that run the stream: the Engines its Stream resource names (env "+
				kubeEnv+")")
	}
	flags.StringVar(&f.kube.Engine, "engine", "", "Kubernetes: talk to the pods of this Engine")
	flags.StringVarP(&f.kube.Selector, "selector", "l", "",
		"Kubernetes: talk to the TRAM pods this label selector selects, instead of an Engine")
	flags.IntVar(&f.kube.AdminPort, "admin-port", 0,
		"Kubernetes: port of the admin API in the pods (default the Engine's, or 50052)")
	f.config = addKubeconfigFlags(flags)
}

// kubeconfigFlags are the flags of kubectl for the kubeconfig, e.g.
// --kubeconfig, --context and -n, with the same meaning.
type kubeconfigFlags struct {
	rules     *clientcmd.ClientConfigLoadingRules
	overrides clientcmd.ConfigOverrides
}

func addKubeconfigFlags(flags *pflag.FlagSet) *kubeconfigFlags {
	k := &kubeconfigFlags{rules: clientcmd.NewDefaultClientConfigLoadingRules()}
	flags.StringVar(&k.rules.ExplicitPath, clientcmd.RecommendedConfigPathFlag, "",
		"Path to the kubeconfig file (default as kubectl)")
	clientcmd.BindOverrideFlags(&k.overrides, flags, clientcmd.RecommendedConfigOverrideFlags(""))
	return k
}

// clientConfig is the kubeconfig with the flags; nil flags are the defaults.
func (k *kubeconfigFlags) clientConfig() clientcmd.ClientConfig {
	if k == nil {
		k = &kubeconfigFlags{rules: clientcmd.NewDefaultClientConfigLoadingRules()}
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(k.rules, &k.overrides)
}

// set reports whether one of the flags that choose the cluster, user or
// namespace was given.
func (k *kubeconfigFlags) set() bool {
	if k == nil {
		return false
	}
	o := k.overrides
	return k.rules.ExplicitPath != "" || o.CurrentContext != "" || o.Context.Namespace != "" ||
		o.Context.Cluster != "" || o.Context.AuthInfo != "" || o.ClusterInfo.Server != "" ||
		o.AuthInfo.Token != "" || o.AuthInfo.Impersonate != ""
}

// target is where a command goes: an address or pods in Kubernetes.
type target struct {
	address string
	kube    *kubeTarget
	// config is the kubeconfig of a target in Kubernetes, with the flags of
	// kubectl; nil for the default.
	config *kubeconfigFlags
}

// resolve finds the target: --kube, --engine or --selector, then
// --admin-endpoint, then TRAM_KUBE or TRAM_ADMIN_ENDPOINT, and at last
// localhost:50052, or Kubernetes for kubectl tram. The cluster and namespace
// come from the kubeconfig and the flags of kubectl.
func (f *targetFlags) resolve() (target, error) {
	kubeFlags := f.kubeOn || f.kube.Engine != "" || f.kube.Selector != ""
	if f.kube.Engine != "" && f.kube.Selector != "" {
		return target{}, errors.New("set --engine or --selector, not both")
	}
	switch {
	case kubeFlags && f.adminEndpoint != "":
		return target{}, errors.New("set --admin-endpoint or a target in Kubernetes, not both")
	case kubeFlags:
		return f.kubeTarget(), nil
	case f.adminEndpoint != "":
		return f.addressTarget(f.adminEndpoint)
	}

	kubeOn, err := kubeFromEnv()
	if err != nil {
		return target{}, err
	}
	endpoint := os.Getenv(adminEndpointEnv)
	switch {
	case kubeOn && endpoint != "":
		return target{}, fmt.Errorf("set %s or %s, not both", kubeEnv, adminEndpointEnv)
	case kubeOn:
		return f.kubeTarget(), nil
	case endpoint != "":
		return f.addressTarget(endpoint)
	case f.plugin:
		return f.kubeTarget(), nil
	default:
		return f.addressTarget(defaultAdminEndpoint)
	}
}

func (f *targetFlags) kubeTarget() target {
	kube := f.kube
	return target{kube: &kube, config: f.config}
}

func (f *targetFlags) addressTarget(address string) (target, error) {
	if f.kube.AdminPort != 0 || f.config.set() {
		return target{}, errors.New(
			"the flags of kubectl like -n, --context and --kubeconfig, and --admin-port, need a " +
				"target in Kubernetes: set --kube, --engine or --selector",
		)
	}
	return target{address: address}, nil
}

func kubeFromEnv() (bool, error) {
	value := os.Getenv(kubeEnv)
	if value == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s=%q is no boolean, e.g. 1 or true", kubeEnv, value)
	}
	return on, nil
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

// connect reaches the target. stream is the stream the command is about,
// empty if none: in Kubernetes without --engine or --selector, the pods are
// those of the Engines its Stream resource names.
func (t target) connect(ctx context.Context, stream string, errOut io.Writer) (connector, error) {
	if t.kube != nil {
		kube := *t.kube
		kube.Stream = stream
		return newKubeConnector(ctx, kube, t.config.clientConfig(), errOut)
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
