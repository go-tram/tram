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
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"

	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

// podPollInterval is how often the pods of a target are listed while
// commands run on each of them, to follow scaling and rollouts.
const podPollInterval = 2 * time.Second

// kubeTarget is TRAM pods in Kubernetes: the pods of an Engine, or the pods a
// label selector selects.
type kubeTarget struct {
	Engine     string `json:"engine,omitempty"`
	Selector   string `json:"selector,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	Context    string `json:"kube-context,omitempty"`
	Kubeconfig string `json:"kubeconfig,omitempty"`
	AdminPort  int    `json:"admin-port,omitempty"`
}

func (k kubeTarget) String() string {
	s := "pods " + k.Selector
	if k.Engine != "" {
		s = "engine " + k.Engine
	}
	if k.Namespace != "" {
		s += " in namespace " + k.Namespace
	}
	if k.Context != "" {
		s += " of kube context " + k.Context
	}
	return s
}

// with returns k with the fields of flags that are set.
func (k kubeTarget) with(flags kubeTarget) kubeTarget {
	if flags.Namespace != "" {
		k.Namespace = flags.Namespace
	}
	if flags.Context != "" {
		k.Context = flags.Context
	}
	if flags.Kubeconfig != "" {
		k.Kubeconfig = flags.Kubeconfig
	}
	if flags.AdminPort != 0 {
		k.AdminPort = flags.AdminPort
	}
	return k
}

// kubeConnector reaches TRAM pods through port forwards, as kubectl
// port-forward does, so it needs the same permissions: get and list pods and
// create pods/portforward, and get Engines for an Engine.
type kubeConnector struct {
	target    kubeTarget
	config    *rest.Config
	clientset kubernetes.Interface
	namespace string
	selector  labels.Selector
	port      int
	errOut    io.Writer
}

func newKubeConnector(ctx context.Context, t kubeTarget, errOut io.Writer) (*kubeConnector, error) {
	// client-go logs lost connections of port forwards through klog; the
	// connector reports what matters itself.
	klog.SetLogger(logr.Discard())

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = t.Kubeconfig
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{CurrentContext: t.Context})
	config, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	namespace := t.Namespace
	if namespace == "" {
		if namespace, _, err = clientConfig.Namespace(); err != nil {
			return nil, fmt.Errorf("kubeconfig: %w", err)
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	k := &kubeConnector{
		target: t, config: config, clientset: clientset, namespace: namespace, port: t.AdminPort, errOut: errOut,
	}
	if t.Engine != "" {
		if err = k.fromEngine(ctx); err != nil {
			return nil, err
		}
	} else if k.selector, err = labels.Parse(t.Selector); err != nil {
		return nil, fmt.Errorf("selector %q: %w", t.Selector, err)
	}
	if k.port == 0 {
		k.port = tramv1alpha1.DefaultAdminPort
	}
	return k, nil
}

// fromEngine selects the pods of the Engine and takes its admin port.
func (k *kubeConnector) fromEngine(ctx context.Context) error {
	client, err := dynamic.NewForConfig(k.config)
	if err != nil {
		return err
	}
	engines := schema.GroupVersionResource{
		Group: tramv1alpha1.GroupVersion.Group, Version: tramv1alpha1.GroupVersion.Version, Resource: "engines",
	}
	engine, err := client.Resource(engines).Namespace(k.namespace).Get(ctx, k.target.Engine, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("engine %s in namespace %s: %w", k.target.Engine, k.namespace, err)
	}
	if k.port == 0 {
		port, found, _ := unstructured.NestedInt64(engine.Object, "spec", "runtime", "api", "adminPort")
		if found {
			k.port = int(port)
		}
	}
	k.selector = labels.SelectorFromSet(labels.Set{tramv1alpha1.EngineLabel: k.target.Engine})
	return nil
}

func (k *kubeConnector) many() bool { return true }

// pods lists the ready pods of the target, sorted by name.
func (k *kubeConnector) pods(ctx context.Context) ([]string, error) {
	list, err := k.clientset.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: k.selector.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list pods of %s: %w", k.target, err)
	}
	var names []string
	for _, pod := range list.Items {
		if ready(&pod) {
			names = append(names, pod.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func ready(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (k *kubeConnector) one(ctx context.Context) (instance, func(), error) {
	pods, err := k.pods(ctx)
	if err != nil {
		return instance{}, nil, err
	}
	if len(pods) == 0 {
		return instance{}, nil, fmt.Errorf("no ready pods of %s", k.target)
	}
	ctx, cancel := context.WithCancel(ctx)
	in, err := k.connectPod(ctx, pods[0])
	if err != nil {
		cancel()
		return instance{}, nil, err
	}
	return in, func() { _ = in.conn.Close(); cancel() }, nil
}

func (k *kubeConnector) each(ctx context.Context, fn func(context.Context, instance) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	p := &podRuns{k: k, fn: fn, running: map[string]context.CancelFunc{}, fail: cancel}
	defer p.wg.Wait()
	ticker := time.NewTicker(podPollInterval)
	defer ticker.Stop()
	for {
		pods, err := k.pods(ctx)
		if err != nil && ctx.Err() == nil {
			return err
		}
		p.update(ctx, pods)

		select {
		case <-ctx.Done():
			if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
				return cause
			}
			return nil
		case <-ticker.C:
		}
	}
}

// podRuns runs fn for the pods that are ready, one run per pod.
type podRuns struct {
	k       *kubeConnector
	fn      func(context.Context, instance) error
	wg      sync.WaitGroup
	running map[string]context.CancelFunc
	waiting bool
	// fail ends all runs with the error of one.
	fail context.CancelCauseFunc
}

// update starts the runs of new pods and stops those of pods that are gone.
func (p *podRuns) update(ctx context.Context, pods []string) {
	for _, pod := range pods {
		if _, ok := p.running[pod]; ok {
			continue
		}
		podCtx, stop := context.WithCancel(ctx)
		p.running[pod] = stop
		p.wg.Go(func() {
			defer stop()
			if err := p.k.run(podCtx, pod, p.fn); err != nil {
				p.fail(err)
			}
		})
	}
	for pod, stop := range p.running {
		if !slices.Contains(pods, pod) {
			stop()
			delete(p.running, pod)
			_, _ = fmt.Fprintf(p.k.errOut, "pod %s is gone\n", pod)
		}
	}
	if len(p.running) == 0 && !p.waiting {
		_, _ = fmt.Fprintf(p.k.errOut, "waiting for ready pods of %s\n", p.k.target)
	}
	p.waiting = len(p.running) == 0
}

// run calls fn for pod. A pod that cannot be reached is reported and skipped:
// it may be going away.
func (k *kubeConnector) run(ctx context.Context, pod string, fn func(context.Context, instance) error) error {
	in, err := k.connectPod(ctx, pod)
	if err != nil {
		if ctx.Err() == nil {
			_, _ = fmt.Fprintf(k.errOut, "pod %s: %v\n", pod, err)
		}
		return nil
	}
	defer in.conn.Close()
	return fn(ctx, in)
}

// connectPod forwards a local port to the admin port of pod until ctx ends
// and connects to it.
func (k *kubeConnector) connectPod(ctx context.Context, pod string) (instance, error) {
	address, err := k.forward(ctx, pod)
	if err != nil {
		return instance{}, err
	}
	conn, err := dial(address)
	if err != nil {
		return instance{}, err
	}
	return instance{name: pod, conn: conn}, nil
}

func (k *kubeConnector) forward(ctx context.Context, pod string) (string, error) {
	url := k.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(k.namespace).Name(pod).SubResource("portforward").URL()
	transport, upgrader, err := spdy.RoundTripperFor(k.config)
	if err != nil {
		return "", err
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	// Like kubectl, try port forwarding over websockets first.
	if tunneling, tunnelErr := portforward.NewSPDYOverWebsocketDialer(url, k.config); tunnelErr == nil {
		dialer = portforward.NewFallbackDialer(tunneling, dialer, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		})
	}

	stop := make(chan struct{})
	readyCh := make(chan struct{})
	forwarder, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"},
		[]string{"0:" + strconv.Itoa(k.port)}, stop, readyCh, io.Discard, io.Discard)
	if err != nil {
		return "", err
	}
	errs := make(chan error, 1)
	go func() { errs <- forwarder.ForwardPorts() }()
	select {
	case <-readyCh:
	case err = <-errs:
		return "", fmt.Errorf("port-forward to %s: %w", pod, err)
	case <-ctx.Done():
		close(stop)
		return "", ctx.Err()
	}
	context.AfterFunc(ctx, func() { close(stop) })
	ports, err := forwarder.GetPorts()
	if err != nil || len(ports) == 0 {
		return "", fmt.Errorf("port-forward to %s: no local port: %w", pod, err)
	}
	return "127.0.0.1:" + strconv.Itoa(int(ports[0].Local)), nil
}
