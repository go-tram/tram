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
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
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

// kubeTarget is TRAM pods in Kubernetes: the pods of an Engine, the pods a
// label selector selects, or the pods of the Engines that run Stream.
type kubeTarget struct {
	Engine    string
	Selector  string
	AdminPort int
	// Stream is the stream a command is about, <namespace>.<name> for the
	// stream of a Stream resource; empty if none.
	Stream string
}

// kubeConnector reaches TRAM pods through port forwards, as kubectl
// port-forward does, so it needs the same permissions: get and list pods and
// create pods/portforward, and get and list Engines and Streams to find the
// pods of a stream.
type kubeConnector struct {
	config    *rest.Config
	clientset kubernetes.Interface
	dynamic   dynamic.Interface
	namespace string
	selector  labels.Selector
	// what is the target in messages, e.g. "engine telemetry".
	what string
	// adminPort is --admin-port, 0 for the port of each pod's Engine;
	// enginePorts holds those ports by Engine.
	adminPort   int
	enginePorts map[string]int
	errOut      io.Writer
}

func engineResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group: tramv1alpha1.GroupVersion.Group, Version: tramv1alpha1.GroupVersion.Version, Resource: "engines",
	}
}

func streamResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: tramv1alpha1.GroupVersion.Group, Version: "v1", Resource: "streams"}
}

// newKubeConnector reaches the pods of t in the cluster of clientConfig, the
// kubeconfig with the flags of kubectl.
func newKubeConnector(
	ctx context.Context,
	t kubeTarget,
	clientConfig clientcmd.ClientConfig,
	errOut io.Writer,
) (*kubeConnector, error) {
	// client-go logs lost connections of port forwards through klog; the
	// connector reports what matters itself.
	klog.SetLogger(logr.Discard())

	config, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	k := &kubeConnector{config: config, adminPort: t.AdminPort, enginePorts: map[string]int{}, errOut: errOut}
	if k.clientset, err = kubernetes.NewForConfig(config); err != nil {
		return nil, err
	}
	if k.dynamic, err = dynamic.NewForConfig(config); err != nil {
		return nil, err
	}

	// The stream of a Stream resource names its namespace; -n wins.
	streamNamespace, streamName, isResource := strings.Cut(t.Stream, ".")
	namespace, explicit, err := clientConfig.Namespace()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	k.namespace = namespace
	if !explicit && t.Engine == "" && t.Selector == "" && isResource {
		k.namespace = streamNamespace
	}

	if t.Selector != "" {
		if k.selector, err = labels.Parse(t.Selector); err != nil {
			return nil, fmt.Errorf("selector %q: %w", t.Selector, err)
		}
		k.what = "pods " + t.Selector
	} else {
		engines, enginesErr := k.engines(ctx, t.Engine, streamName, isResource)
		if enginesErr != nil {
			return nil, enginesErr
		}
		if err = k.selectEngines(ctx, engines); err != nil {
			return nil, err
		}
	}
	k.what += " in namespace " + k.namespace
	return k, nil
}

// engines finds the Engines to talk to: the one named, those the Stream
// resource of the stream names, or the only Engine of the namespace.
func (k *kubeConnector) engines(ctx context.Context, engine, stream string, isResource bool) ([]string, error) {
	if engine != "" {
		return []string{engine}, nil
	}
	if isResource {
		resource, err := k.dynamic.Resource(streamResource()).
			Namespace(k.namespace).
			Get(ctx, stream, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("stream resource %s in namespace %s: %w; name the pods with --engine or --selector",
				stream, k.namespace, err)
		}
		refs, _, _ := unstructured.NestedSlice(resource.Object, "spec", "engineRefs")
		var names []string
		for _, ref := range refs {
			if name, _, _ := unstructured.NestedString(asObject(ref), "name"); name != "" {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("stream resource %s in namespace %s names no Engine", stream, k.namespace)
		}
		return names, nil
	}
	list, err := k.dynamic.Resource(engineResource()).Namespace(k.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("engines in namespace %s: %w", k.namespace, err)
	}
	var names []string
	for _, item := range list.Items {
		names = append(names, item.GetName())
	}
	switch len(names) {
	case 1:
		return names, nil
	case 0:
		return nil, fmt.Errorf("no Engine in namespace %s; name the pods with --selector", k.namespace)
	default:
		slices.Sort(names)
		return nil, fmt.Errorf("several Engines in namespace %s (%s): choose one with --engine",
			k.namespace, strings.Join(names, ", "))
	}
}

func asObject(value any) map[string]any {
	object, _ := value.(map[string]any)
	return object
}

// selectEngines selects the pods of engines and takes their admin ports.
func (k *kubeConnector) selectEngines(ctx context.Context, engines []string) error {
	for _, name := range engines {
		engine, err := k.dynamic.Resource(engineResource()).Namespace(k.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("engine %s in namespace %s: %w", name, k.namespace, err)
		}
		port := tramv1alpha1.DefaultAdminPort
		if p, found, _ := unstructured.NestedInt64(engine.Object, "spec", "runtime", "api", "adminPort"); found {
			port = int(p)
		}
		k.enginePorts[name] = port
	}
	requirement, err := labels.NewRequirement(tramv1alpha1.EngineLabel, selection.In, engines)
	if err != nil {
		return err
	}
	k.selector = labels.NewSelector().Add(*requirement)
	k.what = "engine " + strings.Join(engines, ", ")
	return nil
}

func (k *kubeConnector) many() bool { return true }

// readyPod is a ready pod of the target with the port of its admin API.
type readyPod struct {
	name string
	port int
}

// pods lists the ready pods of the target, sorted by name.
func (k *kubeConnector) pods(ctx context.Context) ([]readyPod, error) {
	list, err := k.clientset.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: k.selector.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list pods of %s: %w", k.what, err)
	}
	var pods []readyPod
	for _, pod := range list.Items {
		if !ready(&pod) {
			continue
		}
		port := k.adminPort
		if port == 0 {
			port = k.enginePorts[pod.Labels[tramv1alpha1.EngineLabel]]
		}
		if port == 0 {
			port = tramv1alpha1.DefaultAdminPort
		}
		pods = append(pods, readyPod{name: pod.Name, port: port})
	}
	slices.SortFunc(pods, func(a, b readyPod) int { return strings.Compare(a.name, b.name) })
	return pods, nil
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
		return instance{}, nil, fmt.Errorf("no ready pods of %s", k.what)
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
func (p *podRuns) update(ctx context.Context, pods []readyPod) {
	for _, pod := range pods {
		if _, ok := p.running[pod.name]; ok {
			continue
		}
		podCtx, stop := context.WithCancel(ctx)
		p.running[pod.name] = stop
		p.wg.Go(func() {
			defer stop()
			if err := p.k.run(podCtx, pod, p.fn); err != nil {
				p.fail(err)
			}
		})
	}
	for pod, stop := range p.running {
		if !slices.ContainsFunc(pods, func(r readyPod) bool { return r.name == pod }) {
			stop()
			delete(p.running, pod)
			_, _ = fmt.Fprintf(p.k.errOut, "pod %s is gone\n", pod)
		}
	}
	if len(p.running) == 0 && !p.waiting {
		_, _ = fmt.Fprintf(p.k.errOut, "waiting for ready pods of %s\n", p.k.what)
	}
	p.waiting = len(p.running) == 0
}

// run calls fn for pod. A pod that cannot be reached is reported and skipped:
// it may be going away.
func (k *kubeConnector) run(ctx context.Context, pod readyPod, fn func(context.Context, instance) error) error {
	in, err := k.connectPod(ctx, pod)
	if err != nil {
		if ctx.Err() == nil {
			_, _ = fmt.Fprintf(k.errOut, "pod %s: %v\n", pod.name, err)
		}
		return nil
	}
	defer in.conn.Close()
	return fn(ctx, in)
}

// connectPod forwards a local port to the admin port of pod until ctx ends
// and connects to it.
func (k *kubeConnector) connectPod(ctx context.Context, pod readyPod) (instance, error) {
	address, err := k.forward(ctx, pod)
	if err != nil {
		return instance{}, err
	}
	conn, err := dial(address)
	if err != nil {
		return instance{}, err
	}
	return instance{name: pod.name, conn: conn}, nil
}

func (k *kubeConnector) forward(ctx context.Context, pod readyPod) (string, error) {
	url := k.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(k.namespace).Name(pod.name).SubResource("portforward").URL()
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
		[]string{"0:" + strconv.Itoa(pod.port)}, stop, readyCh, io.Discard, io.Discard)
	if err != nil {
		return "", err
	}
	errs := make(chan error, 1)
	go func() { errs <- forwarder.ForwardPorts() }()
	select {
	case <-readyCh:
	case err = <-errs:
		return "", fmt.Errorf("port-forward to %s: %w", pod.name, err)
	case <-ctx.Done():
		close(stop)
		return "", ctx.Err()
	}
	context.AfterFunc(ctx, func() { close(stop) })
	ports, err := forwarder.GetPorts()
	if err != nil || len(ports) == 0 {
		return "", fmt.Errorf("port-forward to %s: no local port: %w", pod.name, err)
	}
	return "127.0.0.1:" + strconv.Itoa(int(ports[0].Local)), nil
}
