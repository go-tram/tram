// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-tram/tram/internal/core/domain"
)

func newTree(spec domain.StreamSpec) (*domain.Tree, error) {
	tree, err := domain.NewTree(spec)
	if err != nil {
		return nil, err
	}

	if !tree.IsTree() {
		return nil, domain.ErrGraphNotTree
	}

	if !tree.IsConnected() {
		return nil, domain.ErrGraphNotConnected
	}

	for _, node := range tree.LeafNodes() {
		if node.Kind != domain.KindSink {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, domain.ErrGraphLeafNotSink)
		}
	}

	for _, node := range tree.Map() {
		if node.Kind == domain.KindSink && len(node.Children) != 0 {
			return nil, errors.Join(domain.ErrInvalidStreamSpec, domain.ErrGraphSinkHasChildren)
		}
	}

	return tree, nil
}

func (c *Core) buildStream(tree *domain.Tree) error {
	err := c.createAll(tree)
	if err == nil {
		err = checkHoldTimes(tree)
	}
	if err == nil {
		err = c.wire(tree, true)
	}
	if err != nil {
		c.stopUnbuilt(tree, true)
	}
	return err
}

func (c *Core) createAll(tree *domain.Tree) error {
	spec := tree.Spec
	source, err := c.sourceFactory.New(spec.Source, spec.Name)
	if err != nil {
		return invalidSpec(spec.Source.Name, err)
	}
	if err = tree.Root().AddSource(source); err != nil {
		return invalidSpec(spec.Source.Name, err)
	}
	return c.createReceivers(tree)
}

func (c *Core) buildReceivers(tree *domain.Tree) error {
	err := c.createReceivers(tree)
	if err == nil {
		err = checkHoldTimes(tree)
	}
	if err == nil {
		err = c.wire(tree, false)
	}
	if err != nil {
		c.stopUnbuilt(tree, false)
	}
	return err
}

func (c *Core) stopUnbuilt(tree *domain.Tree, includeSource bool) {
	if err := stopNodes(tree, includeSource); err != nil {
		c.logger.ErrorContext(c.ctx, "Could not stop components of unbuilt stream",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
	}
}

func (c *Core) createReceivers(tree *domain.Tree) error {
	for _, stageSpec := range tree.Spec.Stages {
		stage, err := c.stageFactory.New(stageSpec, tree.Spec.Name)
		if err != nil {
			return invalidSpec(stageSpec.Name, err)
		}
		if err = tree.GetNode(stageSpec.Name).AddStage(stage); err != nil {
			return invalidSpec(stageSpec.Name, err)
		}
	}

	for _, sinkSpec := range tree.Spec.Sinks {
		sink, err := c.sinkFactory.New(sinkSpec, tree.Spec.Name)
		if err != nil {
			return invalidSpec(sinkSpec.Name, err)
		}
		if err = tree.GetNode(sinkSpec.Name).AddSink(sink); err != nil {
			return invalidSpec(sinkSpec.Name, err)
		}
	}
	return nil
}

func (c *Core) wire(tree *domain.Tree, includeRoot bool) error {
	root := tree.Root()
	for _, node := range tree.Map() {
		if node == root || len(node.Children) == 0 {
			continue
		}
		observer, err := node.GetObserver()
		if err != nil {
			return err
		}
		subscriber, err := c.fanoutOf(tree, node)
		if err != nil {
			return err
		}
		if err = observer.Subscribe(subscriber); err != nil {
			return err
		}
	}
	if !includeRoot {
		return nil
	}

	source, err := root.GetObserver()
	if err != nil {
		return err
	}
	subscriber, err := c.sourceSubscriber(tree)
	if err != nil {
		return err
	}
	return source.Subscribe(subscriber)
}

func (c *Core) subscriber(tree *domain.Tree, node *domain.Node) (domain.Subscriber, error) {
	subscriber, err := node.GetSubscriber()
	if err != nil {
		return nil, err
	}
	return c.metrics.instrument(tree.Spec, node, subscriber), nil
}

func (c *Core) fanoutOf(tree *domain.Tree, node *domain.Node) (*fanout, error) {
	children := make([]domain.Subscriber, 0, len(node.Children))
	sinks := make([]string, 0, len(node.Children))
	for _, child := range node.Children {
		subscriber, err := c.subscriber(tree, child)
		if err != nil {
			return nil, err
		}
		children = append(children, subscriber)
		sink := ""
		if child.Kind == domain.KindSink {
			sink = child.Name
		}
		sinks = append(sinks, sink)
	}
	return &fanout{
		name:     node.Name,
		children: children,
		out:      c.metrics.output(tree.Spec, node),
		stream:   tree.Spec.Name,
		kind:     node.Kind,
		sinks:    sinks,
		taps:     c.taps,
	}, nil
}

func (c *Core) sourceSubscriber(tree *domain.Tree) (domain.Subscriber, error) {
	root := tree.Root()
	f, err := c.fanoutOf(tree, root)
	if err != nil {
		return nil, err
	}
	return c.metrics.instrument(tree.Spec, root, f), nil
}

// replaceReceivers builds the stages and sinks of tree on the running source
// of old and returns the stream that replaces old.
func (c *Core) replaceReceivers(old *stream, tree *domain.Tree, spec domain.StreamSpec) (*stream, error) {
	source, err := old.tree.Root().GetSource()
	if err != nil {
		return nil, err
	}
	if err = tree.Root().AddSource(source); err != nil {
		return nil, err
	}
	if err = c.buildReceivers(tree); err != nil {
		return nil, err
	}

	subscriber, err := c.sourceSubscriber(tree)
	if err != nil {
		return nil, err
	}

	source.SetSubscribers([]domain.Subscriber{subscriber})
	if err = stopNodes(old.tree, false); err != nil {
		c.logger.ErrorContext(c.ctx, "Could not cleanly stop replaced components of stream",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
	}

	return &stream{tree: tree, spec: spec, done: old.done}, nil
}

// runStream runs the source of tree and returns its stream, which the caller
// registers under the lock of the stream's name.
func (c *Core) runStream(tree *domain.Tree, spec domain.StreamSpec, restarts int) *stream {
	s := &stream{
		tree: tree,
		spec: spec,
		done: make(chan struct{}),
	}

	c.wg.Go(func() {
		started := time.Now()
		c.runSource(tree)
		close(s.done)

		if time.Since(started) >= c.maxRestartDelay {
			restarts = 0
		}
		c.scheduleRestart(tree.Spec.Name, s.done, restarts)
	})
	return s
}

func (c *Core) runSource(tree *domain.Tree) {
	c.logger.InfoContext(c.ctx, "Running stream", slog.String("stream", tree.Spec.Name))
	source, err := tree.Root().GetSource()
	if err != nil {
		c.logger.ErrorContext(c.ctx, "Could not get source of stream",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
		return
	}
	if err = source.Run(c.ctx); err != nil {
		c.logger.ErrorContext(c.ctx, "Stream stopped with an error",
			slog.String("stream", tree.Spec.Name), slog.Any("error", err))
	}
}

func (c *Core) scheduleRestart(name string, done chan struct{}, restarts int) {
	if c.ctx.Err() != nil {
		return
	}
	time.AfterFunc(c.restartDelay(restarts), func() {
		c.restartStream(name, done, restarts+1)
	})
}

func (c *Core) restartDelay(restarts int) time.Duration {
	delay := c.minRestartDelay
	for range restarts {
		delay *= 2
		if delay >= c.maxRestartDelay {
			return c.maxRestartDelay
		}
	}
	return delay
}

func (c *Core) restartStream(name string, done chan struct{}, restarts int) {
	if !c.begin() {
		return
	}
	defer c.ops.Done()
	unlock := c.lockName(name)
	defer unlock()

	// The stream may have been replaced or deleted meanwhile.
	old, exists := c.lookup(name)
	if !exists || old.done != done {
		return
	}

	c.logger.WarnContext(c.ctx, "Restarting stream whose source stopped",
		slog.String("stream", name), slog.Int("restart", restarts))
	if err := c.stopStream(old); err != nil {
		c.logger.ErrorContext(c.ctx, "Could not cleanly stop stream",
			slog.String("stream", name), slog.Any("error", err))
	}

	tree, err := newTree(old.tree.Spec)
	if err == nil {
		err = c.buildStream(tree)
	}
	if err != nil {
		c.logger.ErrorContext(c.ctx, "Could not restart stream",
			slog.String("stream", name), slog.Any("error", err))
		c.scheduleRestart(name, done, restarts)
		return
	}
	c.replace(name, c.runStream(tree, old.spec, restarts))
}

// restoreStream builds and runs the stream of a failed update again and
// returns it, or nil if that fails too.
func (c *Core) restoreStream(resolved, spec domain.StreamSpec) *stream {
	tree, err := newTree(resolved)
	if err == nil {
		err = c.buildStream(tree)
	}
	if err != nil {
		c.logger.ErrorContext(c.ctx, "Could not restore stream after failed update, retrying later",
			slog.String("stream", spec.Name), slog.Any("error", err))
		return nil
	}
	if resolved.Suspended {
		return &stream{tree: tree, spec: spec}
	}
	return c.runStream(tree, spec, 0)
}

func (c *Core) stopStream(s *stream) error {
	if s.stopped {
		return nil
	}
	s.stopped = true

	source, err := s.tree.Root().GetSource()
	if err != nil {
		return errors.Join(domain.ErrStreamStopFailed, err)
	}

	var errs []error
	if err = source.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("%s: %w", source.Name(), err))
	}
	if s.done != nil {
		<-s.done
	}

	if err = stopNodes(s.tree, false); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(append([]error{domain.ErrStreamStopFailed}, errs...)...)
	}
	return nil
}

// stopNodes stops every created stage and sink of tree, and the source if
// includeSource is set. Nodes are stopped top-down, so a stage that still
// holds events (like a batch) can flush them to children that are running.
func stopNodes(tree *domain.Tree, includeSource bool) error {
	var errs []error
	queue := []*domain.Node{tree.Root()}
	for len(queue) > 0 {
		node := queue[0]
		queue = append(queue[1:], node.Children...)

		if node.Kind == domain.KindSource && !includeSource {
			continue
		}
		stoppable, ok := node.Processor.(domain.Stoppable)
		if !ok {
			continue
		}
		if err := stoppable.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", node.Name, err))
		}
	}
	return errors.Join(errs...)
}

// invalidSpec reports that the spec of the named node is invalid.
func invalidSpec(name string, err error) error {
	return errors.Join(fmt.Errorf("%w: %s", domain.ErrInvalidStreamSpec, name), err)
}
