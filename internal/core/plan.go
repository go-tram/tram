// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"context"
	"errors"
	"slices"

	"github.com/go-tram/tram/internal/core/domain"
)

// PlanStream validates spec and resolved like an upsert would and reports
// what applying them would do, without changing the running stream. It
// instantiates the components to validate their specs, then stops them again
// without running them. The source of a running stream is not instantiated
// again, because e.g. an OTLP source binds its port in New; a changed source
// is validated by the real upsert once it stopped the old stream.
//
// It holds the lock of the stream's name, so the stream it compares with is
// not replaced meanwhile; plans of other streams and changes to them run in
// parallel.
func (c *Core) PlanStream(_ context.Context, spec, resolved domain.StreamSpec) (*domain.StreamPlan, error) {
	tree, err := newResolvedTree(spec, resolved)
	if err != nil {
		return nil, err
	}

	if !c.begin() {
		return nil, domain.ErrCoreClosed
	}
	defer c.ops.Done()
	unlock := c.lockName(spec.Name)
	defer unlock()

	old, exists := c.lookup(spec.Name)
	running := exists && old.running()

	plan := &domain.StreamPlan{Stream: spec.Name}
	switch {
	case running && old.tree.Spec.Equal(resolved) && old.spec.Equal(spec):
		plan.Action = domain.StreamPlanActionNoop
		return plan, nil
	case running && old.tree.Spec.Source.Equal(resolved.Source):
		plan.Action = domain.StreamPlanActionHotSwap
	default:
		plan.Action = domain.StreamPlanActionRebuild
		plan.SourceReplaced = exists
	}

	if running {
		err = c.validateWithSource(tree, old)
	} else {
		err = c.validateAll(tree)
	}
	if err != nil {
		return nil, errors.Join(domain.ErrStreamBuildFailed, err)
	}

	plan.CreatedNodes, plan.StoppedNodes = planNodes(old, exists, resolved)
	return plan, nil
}

// validateWithSource builds the stages and sinks of tree on the running
// source of old, so hold times can be checked against its delivery timeout,
// and stops them again. The running source is not stopped.
func (c *Core) validateWithSource(tree *domain.Tree, old *stream) error {
	source, err := old.tree.Root().GetSource()
	if err != nil {
		return err
	}
	if err = tree.Root().AddSource(source); err != nil {
		return err
	}
	defer c.stopUnbuilt(tree, false)
	if err = c.createReceivers(tree); err != nil {
		return err
	}
	return checkHoldTimes(tree)
}

// validateAll builds the whole tree and stops it again. No stream is
// running, so the source's resources, e.g. its port, are free.
func (c *Core) validateAll(tree *domain.Tree) error {
	defer c.stopUnbuilt(tree, true)
	if err := c.createAll(tree); err != nil {
		return err
	}
	return checkHoldTimes(tree)
}

// planNodes lists the stages and sinks the upsert would create and stop.
func planNodes(old *stream, exists bool, resolved domain.StreamSpec) ([]string, []string) {
	created := nodeNames(resolved)
	var stopped []string
	if exists {
		stopped = nodeNames(old.spec)
	}
	return created, stopped
}

// nodeNames returns the sorted names of the stages and sinks of spec.
func nodeNames(spec domain.StreamSpec) []string {
	var names []string
	for _, stage := range spec.Stages {
		names = append(names, stage.Name)
	}
	for _, sink := range spec.Sinks {
		names = append(names, sink.Name)
	}
	slices.Sort(names)
	return names
}
