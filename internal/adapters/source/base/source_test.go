// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package base_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
)

type noopSubscriber struct{}

func (noopSubscriber) Name() string                                { return "noop" }
func (noopSubscriber) From() string                                { return "source" }
func (noopSubscriber) Stop() error                                 { return nil }
func (noopSubscriber) Notify(context.Context, *domain.Event) error { return nil }

type blockingSubscriber struct {
	name     string
	received atomic.Int32
	started  chan struct{}
	release  chan struct{}
}

func (s *blockingSubscriber) Name() string { return s.name }
func (s *blockingSubscriber) From() string { return "source" }
func (s *blockingSubscriber) Stop() error  { return nil }
func (s *blockingSubscriber) Notify(context.Context, *domain.Event) error {
	if s.release != nil {
		close(s.started)
		<-s.release
	}
	s.received.Add(1)
	return nil
}

func TestSetSubscribersWaitsForDeliveries(t *testing.T) {
	t.Parallel()

	src, err := base.New[struct{}]("source", nil, validator.New())
	require.NoError(t, err)

	old := &blockingSubscriber{name: "old", started: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, src.Subscribe(old))

	delivered := make(chan error, 1)
	go func() { delivered <- src.NotifySubscribers(t.Context(), &domain.Event{}) }()
	<-old.started

	replacement := &blockingSubscriber{name: "new"}
	replaced := make(chan struct{})
	go func() {
		src.SetSubscribers([]domain.Subscriber{replacement})
		close(replaced)
	}()

	select {
	case <-replaced:
		t.Fatal("SetSubscribers returned while an event was still being delivered to the old subscriber")
	case <-time.After(50 * time.Millisecond):
	}

	close(old.release)
	require.NoError(t, <-delivered)
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("SetSubscribers did not return after the delivery finished")
	}

	require.NoError(t, src.NotifySubscribers(t.Context(), &domain.Event{}))
	assert.Equal(t, int32(1), old.received.Load(), "the in-flight event reached the old subscriber")
	assert.Equal(t, int32(1), replacement.received.Load(), "later events reach only the new subscriber")
}

func BenchmarkNotifySubscribers(b *testing.B) {
	event := &domain.Event{}

	b.Run("baseline=direct-call", func(b *testing.B) {
		var sub domain.Subscriber = noopSubscriber{}
		b.ReportAllocs()
		cpu := benchcpu.Start()
		for b.Loop() {
			_ = sub.Notify(b.Context(), event)
		}
		cpu.Report(b)
	})

	for _, children := range []int{1, 2, 8} {
		src, err := base.New[struct{}]("source", nil, validator.New())
		if err != nil {
			b.Fatal(err)
		}
		for range children {
			_ = src.Subscribe(noopSubscriber{})
		}

		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			b.ReportAllocs()
			cpu := benchcpu.Start()
			for b.Loop() {
				_ = src.NotifySubscribers(b.Context(), event)
			}
			cpu.Report(b)
		})

		b.Run(fmt.Sprintf("children=%d/parallel", children), func(b *testing.B) {
			b.ReportAllocs()
			cpu := benchcpu.Start()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = src.NotifySubscribers(context.Background(), event)
				}
			})
			cpu.Report(b)
		})
	}
}
