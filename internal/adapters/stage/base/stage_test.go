// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package base_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/stage/base"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
)

type noopSubscriber struct{}

func (noopSubscriber) Name() string                                { return "noop" }
func (noopSubscriber) From() string                                { return "stage" }
func (noopSubscriber) Stop() error                                 { return nil }
func (noopSubscriber) Notify(context.Context, *domain.Event) error { return nil }

func BenchmarkNotifySubscribers(b *testing.B) {
	event := &domain.Event{}
	ctx := context.WithValue(context.Background(), domain.ContextKeyPath, "source")

	for _, children := range []int{1, 2, 8} {
		stage, err := base.New[struct{}]("stage", "source", nil, validator.New())
		if err != nil {
			b.Fatal(err)
		}
		for range children {
			_ = stage.Subscribe(noopSubscriber{})
		}

		b.Run(fmt.Sprintf("children=%d", children), func(b *testing.B) {
			b.ReportAllocs()
			cpu := benchcpu.Start()
			for b.Loop() {
				_ = stage.NotifySubscribers(ctx, event)
			}
			cpu.Report(b)
		})
	}
}
