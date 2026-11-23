// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package decryption_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/stage/decryption"
	"github.com/go-tram/tram/internal/adapters/stage/encryption"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

func BenchmarkNotifyLogs(b *testing.B) {
	variants := []struct {
		name       string
		attributes []string
	}{
		{name: "body-only"},
		{name: "body-plus-attributes", attributes: []string{"team", "attr-0", "attr-1", "attr-2", "attr-3"}},
	}

	for _, variant := range variants {
		enc, stage := newStages(b, variant.attributes)
		for _, shape := range otlpgen.Shapes() {
			shape.Teams = 2
			encrypted := encryptedLogs(b, enc, shape)

			b.Run(variant.name+"/"+shape.String(), func(b *testing.B) {
				b.ReportAllocs()
				cpu := benchcpu.Start()
				for b.Loop() {
					if notifyErr := stage.Notify(b.Context(), encrypted); notifyErr != nil {
						b.Fatal(notifyErr)
					}
				}
				cpu.Report(b)
				b.ReportMetric(float64(shape.Records*b.N)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}

// newStages returns an encryption stage and the decryption stage that
// reverses it, both encrypting the body and attributes.
func newStages(b *testing.B, attributes []string) (*encryption.Encryption, *decryption.Decryption) {
	b.Helper()
	enc, err := encryption.New(
		domain.StageSpec{Name: "enc", From: "source", Spec: localSpec(key, attributes...)},
		validator.New(),
		slog.Default(),
	)
	if err != nil {
		b.Fatal(err)
	}
	dec, err := decryption.New(
		domain.StageSpec{Name: "dec", From: "enc", Spec: localSpec(key, attributes...)},
		validator.New(),
		slog.Default(),
	)
	if err != nil {
		b.Fatal(err)
	}
	return enc, dec
}

// encryptedLogs returns logs of shape as enc passes them on.
func encryptedLogs(b *testing.B, enc *encryption.Encryption, shape otlpgen.Shape) *domain.Event {
	b.Helper()
	c := &capture{}
	if err := enc.Subscribe(c); err != nil {
		b.Fatal(err)
	}
	if err := enc.Notify(context.Background(), &domain.Event{Logs: otlpgen.Logs(shape)}); err != nil {
		b.Fatal(err)
	}
	if len(c.events) != 1 {
		b.Fatal("expected one encrypted event")
	}
	return c.events[0]
}
