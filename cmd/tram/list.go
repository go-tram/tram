// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	brokerdrivers "github.com/go-tram/tram/internal/adapters/broker/drivers"
	bufferdrivers "github.com/go-tram/tram/internal/adapters/buffer/drivers"
	cryptodrivers "github.com/go-tram/tram/internal/adapters/crypto/drivers"
	sinkbroker "github.com/go-tram/tram/internal/adapters/sink/broker"
	sinkbuffer "github.com/go-tram/tram/internal/adapters/sink/buffer"
	"github.com/go-tram/tram/internal/adapters/sink/debug"
	sinknoop "github.com/go-tram/tram/internal/adapters/sink/noop"
	sinkotlp "github.com/go-tram/tram/internal/adapters/sink/otlp"
	sourcebroker "github.com/go-tram/tram/internal/adapters/source/broker"
	sourcebuffer "github.com/go-tram/tram/internal/adapters/source/buffer"
	"github.com/go-tram/tram/internal/adapters/source/generator"
	sourceotlp "github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/adapters/stage/batch"
	"github.com/go-tram/tram/internal/adapters/stage/decryption"
	"github.com/go-tram/tram/internal/adapters/stage/delay"
	"github.com/go-tram/tram/internal/adapters/stage/encryption"
	"github.com/go-tram/tram/internal/adapters/stage/filter"
	filterdrivers "github.com/go-tram/tram/internal/adapters/stage/filter/drivers"
	stagenoop "github.com/go-tram/tram/internal/adapters/stage/noop"
	"github.com/go-tram/tram/internal/adapters/stage/transform"
	transformdrivers "github.com/go-tram/tram/internal/adapters/stage/transform/drivers"
	"github.com/go-tram/tram/internal/adapters/stage/validation"
	validationdrivers "github.com/go-tram/tram/internal/adapters/stage/validation/drivers"
	"github.com/go-tram/tram/internal/adapters/stage/valuemap"
	"github.com/go-tram/tram/internal/core/domain"
)

// component is an entry of tram list.
type component struct {
	Kind        domain.NodeKind `json:"kind"`
	Type        string          `json:"type"`
	Drivers     []string        `json:"drivers,omitempty"`
	Description string          `json:"description"`
}

// components lists every component, in the order of COMPONENTS.md, with the
// first sentence of its documentation. TestListMatchesTheFactories fails when
// a type is missing in its factory.
func components() []component {
	broker := brokerdrivers.Names()
	buffer := bufferdrivers.Names()
	crypto := cryptodrivers.Names()
	return []component{
		{domain.KindSource, sourceotlp.Type, nil, "Receives OTLP over HTTP, gRPC or both"},
		{domain.KindSource, sourcebroker.Type, broker, "Receives OTLP data from a message broker"},
		{domain.KindSource, sourcebuffer.Type, buffer, "Reads the queue a buffer sink of the same instance writes"},
		{
			domain.KindSource,
			generator.Type,
			nil,
			"Emits an empty event at a fixed interval, for testing how a stream is wired",
		},
		{domain.KindStage, validation.Type, validationdrivers.Names(),
			"Checks each event as a whole and passes, drops or rejects it"},
		{domain.KindStage, filter.Type, filterdrivers.Names(), "Keeps only the records that match its rules"},
		{domain.KindStage, transform.Type, transformdrivers.Names(),
			"Changes the attributes of records, scopes and resources"},
		{domain.KindStage, valuemap.Type, nil, "Sets an attribute to the value a table maps another attribute to"},
		{domain.KindStage, encryption.Type, crypto, "Encrypts the body and listed attributes of log records"},
		{domain.KindStage, decryption.Type, crypto, "Decrypts what the encryption stage encrypted"},
		{domain.KindStage, batch.Type, nil, "Merges events that arrive concurrently into one event"},
		{domain.KindStage, delay.Type, nil, "Holds every event back for a fixed duration"},
		{domain.KindStage, stagenoop.Type, nil, "Passes events on unchanged"},
		{domain.KindSink, sinkotlp.Type, nil, "Sends OTLP over HTTP to an OTLP endpoint"},
		{domain.KindSink, sinkbroker.Type, broker, "Sends OTLP data to a message broker"},
		{domain.KindSink, sinkbuffer.Type, buffer, "Stores every event durably in a queue on the instance's disk"},
		{domain.KindSink, debug.Type, nil, "Writes every event to the log"},
		{domain.KindSink, sinknoop.Type, nil, "Drops every event"},
	}
}

func newListCmd() *cobra.Command {
	var output string
	kinds := map[string]domain.NodeKind{
		"sources": domain.KindSource, "stages": domain.KindStage, "sinks": domain.KindSink,
	}
	cmd := &cobra.Command{
		Use:   "list [sources|stages|sinks]",
		Short: "List the components and their drivers",
		Long: `List the sources, stages and sinks this binary supports, with the drivers
of those that have them. COMPONENTS.md documents their specs.`,
		Args:      cobra.MatchAll(cobra.MaximumNArgs(1), cobra.OnlyValidArgs),
		ValidArgs: []string{"sources", "stages", "sinks"},
		RunE: func(cmd *cobra.Command, args []string) error {
			selected := components()
			if len(args) == 1 {
				selected = nil
				for _, c := range components() {
					if c.Kind == kinds[args[0]] {
						selected = append(selected, c)
					}
				}
			}
			switch output {
			case outputText:
				return writeComponents(cmd.OutOrStdout(), selected)
			case outputJSON:
				encoder := json.NewEncoder(cmd.OutOrStdout())
				encoder.SetIndent("", "  ")
				return encoder.Encode(selected)
			default:
				return fmt.Errorf("invalid --output %q: use %s or %s", output, outputText, outputJSON)
			}
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", outputText, "Output: text or json")
	return cmd
}

func writeComponents(out io.Writer, list []component) error {
	w := tabwriter.NewWriter(out, 0, 0, tablePadding, ' ', 0)
	_, _ = fmt.Fprintln(w, "KIND\tTYPE\tDRIVERS\tDESCRIPTION")
	for _, c := range list {
		drivers := strings.Join(c.Drivers, ", ")
		if drivers == "" {
			drivers = "-"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", c.Kind, c.Type, drivers, c.Description)
	}
	return w.Flush()
}
