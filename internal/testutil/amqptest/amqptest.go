// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package amqptest starts an AMQP 1.0 broker, ActiveMQ Artemis, in a container
// for tests.
package amqptest

import (
	"context"
	"io"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/go-tram/tram/internal/testutil/containertest"
)

const (
	Image = "apache/activemq-artemis:2.38.0-alpine"

	amqpPort = "5672/tcp"
)

// adminUser is the user and password of the container's administrator, for
// the artemis command line.
const adminUser = "artemis"

// Broker is a running Artemis.
type Broker struct {
	URL       string
	container testcontainers.Container
}

// Start starts Artemis with anonymous AMQP access. The test is skipped without
// a container runtime.
func Start(tb testing.TB) *Broker {
	tb.Helper()
	ctx := context.Background()
	containertest.SkipWithoutRuntime(ctx, tb)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Image:        Image,
		ExposedPorts: []string{amqpPort},
		Env:          map[string]string{"ANONYMOUS_LOGIN": "true"},
		WaitingFor:   wait.ForAll(wait.ForLog("AMQ221007"), wait.ForListeningPort(amqpPort)),
		Started:      true,
	})
	testcontainers.CleanupContainer(tb, container)
	if err != nil {
		tb.Fatalf("start Artemis container: %v", err)
	}
	endpoint, err := container.PortEndpoint(ctx, amqpPort, "")
	if err != nil {
		tb.Fatalf("get Artemis AMQP endpoint: %v", err)
	}
	return &Broker{URL: "amqp://" + endpoint, container: container}
}

// CreateQueue creates a durable anycast queue with its address, as a
// deployment does outside TRAM. Unlike queues Artemis creates on demand, it
// stays when its last consumer leaves.
func (b *Broker) CreateQueue(tb testing.TB, name string) {
	tb.Helper()
	code, out, err := b.container.Exec(context.Background(), []string{
		"/var/lib/artemis-instance/bin/artemis", "queue", "create",
		"--name", name, "--address", name, "--anycast", "--durable", "--auto-create-address",
		"--silent", "--user", adminUser, "--password", adminUser,
	})
	if err != nil {
		tb.Fatalf("create queue %s: %v", name, err)
	}
	if code != 0 {
		output, _ := io.ReadAll(out)
		tb.Fatalf("create queue %s: exit code %d: %s", name, code, string(output))
	}
}

// DeleteQueue deletes the queue and its messages, e.g. after a benchmark
// that filled it.
func (b *Broker) DeleteQueue(tb testing.TB, name string) {
	tb.Helper()
	code, out, err := b.container.Exec(context.Background(), []string{
		"/var/lib/artemis-instance/bin/artemis", "queue", "delete",
		"--name", name, "--removeConsumers", "--autoDeleteAddress",
		"--silent", "--user", adminUser, "--password", adminUser,
	})
	if err != nil {
		tb.Errorf("delete queue %s: %v", name, err)
		return
	}
	if code != 0 {
		output, _ := io.ReadAll(out)
		tb.Errorf("delete queue %s: exit code %d: %s", name, code, string(output))
	}
}
