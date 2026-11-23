// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package kafkatest starts a Kafka broker in a container for tests.
package kafkatest

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/go-tram/tram/internal/testutil/containertest"
)

const (
	topicTimeout = 10 * time.Second
	topicPoll    = 50 * time.Millisecond
)

// Image is the only image the testcontainers module supports, Kafka in KRaft
// mode.
const Image = "confluentinc/confluent-local:7.5.0"

// Start starts Kafka and returns the addresses of its brokers. The test is
// skipped without a container runtime.
func Start(tb testing.TB) []string {
	tb.Helper()
	ctx := context.Background()
	containertest.SkipWithoutRuntime(ctx, tb)

	container, err := tckafka.Run(ctx, Image, tckafka.WithClusterID("tram-test"))
	testcontainers.CleanupContainer(tb, container)
	if err != nil {
		tb.Fatalf("start Kafka container: %v", err)
	}
	brokers, err := container.Brokers(ctx)
	if err != nil {
		tb.Fatalf("get Kafka brokers: %v", err)
	}
	return brokers
}

// CreateTopic creates topic with partitions, as a deployment does outside
// TRAM.
func CreateTopic(tb testing.TB, brokers []string, topic string, partitions int32) {
	tb.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		tb.Fatalf("connect to Kafka: %v", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	resp, err := admin.CreateTopic(context.Background(), partitions, 1, nil, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		tb.Fatalf("create topic %s: %v", topic, err)
	}
	// Kafka spreads a new topic's metadata with a delay; until then it looks
	// missing to clients.
	deadline := time.Now().Add(topicTimeout)
	for time.Now().Before(deadline) {
		topics, listErr := admin.ListTopics(context.Background(), topic)
		if detail, ok := topics[topic]; listErr == nil && ok && detail.Err == nil {
			return
		}
		time.Sleep(topicPoll)
	}
	tb.Fatalf("topic %s did not appear within %s", topic, topicTimeout)
}

// DeleteTopic deletes topic and the data in it, e.g. after a benchmark that
// filled it.
func DeleteTopic(tb testing.TB, brokers []string, topic string) {
	tb.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		tb.Errorf("connect to Kafka: %v", err)
		return
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).DeleteTopic(context.Background(), topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		tb.Errorf("delete topic %s: %v", topic, err)
	}
}
