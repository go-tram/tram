// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package broker is the contract between the broker sink and source and the
// drivers for message brokers. The sink and source implement TRAM's semantics
// once: routing records to destinations, the publish window, workers and how
// a delivery ends. A driver maps them onto one broker's client and its means
// of flow control and redelivery.
//
// Messages carry plain OTLP data and no headers, so tools other than TRAM can
// produce and consume them.
package broker

import (
	"context"
	"log/slog"
	"time"
)

// Message is what a sink publishes.
type Message struct {
	// Destination is where the message goes: a NATS subject, a Kafka topic,
	// an AMQP routing key.
	Destination string
	// Key is the partition key of the message, empty without partitioning.
	// Drivers whose broker partitions by key use it.
	Key string
	// Body is one serialized OTLP LogsData, MetricsData or TracesData.
	Body []byte
	// Signal names what Body holds: logs, metrics or traces. Drivers of
	// external brokers do not transmit it, since messages carry no headers;
	// embedded drivers, like those of the buffer, store it, and their
	// deliveries implement Signaled.
	Signal string
}

// Signaled is implemented by deliveries that know the signal of their body,
// so their source needs no signal of its own.
type Signaled interface {
	Signal() string
}

// Counted is implemented by deliveries that know how often their message
// failed before, so the source can back off further with every failure.
type Counted interface {
	Attempts() int
}

// Publisher sends messages to its broker.
type Publisher interface {
	// CheckDestination returns an error wrapping ErrInvalidDestination if no
	// message could ever be published to dest, e.g. a NATS subject with a
	// wildcard. The sink rejects such events instead of retrying them.
	CheckDestination(dest string) error
	// MaxMessageSize is the largest body the broker accepts, 0 if unknown.
	// The sink rejects larger messages instead of retrying them forever.
	MaxMessageSize() int
	// Publish starts sending m and returns without waiting for the broker.
	// It blocks while the driver's window of unacknowledged messages is
	// full. A message larger than MaxMessageSize fails with
	// ErrMessageTooLarge.
	Publish(ctx context.Context, m Message) (Ack, error)
	// Close waits for messages in flight and closes the connection.
	Close() error
}

// Ack is the broker's answer to one published message.
type Ack interface {
	// Wait returns once the broker stored the message, or with the reason it
	// did not.
	Wait(ctx context.Context) error
}

// Subscriber receives the messages of one durable or temporary consumer.
type Subscriber interface {
	// Open creates or attaches the consumer. It fails if the broker has
	// nothing to consume from yet, e.g. no stream for the subject.
	Open(ctx context.Context) error
	// Next returns the next message. The driver buffers at most the capacity
	// it was created with, so a buffered message waits for at most one round
	// of handling. Next retries lost connections itself and returns only
	// with a message or ctx's error.
	Next(ctx context.Context) (Delivery, error)
	// Close closes the connection. Deliveries still open are redelivered by
	// the broker.
	Close() error
}

// Delivery is one received message. Exactly one of Ack, Retry and Reject ends
// it.
type Delivery interface {
	Body() []byte
	// Destination is where the message was published to.
	Destination() string
	// Ack confirms the message.
	Ack() error
	// Retry has the broker deliver the message again after delay.
	Retry(delay time.Duration) error
	// Reject drops the message for good, e.g. data that is not valid OTLP.
	Reject() error
}

// PublisherConfig is what the sink tells a driver about a publisher.
type PublisherConfig struct {
	// Name identifies the connection, e.g. in the broker's monitoring.
	Name string
	// MaxPending bounds the messages published but not yet acknowledged;
	// Publish blocks at the bound. Always above 0.
	MaxPending int
}

// SubscriberConfig is what the source tells a driver about a subscriber.
type SubscriberConfig struct {
	// Name identifies the connection, e.g. in the broker's monitoring.
	Name string
	// Capacity is the number of workers, the most messages the subscriber
	// buffers. Always above 0.
	Capacity int
}

// PublisherDriver creates the publishers of one kind of broker. It is all the
// broker sink sees of a driver, so a sink cannot subscribe. opts are the
// driver's options from the component spec; the driver parses and validates
// them.
type PublisherDriver interface {
	NewPublisher(opts map[string]any, cfg PublisherConfig, logger *slog.Logger) (Publisher, error)
	// CheckPublisher parses and validates opts like NewPublisher, without
	// connecting.
	CheckPublisher(opts map[string]any) error
}

// SubscriberDriver creates the subscribers of one kind of broker. It is all the
// broker source sees of a driver, so a source cannot publish. opts are the
// driver's options from the component spec; the driver parses and validates
// them.
type SubscriberDriver interface {
	// NewSubscriber connects; the consumer is attached by Open.
	NewSubscriber(opts map[string]any, cfg SubscriberConfig, logger *slog.Logger) (Subscriber, error)
	// CheckSubscriber parses and validates opts like NewSubscriber, without
	// connecting.
	CheckSubscriber(opts map[string]any) error
}

// Driver serves both sides of one kind of broker, sharing its connection
// options. The conformance suite tests the round trip through one.
type Driver interface {
	PublisherDriver
	SubscriberDriver
}
