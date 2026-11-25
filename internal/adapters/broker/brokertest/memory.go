// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package brokertest has an in-memory broker driver for testing the broker
// sink and source without a container, and a conformance suite every driver
// has to pass.
package brokertest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/go-tram/tram/internal/adapters/broker"
)

// MemoryName is the name to register the memory driver under.
const MemoryName = "memory"

// ErrPublishFailed is returned by publishes to a destination set with Fail.
var ErrPublishFailed = errors.New("publish failed")

// Memory is an in-memory broker and its driver. Subscribers read the messages
// of one destination, set by the option "destination"; each subscriber with
// the same option "durable" shares one queue, like a durable consumer.
type Memory struct {
	mu        sync.Mutex
	queues    map[string]*queue // by destination and durable
	published []broker.Message
	failing   map[string]bool
	maxSize   int
}

var _ broker.Driver = (*Memory)(nil)

// NewMemory returns an empty broker. maxSize is the largest body it accepts,
// 0 for no limit.
func NewMemory(maxSize int) *Memory {
	return &Memory{queues: map[string]*queue{}, failing: map[string]bool{}, maxSize: maxSize}
}

// Published returns all messages published so far.
func (m *Memory) Published() []broker.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]broker.Message(nil), m.published...)
}

// Fail makes publishes to dest fail, as if the broker refused them.
func (m *Memory) Fail(dest string, fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failing[dest] = fail
}

// Outcome counts how the deliveries of a queue ended.
type Outcome struct {
	Acked, Retried, Rejected, Open int
}

// Outcome returns the counts of the queue of dest and durable.
func (m *Memory) Outcome(dest, durable string) Outcome {
	q := m.queue(dest, durable)
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.outcome
}

// Declare creates the queue of dest and durable ahead of its subscriber, like
// a durable consumer that exists before the source starts, so messages
// published before the source opens wait for it.
func (m *Memory) Declare(dest, durable string) {
	m.queue(dest, durable)
}

// Inject delivers body to the queues of dest as if it had been published by
// another producer.
func (m *Memory) Inject(dest string, body []byte) {
	m.deliver(broker.Message{Destination: dest, Body: body})
}

func (m *Memory) queue(dest, durable string) *queue {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := dest + "\x00" + durable
	q, ok := m.queues[key]
	if !ok {
		q = &queue{dest: dest, ready: make(chan struct{}, 1)}
		m.queues[key] = q
	}
	return q
}

func (m *Memory) deliver(msg broker.Message) {
	m.mu.Lock()
	var queues []*queue
	for _, q := range m.queues {
		if q.dest == msg.Destination {
			queues = append(queues, q)
		}
	}
	m.mu.Unlock()
	for _, q := range queues {
		q.push(msg.Body)
	}
}

func (m *Memory) CheckPublisher(map[string]any) error { return nil }

func (m *Memory) CheckSubscriber(opts map[string]any) error {
	if dest, _ := opts["destination"].(string); dest == "" {
		return errors.New("memory subscriber needs a destination")
	}
	return nil
}

func (m *Memory) NewPublisher(map[string]any, broker.PublisherConfig, *slog.Logger) (broker.Publisher, error) {
	return &memoryPublisher{m: m}, nil
}

func (m *Memory) NewSubscriber(
	opts map[string]any,
	cfg broker.SubscriberConfig,
	_ *slog.Logger,
) (broker.Subscriber, error) {
	dest, _ := opts["destination"].(string)
	if dest == "" {
		return nil, errors.New("memory subscriber needs a destination")
	}
	durable, _ := opts["durable"].(string)
	return &memorySubscriber{m: m, dest: dest, durable: durable, capacity: cfg.Capacity}, nil
}

type memoryPublisher struct{ m *Memory }

func (p *memoryPublisher) CheckDestination(dest string) error {
	if dest == "" || strings.ContainsAny(dest, " *>") {
		return fmt.Errorf("%w: %q", broker.ErrInvalidDestination, dest)
	}
	return nil
}

func (p *memoryPublisher) MaxMessageSize() int { return p.m.maxSize }

func (p *memoryPublisher) Publish(_ context.Context, msg broker.Message) (broker.Ack, error) {
	if p.m.maxSize > 0 && len(msg.Body) > p.m.maxSize {
		return nil, fmt.Errorf("%w: %d bytes", broker.ErrMessageTooLarge, len(msg.Body))
	}
	p.m.mu.Lock()
	failing := p.m.failing[msg.Destination]
	if !failing {
		p.m.published = append(p.m.published, msg)
	}
	p.m.mu.Unlock()
	if failing {
		return memoryAck{err: fmt.Errorf("%w: %s", ErrPublishFailed, msg.Destination)}, nil
	}
	p.m.deliver(msg)
	return memoryAck{}, nil
}

func (p *memoryPublisher) Close() error { return nil }

type memoryAck struct{ err error }

func (a memoryAck) Wait(context.Context) error { return a.err }

type queue struct {
	dest    string
	mu      sync.Mutex
	items   [][]byte
	ready   chan struct{}
	outcome Outcome
}

func (q *queue) push(body []byte) {
	q.mu.Lock()
	q.items = append(q.items, body)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *queue) pop() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, false
	}
	body := q.items[0]
	q.items = q.items[1:]
	if len(q.items) > 0 {
		select {
		case q.ready <- struct{}{}:
		default:
		}
	}
	q.outcome.Open++
	return body, true
}

type memorySubscriber struct {
	m        *Memory
	dest     string
	durable  string
	capacity int
	q        *queue
}

func (s *memorySubscriber) Open(context.Context) error {
	s.q = s.m.queue(s.dest, s.durable)
	return nil
}

func (s *memorySubscriber) Next(ctx context.Context) (broker.Delivery, error) {
	for {
		if body, ok := s.q.pop(); ok {
			return &memoryDelivery{q: s.q, dest: s.dest, body: body}, nil
		}
		select {
		case <-s.q.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *memorySubscriber) Close() error { return nil }

type memoryDelivery struct {
	q    *queue
	dest string
	body []byte
	once sync.Once
}

func (d *memoryDelivery) Body() []byte        { return d.body }
func (d *memoryDelivery) Destination() string { return d.dest }

func (d *memoryDelivery) end(count func(*Outcome)) error {
	ended := false
	d.once.Do(func() {
		d.q.mu.Lock()
		count(&d.q.outcome)
		d.q.outcome.Open--
		d.q.mu.Unlock()
		ended = true
	})
	if !ended {
		return errors.New("delivery already ended")
	}
	return nil
}

func (d *memoryDelivery) Ack() error { return d.end(func(o *Outcome) { o.Acked++ }) }

func (d *memoryDelivery) Reject() error { return d.end(func(o *Outcome) { o.Rejected++ }) }

func (d *memoryDelivery) Retry(delay time.Duration) error {
	err := d.end(func(o *Outcome) { o.Retried++ })
	if err == nil {
		time.AfterFunc(delay, func() { d.q.push(d.body) })
	}
	return err
}
