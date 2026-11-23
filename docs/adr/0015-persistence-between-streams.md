# 0015: Streams Keep Nothing on Disk; Buffers Connect Two Streams

- **Status:** Accepted
- **Date:** 2026-11-21

## Context

Some setups need to answer clients before the destination has the data, e.g. to ride out an outage of the destination, and some have no broker such as NATS to put in between. A stream could get a write-ahead log of its own, e.g. as a stage that persists events before passing them on.

## Decision

A stream processes events only in memory and keeps nothing on disk ([0007](0007-synchronous-delivery-and-backpressure.md)). Writing ahead is built from two streams and a queue between them: a buffer sink stores events in a queue on the instance's disk (driver `sqlite`), and a buffer source in another stream reads and delivers them, retrying until the queue's `max_age`. The queue name connects the two. The buffer is a broker embedded in the instance, and its source and sink behave like the broker source and sink.

## Consequences

- Streams stay synchronous and stateless; whether and where data is persisted is visible in the configuration, not hidden in a stage
- The same pattern works with a real broker between the streams, so moving from a local buffer to NATS, Kafka or AMQP changes components, not the design
- A local queue is one copy on one instance: losing the disk loses the data, another instance cannot deliver its backlog, and scaling down needs the queue drained first, which the manager does for Engines with drain Jobs. Where that is not acceptable, a replicated broker between the streams is the choice
- No write-ahead logic per stage, branch or sink to build and test

## Alternatives

- **A write-ahead log inside a stream, e.g. a WAL stage:** would persist data in the middle of a synchronous pipeline, need tracking per branch for fan-out, and duplicate what a queue between streams already does
