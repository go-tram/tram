# 0007: Synchronous Delivery and Backpressure

- **Status:** Accepted
- **Date:** 2026-10-03

## Context

A pipeline can acknowledge data when it arrives and deliver it later from a queue, or acknowledge it only once it is delivered. The first decouples senders from slow sinks, but data in the queue is lost when the process dies, and the sender cannot tell.

## Decision

TRAM acknowledges a request only after the stream delivered it. A batch stage holds every request until its batch is delivered, so a sender waits up to the batch's timeout. Slow or failing sinks slow down senders instead of filling a queue. Where data has to survive a sink's outage, a buffer sink or a broker between streams is configured explicitly, and the acknowledgement then means "stored".

## Consequences

- Senders feel backpressure: with a batch stage, a sender that sends one request at a time gets one request through per batch timeout; concurrency on the sender side raises throughput
- `delivery_timeout`, batch timeouts and the senders' timeouts have to fit together, see Hold Times in COMPONENTS.md
- Nothing is acknowledged that TRAM could still lose in memory
