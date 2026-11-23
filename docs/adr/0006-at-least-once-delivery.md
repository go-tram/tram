# 0006: At-Least-Once Delivery Without Deduplication

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

Data can be lost or duplicated between a client and a sink: a sink fails after others accepted the data, a broker redelivers a message, an acknowledgement gets lost. Exactly-once would need deduplication by ID across restarts and instances, which OTLP data does not provide.

## Decision

Delivery is at-least-once: TRAM answers a request with success only once all sinks accepted the data, and otherwise answers so that the client retries. It does not deduplicate. Duplicates are left to the destination, which can deduplicate where it matters.

## Consequences

- A sink can receive the same data more than once, e.g. after a `503` that the client retries although some sinks had accepted the data; COMPONENTS.md documents the cases under Stream Behavior
- At-least-once holds only as far as the sender retries; agents and SDKs need retries enabled
