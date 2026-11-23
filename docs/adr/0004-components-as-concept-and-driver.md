# 0004: Components as Concept and Driver

- **Status:** Accepted
- **Date:** 2026-11-18

## Context

Several components do the same thing with different technologies: a broker sink publishes to NATS, Kafka or AMQP; encryption uses local keys or a KMS; validation uses CEL or rules. One component type per technology would repeat the shared behavior (destinations, batching, metrics, error handling) and its configuration in each.

## Decision

A component is a concept, and the technology it uses is its driver:

- The component's spec has a `driver` field and the options of the driver under a field named after it, e.g. `{"driver": "nats", "nats": {"url": …}}`
- Fields shared by all drivers stay at the component level; the driver interface is small and holds only what differs
- `driver` always means the technology the component talks to or evaluates with. Other axes get their own field, e.g. the `format` of the planned object storage sink ([0012](0012-object-storage-sink.md))
- Components that are their technology, like the OTLP source and sink, have no driver

## Consequences

- A new technology is a new driver, with the shared behavior, configuration and tests of its component for free
- Adding a driver to a component that has none is a breaking change of its configuration, see [0013](0013-value-map-driver.md)
