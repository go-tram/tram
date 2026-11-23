# 0003: Ports and Adapters

- **Status:** Accepted

## Context

TRAM is configured from three places (API, files, Kubernetes resources) and talks to many technologies (NATS, Kafka, AMQP, SQLite, KMS). The stream logic should not depend on any of them.

## Decision

The code follows a ports-and-adapters structure:

- `internal/core/domain`: events, specs and the contracts of components, depending only on the OTLP protobufs and the standard library
- `internal/ports`: the interfaces the core offers and needs
- `internal/core`: builds and runs streams
- `internal/adapters`: everything that touches the outside, i.e. the API, file sync, operator, sources, stages, sinks and their drivers

Dependencies point inwards: adapters import the core and the domain, never the other way around. Components implement small interfaces (`Observer`, `Subscriber`, `Stoppable`, …) instead of one large one.

## Consequences

- A new configuration source or component is a new adapter; the core does not change
- Tests of the core use fake sources and sinks; tests of adapters use the real core
