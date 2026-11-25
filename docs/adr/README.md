# Architecture Decisions

This directory records the decisions that shape TRAM, as Architecture Decision Records (ADRs): what was decided, why, and what follows from it. The documentation describes how TRAM behaves; the records explain why it behaves that way and which alternatives were turned down, so a later change can tell a deliberate trade-off from an oversight.

| ADR | Decision | Status |
|-----|----------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-otlp-as-the-data-model.md) | OTLP as the data model of the core | Accepted |
| [0003](0003-ports-and-adapters.md) | Ports and adapters | Accepted |
| [0004](0004-components-as-concept-and-driver.md) | Components as concept and driver | Accepted |
| [0005](0005-events-are-read-only.md) | Events are shared and read-only | Accepted |
| [0006](0006-at-least-once-delivery.md) | At-least-once delivery without deduplication | Accepted |
| [0007](0007-synchronous-delivery-and-backpressure.md) | Synchronous delivery and backpressure | Accepted |
| [0008](0008-requests-accepted-or-refused-as-a-whole.md) | Requests are accepted or refused as a whole | Accepted |
| [0009](0009-no-authentication-in-tram.md) | No authentication or authorization in TRAM | Accepted |
| [0010](0010-one-stream-state-last-write-wins.md) | One set of streams, the last write wins | Superseded by 0018 |
| [0011](0011-transform-stage.md) | Transform stage with a CEL driver, attributes only | Accepted |
| [0012](0012-object-storage-sink.md) | Object storage sink with S3 as its first driver | Accepted, not built |
| [0013](0013-value-map-driver.md) | A driver for value_map only when needed | Proposed |
| [0014](0014-releases-from-conventional-commits.md) | Releases from Conventional Commits | Accepted |
| [0015](0015-persistence-between-streams.md) | Streams keep nothing on disk; buffers connect two streams | Accepted |
| [0016](0016-request-size-limit-at-the-source.md) | Limit the request size at the source, decompressed | Accepted |
| [0017](0017-parallel-stream-operations.md) | Operations on different streams run in parallel | Accepted |
| [0018](0018-resources-own-their-streams.md) | Stream resources own their streams | Accepted |
| [0019](0019-tap-running-streams.md) | Tap running streams in the core, outside their spec | Accepted |

## Writing a Record

Add a decision that is hard to reverse, changes a contract (configuration, API, delivery semantics, metrics) or turns down an obvious alternative. Smaller choices belong in the pull request.

- Copy the structure of an existing record into `NNNN-short-title.md`, with the next free number, and add it to the table above and to the navigation in `docs/zensical.toml`
- **Status:** `Proposed` while it is discussed, `Accepted` once decided (`Accepted, not built` until it is implemented), `Superseded by NNNN` when a later record replaces it. Records are not deleted or rewritten; a changed decision gets a new record
- **Date:** when the decision was made. Records 0002 to 0015 were written down on 2026-11-23 for decisions made before; their dates are those of the decision where it is known
- Keep them short: context, decision, consequences, and the alternatives that were turned down
