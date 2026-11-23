# 0016: Limit the Request Size at the Source, Decompressed

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

Events are never split ([0008](0008-requests-accepted-or-refused-as-a-whole.md)). A broker sink rejects an event with a message larger than the broker accepts, e.g. NATS' `max_payload`, but only after it has sent the event's other messages; a buffer sink rejects an event above its `max_bytes`. A large request thus could be delivered in part and then refused.

## Decision

The OTLP source limits the size of a request with `max_request_bytes` (20 MiB by default, which was the fixed limit before) and refuses larger requests with `413` (`ResourceExhausted` over gRPC) before decoding them. The limit applies to the body as sent and after decompression; what counts is the decompressed size, since that is what the stream and the limits after it see, and it guards against compression bombs. The size is not limited by a separate stage, since a stage only sees the decoded event, after the cost was paid.

## Consequences

- Set `max_request_bytes` and a batch stage's `max_bytes` below the smallest limit downstream, so no message is too large there
- A proxy in front of TRAM sees only the compressed size; its limit protects the proxy, and TRAM's the pipeline
