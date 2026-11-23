# 0008: Requests Are Accepted or Refused as a Whole

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

OTLP clients retry only some responses (HTTP `429`, `502`, `503`, `504`; gRPC `Unavailable` and a few others) and drop data on the rest. OTLP also allows partial success. A wrong choice either loses data that would have succeeded on a retry, or makes clients retry forever what will never succeed.

## Decision

- A request becomes one event, which is never split, and it is accepted or refused as a whole; partial success is not used
- A response is retriable only if the client did nothing wrong: `503` (`Unavailable` with `RetryInfo`) when delivery failed or took longer than `delivery_timeout`. Data that cannot be read or that a stage or sink rejects gets `422` (`InvalidArgument`), an unsupported content type `415`, a body above the size limit `413` (`ResourceExhausted`)
- A rejection is `422` wherever it comes from, including a sink whose destination refuses the data permanently because of its own configuration. That is documented as a caveat rather than turned into a retriable status

## Consequences

- Clients never retry data that cannot succeed, and retry everything else
- One bad record refuses its whole request, also in a batch: validate before a batch stage so one sender's bad request does not fail a batch shared with others
