# 0005: Events Are Shared and Read-Only

- **Status:** Accepted

## Context

A stream is a tree: after a stage, every branch receives the event, concurrently. Copying the event for every branch costs memory and time in proportion to its size; sharing it is free, but a branch that changes it would change it for the others while they read it.

## Decision

An event is passed by reference to all branches and is read-only for every subscriber. A stage that changes data forwards a copy and leaves the event it received as it is. Copies are shallow where possible: a stage copies only the parts it changes and shares the rest (copy on write), e.g. the transform stage copies a resource only when a statement changes it.

## Consequences

- Fan-out costs nothing per branch. A stage that writes to a shared event is a data race, which the tests, run with `-race`, catch
- A partial copy has to keep every field of the OTLP types it copies; `TestCopiesKeepEveryField` fails when a new protobuf field is not copied
