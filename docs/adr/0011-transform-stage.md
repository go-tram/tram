# 0011: Transform Stage With a CEL Driver, Attributes Only

- **Status:** Accepted
- **Date:** 2026-11-22

## Context

Users need to change data in a stream: set, delete or rename attributes, truncate values. `value_map` already maps values through tables; the encryption stage covers redaction.

## Decision

- A `transform` stage, as concept and driver ([0004](0004-components-as-concept-and-driver.md)): `driver: cel` first, with its statements under `cel`. An `ottl` driver may follow, with OTTL's own statements; only stage-level fields are shared between drivers
- Version 1 changes attributes of records, resources and scopes: set, delete, rename, truncate, each with an optional `when`. Not the log body or severity, no hashing, no parsing of bodies. The target paths form a grammar, so adding `body` or `severity_*` later is not a breaking change
- `on_error` decides what a failing statement does: `skip` (the default: skip the statement, keep the entry, count it), `reject` (`422`) or `drop`
- Boundary to `value_map`: transform is logic over an entry, value_map is data from tables. Transform has no lookup function, value_map no expressions; a stream chains both
- Everything compiles when the stream is created, so a dry run reports errors

## Consequences

- Copies follow [0005](0005-events-are-read-only.md): only changed parts are copied
- Body and severity changes, OTTL and hashing are later additions, not part of v1
