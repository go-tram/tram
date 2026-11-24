# 0010: One Set of Streams, the Last Write Wins

- **Status:** Superseded by [0018](0018-resources-own-their-streams.md)

## Context

Streams can come from the API, from files (file sync) and from `Stream` resources (operator), also at the same time. Each source could own its streams, with TRAM refusing changes from others, or all could write to one shared state.

## Decision

The core holds one set of streams, and every source writes to it through the same operations. A stream has no owner: the last write decides what runs. The file sync writes changes of the other sources back to files, so the files mirror what runs. The operator applies a resource when it changes and when the instance starts, and does not write back. The README's Conflicts Between Configuration Sources describes what follows for each combination.

## Consequences

- The sources stay simple and independent, and the API can always read what runs
- A change from another source to a stream of a `Stream` resource lasts until the resource changes or the instance restarts. Operators are told to give every stream one owning source
- Ownership could be enforced later, e.g. by refusing API changes to streams of resources, without changing the data model

## Alternatives

- **Ownership per stream:** clear, but needs an owner field in every stream, rules for taking a stream over, and errors for users who only wanted to try a change through the API
- **The operator writes back to resources:** makes Kubernetes the single source, but gives TRAM write access to resources and creates update loops between instances
