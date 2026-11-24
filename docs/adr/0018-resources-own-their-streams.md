# 0018: Stream Resources Own Their Streams

- **Status:** Accepted
- **Date:** 2026-11-24
- **Supersedes:** [0010](0010-one-stream-state-last-write-wins.md)

## Context

Under [0010](0010-one-stream-state-last-write-wins.md), a stream belonged to no source and the last write won. The operator applied a `Stream` resource only when it changed and when the instance started, so a change through the API or the file sync to the stream of a resource lasted until then, possibly for weeks, and differed between the instances of an Engine. A deleted file stopped the stream the same way. Kubernetes users expect a resource to describe what runs.

## Decision

- A `Stream` resource owns the stream of its name, `<namespace>.<name>`, on every instance that runs it. The operator watches the changes of the core, the same way the file sync does, and reconciles the resource of every changed or deleted stream; the reconcile applies the resource again if the stream differs from it, or starts it again if it was deleted
- The operator reconciles only streams of resources it runs: resources in its cache, i.e. in a watched namespace and, with an engine, labeled for it, whose `spec.engineRefs` name the engine. A stream under a name like `<namespace>.<name>` without such a resource is left alone, since the reconcile of a missing resource would delete it
- Changes are undone, not refused: the API and the file sync still accept them, and the operator applies the resource right after. The core keeps no owner per stream
- The operator receives the changes of the core without waiting for its controller, which starts later; the core is never held up by the operator
- Streams that no resource owns keep the rule of 0010: the last write wins

## Consequences

- What runs matches the resources, on every instance, within a moment of a change elsewhere
- A change to the stream of a resource through the API or a file has an effect only for that moment, e.g. it may restart the stream's source once
- With file sync, the file of a stream of a resource follows the resource again after an edit
- The operator's own changes reach it as well; their reconciles find the stream as the resource describes it and change nothing
- `tram drain` runs no API or file sync and does not watch the core

## Alternatives

- **Refuse changes to streams of resources in the API and the file sync:** clearer for the user, but the core would need an owner per stream and every source would need to know about the operator. Can be added later, on top of this
- **Write changes back to the resources:** gives TRAM write access to the resources and lets instances overwrite each other
