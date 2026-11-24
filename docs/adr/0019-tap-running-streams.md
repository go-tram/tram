# 0019: Tap Running Streams in the Core, Outside Their Spec

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

Debugging a stream means seeing its events: what the source receives, what a stage makes of it, what a sink gets and why it fails. A sink already attaches to any component with `from`, and the debug sink logs events, so a tap could be a sink the CLI adds to the stream's spec and removes again. That has four problems:

- **It changes the stream.** Adding a sink replaces the stages and sinks below the source, twice: when the tap starts and when it ends. No event is lost, the old components are drained first, but their state, e.g. a rate limit, starts over
- **It conflicts with the configuration sources.** The change is a write like any other ([0010](0010-one-stream-state-last-write-wins.md)): the file sync writes it to its file, and the next reconcile of a `Stream` resource undoes it
- **The connection goes the wrong way.** A sink sends to the CLI, which a server in a cluster cannot reach; `kubectl port-forward` only connects to the server
- **It can hold up the stream.** Delivery is synchronous ([0007](0007-synchronous-delivery-and-backpressure.md)): a sink that waits or fails for a slow or vanished CLI slows down or fails the stream, and a CLI that crashes leaves its sink in the spec

## Decision

A tap is not part of the spec. The core keeps the taps of each stream by name, and the fanout of every component, which the core builds and which delivers to the component's children, hands the events to them: the output of the source and of every stage, and for every sink among its children what the sink received, with its error and duration. With no tap anywhere, a fanout checks one atomic counter and goes on.

- **Never wait**: a tap holds up to 64 events. When it is full or beyond its rate, the event is dropped and counted, and the count goes out with the next event
- **No copy**: events are read-only ([0005](0005-events-are-read-only.md)), so a tap shares the event and reads it later
- **Lifetime by name**: since taps belong to the stream's name, not to its components, they keep running when the stream is updated, rebuilt or restarted. A tap ends when its context ends, the stream is deleted, its component is removed, or the core closes
- **API**: `TapService.TapStream` in `tram.v1` is a server-streaming RPC. It is served on an endpoint of its own, the admin API (`--admin-endpoint`, `localhost:50052` by default), not with the stream API: reading the data of the streams is a different right from configuring them, and a port of its own is opened or closed on the network level, without a flag or a restart. On `localhost`, only `kubectl port-forward` reaches it on Kubernetes, which takes the `pods/portforward` permission, so Kubernetes controls who taps. gRPC and the HTTP gateway share the port: requests with a gRPC content type over HTTP/2 go to gRPC, the rest to the gateway, which serves the tap as `GET /v1/streams/{name}/tap`, one JSON object per line. The admin API also serves the `StreamService`, so it is the one API of the CLI: whoever may read the data may configure the streams too, not the other way round. Later services that look into the running TRAM, e.g. for `tram top`, go there too
- **CLI**: `tram tap` calls the RPC over gRPC at `--admin-endpoint`, which streams natively and cancels the tap when the CLI stops; the connection goes from the CLI to the server, so a port forward is enough. CLI commands that configure streams will use the same endpoint

## Consequences

- A tap shows what passed, not everything: a busy stream drops most events for a slow tap, so a tap is for looking, not for collecting data; a sink is for that
- TRAM still has no authentication ([0009](0009-no-authentication-in-tram.md)): whoever reaches the admin endpoint reads the data of every stream. Its default on `localhost` keeps it closed until someone opens it; `--admin-endpoint=""` turns it off
- Shutting down ends open taps right away instead of waiting for them
- A tap sees one instance: with several replicas, each pod runs every stream with its share of the data, and a tap shows that pod's share. `tram tap --engine` taps all pods of an Engine at once: the CLI forwards a port to each pod, merges their taps and follows scaling and rollouts
- A stage's own state is invisible to a tap: it shows what the stage passed on, e.g. a batch after its flush, not what it holds

## Alternatives

- **A sink in the spec**: turned down for the reasons above
- **Subscribe and unsubscribe on every observer**: every source and stage would need to support removing a subscriber, and a tap on a sink could not see the sink's result. The fanout is the core's own and sees both
- **Taps through the metrics wrapper of each component**: the wrapper exists only when the stream has metrics for the component
- **The TapService on the stream API, with a flag to turn it off**: whoever may configure streams could read their data, and changing that needs a restart
