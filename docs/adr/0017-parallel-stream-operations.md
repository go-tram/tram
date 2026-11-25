# 0017: Operations on Different Streams Run in Parallel

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

The core guarded all streams with one lock, held while a stream was built or stopped and while watchers were notified. Building a stream can take long, e.g. while its sinks connect to a broker, and stopping one waits for the deliveries in progress. One slow stream held up every other change, reads through the API, the operator and the file sync. An instance loaded its streams one after the other, both from files and from `Stream` resources, and stopped them one after the other on shutdown.

## Decision

- **A lock per stream name.** Creating, updating, deleting, restarting and planning a stream hold the lock of its name for the whole operation and read the running stream only once they hold it. Operations on one name run one after the other; those on different names run in parallel. The lock belongs to the name, not to the running stream: a stream is replaced on every update, and a new name has no stream yet to lock
- **A short lock for the core's state.** It guards the map of streams, the watchers and whether the core is closed, and is never held while a stream is built or stopped, nor while watchers are notified
- **A new stream is listed once its watchers know it.** `GetStream` and `GetStreams` show a new stream only after every watcher received its event. The file sync relies on it: it deletes listed streams without a file, and writes the file when it receives the event
- **Close** refuses new operations, waits for those in progress, stops all streams in parallel, and gives up waiting when its context ends; `tram server` allows 25 seconds
- **Loading in parallel.** The file sync applies and deletes up to 8 streams at once, and the operator reconciles up to 8 `Stream` resources at once

## Consequences

- An instance with many streams starts and stops faster, as much as the streams take long to connect, and a new replica runs its streams sooner
- A slow or failing stream no longer holds up the others or the API
- Changes to one stream from several sources still apply one after the other, and the last one wins ([0010](0010-one-stream-state-last-write-wins.md))
- A watcher that is slow to receive events holds up the operations on that stream, not the core
- Connections to brokers are opened by up to 8 streams at once, not all of them: the bound keeps a start from flooding a broker or the Kubernetes API

## Alternatives

- **A lock per running stream:** turned down, since an update replaces the stream: an operation waiting for the lock of the old one would continue with a stream that is gone, and two creations of a new name would find nothing to lock
- **No bound on parallel loading:** turned down, since hundreds of streams would connect to their brokers, and read their secrets from the Kubernetes API, at the same moment
