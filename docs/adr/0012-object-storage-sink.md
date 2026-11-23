# 0012: Object Storage Sink With S3 as Its First Driver

- **Status:** Accepted, not built
- **Date:** 2026-11-22

## Context

Data should be archived in object storage, e.g. for long retention or analysis. Most providers speak the S3 API (AWS, STACKIT, MinIO, Ceph, R2); the format of the objects is a choice of its own (OTLP protobuf or JSON, Parquet, plain log bodies).

## Decision

- A sink `type: object_storage`, with the storage API as its driver ([0004](0004-components-as-concept-and-driver.md)): `s3` first, then `file` for local directories and tests; others only on demand. The driver interface stays small: put an object with metadata, its size limit, checking a key, closing
- The format is a separate field, `format`, with its options under its own name (`otlp_proto`, `otlp_json`, `parquet`, `body`). Encoders live in their own package, so other sinks can use them
- The sink, not the driver, collects events into objects by size and age, builds keys from a template, encodes and compresses, and acknowledges after the upload (at-least-once, [0006](0006-at-least-once-delivery.md))
- Order: `s3` with `otlp_proto`, then Parquet for logs and traces, then metrics

## Open

- Whether the sink collects into objects itself or relies on a batch stage before it; the sink collecting itself is preferred

## Alternatives

- **Format as the driver:** turned down, since `driver` means the technology talked to everywhere else, and format and storage vary independently
