# 0002: OTLP as the Data Model of the Core

- **Status:** Accepted

## Context

TRAM moves telemetry between OpenTelemetry clients, brokers and backends. A pipeline can work on its own internal format and convert at the edges, like the OpenTelemetry Collector's pdata, or on the wire format itself.

## Decision

The core works on the OTLP protobuf types directly: an event holds `LogsData`, `MetricsData` or `TracesData`, and the domain package also encodes and decodes them (`MarshalOTLPPayloads`, `UnmarshalOTLPData`). There is no internal data model of TRAM's own. The OTLP source has no driver, since OTLP is the technology it serves; HTTP and gRPC are its ports.

## Consequences

- No conversion between sources, stages and sinks: what a client sent is what a sink sends on, and a broker between two streams carries plain OTLP
- CEL expressions see the OTLP structure, with the field names users know from the specification
- Another data format, e.g. a non-OTLP log format, cannot be plugged in as an adapter; it would have to be converted to OTLP at a source. That is accepted: TRAM is an OTLP pipeline
- New OTLP fields arrive with the protobuf module; copies of events have to keep them, which `TestCopiesKeepEveryField` checks

## Alternatives

- **An internal model (like pdata):** decouples the core from the wire format, at the cost of a conversion on every hop and a second model to learn. Not needed while TRAM only speaks OTLP
