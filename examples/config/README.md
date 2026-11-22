# TRAM Example Configurations

This directory contains example JSON configurations for TRAM streams. The examples demonstrate various pipeline patterns including OTLP sinks, broker sources over NATS, and benchmark scenarios.

## Quick Start

All examples in this directory have corresponding Kubernetes YAML configurations in the `../k8s/` directory with the same base filename. For example:

- `config/otlp_basic.json` ↔ `k8s/examples/otlp_basic.yaml`
- `config/benchmark_otlp_nats.json` ↔ `k8s/examples/benchmark_otlp_nats.yaml`

## Available Examples

### Basic OTLP Examples

These examples demonstrate using the OTLP sink in TRAM for various use cases.

#### [`otlp_basic.json`](otlp_basic.json)
**Purpose**: Simple OTLP to OTLP forwarding configuration.
- Receives OTLP data on port 4318
- Forwards to another OTLP endpoint
- No processing stages
- **Kubernetes version**: [../k8s/examples/otlp_basic.yaml](../k8s/examples/otlp_basic.yaml)

#### [`otlp_secure.json`](otlp_secure.json)
**Purpose**: Secure OTLP forwarding with HTTPS and authentication headers.
- HTTPS endpoint
- Bearer token authentication
- Custom headers for metadata
- **Kubernetes version**: [../k8s/examples/otlp_secure.yaml](../k8s/examples/otlp_secure.yaml)

#### [`otlp_insecure_dev.json`](otlp_insecure_dev.json)
**Purpose**: Development configuration for HTTPS endpoints with invalid certificates.
- Uses `insecure: true` to skip TLS certificate validation
- Useful for local development and testing
- Generator source for testing
- **Kubernetes version**: [../k8s/examples/otlp_insecure_dev.yaml](../k8s/examples/otlp_insecure_dev.yaml)

### Processing Pipeline Examples

#### [`otlp_batch.json`](otlp_batch.json)
**Purpose**: High-throughput OTLP forwarding with batching.
- Batches events for better throughput
- 1000 records max per batch
- 200ms timeout for batch flush
- **Kubernetes version**: [../k8s/examples/otlp_batch.yaml](../k8s/examples/otlp_batch.yaml)

#### [`otlp_to_buffer.json`](otlp_to_buffer.json) and [`buffer_to_otlp.json`](buffer_to_otlp.json)
**Purpose**: OTLP forwarding that rides out outages of the destination, with two streams around a buffer queue.
- `otlp_to_buffer` stores every request in the queue `outbox` before answering the client
- `buffer_to_otlp` delivers the queue to the destination, retrying for up to the queue's `max_age` (24 hours)
- Both run on the same instance, with `tram server --data-dir` on a persistent volume
- **Kubernetes version**: [../k8s/examples/otlp_to_buffer.yaml](../k8s/examples/otlp_to_buffer.yaml) and [../k8s/examples/buffer_to_otlp.yaml](../k8s/examples/buffer_to_otlp.yaml), on an Engine with `storage`

#### [`otlp_with_filters.json`](../tests/otlp_with_filters.json)
**Purpose**: OTLP forwarding with CEL filtering. Lives in `../tests/` because it carries test cases.
- Filters logs based on customer attribute
- Shows how to use CEL expressions in filters
- Sends filtered data to customer-specific endpoint
- **Kubernetes version**: [../k8s/examples/otlp_with_filters.yaml](../k8s/examples/otlp_with_filters.yaml)

#### [`otlp_with_transform.json`](../tests/otlp_with_transform.json)
**Purpose**: Normalize attributes with the transform stage. Lives in `../tests/` because it carries test cases.
- Derives `deployment.environment` on the resource from the namespace, once per resource
- Deletes `password`, renames `user` to `user.name`, truncates `note` and lowercases the severity into `level`
- Its test cases show a normalized record and one the statements do not apply to, kept as it is (`on_error: skip`); run them with `tram test ../tests/otlp_with_transform.json`
- **Kubernetes version**: [../k8s/examples/otlp_with_transform.yaml](../k8s/examples/otlp_with_transform.yaml)

#### [`otlp_with_validation.json`](otlp_with_validation.json)
**Purpose**: OTLP forwarding with CEL validation.
- Validates that all resources have `service.name` attribute
- Rejects invalid data with HTTP 422
- Protects downstream systems from malformed data
- **Kubernetes version**: [../k8s/examples/otlp_with_validation.yaml](../k8s/examples/otlp_with_validation.yaml)

### Multi-Sink Examples

#### [`otlp_multi_sink.json`](otlp_multi_sink.json)
**Purpose**: Multiple OTLP sinks from a single source (fan-out architecture).
- Splits data into different streams using filters
- Sends error/warn logs to one endpoint
- Sends all data to another endpoint
- **Kubernetes version**: [../k8s/examples/otlp_multi_sink.yaml](../k8s/examples/otlp_multi_sink.yaml)

### Integration Examples

#### [`otlp_nats_bridge.json`](otlp_nats_bridge.json)
**Purpose**: Bridges NATS JetStream to OTLP HTTP.
- Consumes OTLP logs from NATS JetStream with the broker source
- Uses durable consumer with multiple workers
- Batches messages for efficiency
- Forwards to central OTLP collector
- **Kubernetes version**: [../k8s/examples/otlp_nats_bridge.yaml](../k8s/examples/otlp_nats_bridge.yaml)

#### [`otlp_benchmark.json`](otlp_benchmark.json)
**Purpose**: High-performance benchmark configuration.
- Generator source for load testing
- Large batch sizes (8192 records)
- Short batch timeout (100ms)
- **Kubernetes version**: [../k8s/examples/otlp_benchmark.yaml](../k8s/examples/otlp_benchmark.yaml)

### Benchmark Configurations

These configurations match the exact scenarios used in the TRAM benchmarks (see [BENCHMARK.md](../../BENCHMARK.md)). They demonstrate pipelines with OTLP sources, various processing stages, and broker sinks over NATS. The JetStream streams have to exist, since TRAM creates none.

#### [`benchmark_otlp_nats.json`](benchmark_otlp_nats.json)
**Purpose**: Exact configuration for the `otlp-nats` benchmark scenario: **OTLP → NATS**
- Uses the same configuration as the official benchmarks
- Matches the `otlp-nats` scenario from BENCHMARK.md
- **Kubernetes version**: [../k8s/examples/benchmark_otlp_nats.yaml](../k8s/examples/benchmark_otlp_nats.yaml)

#### [`benchmark_otlp_batch_nats.json`](benchmark_otlp_batch_nats.json)
**Purpose**: Exact configuration for the `otlp-batch-nats` benchmark scenario: **OTLP → batch → NATS**
- batch stage with `max_records: 1000`, `timeout: "10ms"` as specified in benchmarks
- **Kubernetes version**: [../k8s/examples/benchmark_otlp_batch_nats.yaml](../k8s/examples/benchmark_otlp_batch_nats.yaml)

#### [`benchmark_otlp_split_nats.json`](benchmark_otlp_split_nats.json)
**Purpose**: Exact configuration for the `otlp-split-nats` benchmark scenario: **OTLP → 2 × filter → 2 × NATS**
- Splits data into team-0 and other teams using filter stages with the CEL driver
- Matches the benchmark configuration exactly
- **Kubernetes version**: [../k8s/examples/benchmark_otlp_split_nats.yaml](../k8s/examples/benchmark_otlp_split_nats.yaml)

#### [`benchmark_nats_filter_nats.json`](benchmark_nats_filter_nats.json)
**Purpose**: Exact configuration for the `nats-filter-nats` benchmark scenario: **NATS → filter → NATS**
- Uses durable consumer with 8 workers as specified in benchmarks
- **Kubernetes version**: [../k8s/examples/benchmark_nats_filter_nats.yaml](../k8s/examples/benchmark_nats_filter_nats.yaml)

## Configuration Reference

### OTLP Sink Reference

**Required Fields:**
- `url`: OTLP HTTP endpoint URL

**Optional Fields:**
- `headers`: HTTP headers as key-value pairs
- `timeout`: Request timeout as Go duration (default: 10s)
- `insecure`: Skip TLS certificate validation (default: false)

**Endpoint Paths:**
The OTLP sink automatically uses the correct paths:
- Logs: `{url}/v1/logs`
- Metrics: `{url}/v1/metrics`
- Traces: `{url}/v1/traces`

**Error Handling:**
- HTTP errors (non-2xx) are returned to the caller
- Invalid configurations are rejected at startup
- Nil events are handled gracefully
- Empty events (no logs/metrics/traces) result in errors

### Broker Source/Sink Reference (NATS driver)

TRAM creates no streams: set them up before the stream starts, e.g. `nats stream add input-stream --subjects 'telemetry.>' --defaults`. See [COMPONENTS.md](../../COMPONENTS.md#broker-source) for all fields.

**Broker Source Fields:**
- `driver`: `nats`
- `signal`: `logs`, `metrics` or `traces`; every message of the subject holds this signal
- `workers`: Number of concurrent workers
- `nats.url`, `nats.subject` (consumer filter), `nats.stream` (optional), `nats.durable` (optional)

**Broker Sink Fields:**
- `driver`: `nats`
- `nats.url`: NATS server URL
- `destination.logs` / `.metrics` / `.traces`: CEL expressions that return the subject of each record

## Usage Patterns

### Simple OTLP Forwarding
```json
{
  "sinks": [{
    "type": "otlp",
    "name": "my_sink",
    "from": "my_source",
    "spec": {"url": "http://collector:4318"}
  }]
}
```

### OTLP with Authentication
```json
{
  "sinks": [{
    "type": "otlp", 
    "name": "auth_sink",
    "from": "my_source",
    "spec": {
      "url": "https://collector:443",
      "headers": {"Authorization": "Bearer token"}
    }
  }]
}
```

### Broker Source and Sink over NATS
```json
{
  "source": {
    "type": "broker",
    "name": "my_source",
    "spec": {
      "driver": "nats",
      "signal": "logs",
      "nats": {
        "url": "nats://nats-server:4222",
        "subject": "input.logs",
        "durable": "tram-consumer"
      }
    }
  },
  "sinks": [{
    "type": "broker",
    "name": "my_sink",
    "from": "my_source",
    "spec": {
      "driver": "nats",
      "nats": { "url": "nats://nats-server:4222" },
      "destination": { "logs": "'output.logs'" }
    }
  }]
}
```

### Development Testing with Generator
```json
{
  "sources": [{
    "type": "generator",
    "name": "test_gen",
    "spec": {
      "records_per_event": 100,
      "records": 1000
    }
  }],
  "sinks": [{
    "type": "otlp",
    "name": "test_sink",
    "from": "test_gen",
    "spec": {
      "url": "http://localhost:4318",
      "insecure": true
    }
  }]
}
```

## Kubernetes Examples

All example configurations in this directory have corresponding Kubernetes YAML configurations in the `../k8s/` directory with the same base filename. The complete list includes:

- `config/otlp_basic.json` ↔ `k8s/examples/otlp_basic.yaml`
- `config/otlp_secure.json` ↔ `k8s/examples/otlp_secure.yaml`
- `config/otlp_batch.json` ↔ `k8s/examples/otlp_batch.yaml`
- `tests/otlp_with_filters.json` ↔ `k8s/examples/otlp_with_filters.yaml`
- `tests/otlp_with_transform.json` ↔ `k8s/examples/otlp_with_transform.yaml`
- `config/otlp_with_validation.json` ↔ `k8s/examples/otlp_with_validation.yaml`
- `config/otlp_multi_sink.json` ↔ `k8s/examples/otlp_multi_sink.yaml`
- `config/otlp_nats_bridge.json` ↔ `k8s/examples/otlp_nats_bridge.yaml`
- `config/otlp_benchmark.json` ↔ `k8s/examples/otlp_benchmark.yaml`
- `config/otlp_insecure_dev.json` ↔ `k8s/examples/otlp_insecure_dev.yaml`
- `config/benchmark_otlp_nats.json` ↔ `k8s/examples/benchmark_otlp_nats.yaml`
- `config/benchmark_otlp_batch_nats.json` ↔ `k8s/examples/benchmark_otlp_batch_nats.yaml`
- `config/benchmark_otlp_split_nats.json` ↔ `k8s/examples/benchmark_otlp_split_nats.yaml`
- `config/benchmark_nats_filter_nats.json` ↔ `k8s/examples/benchmark_nats_filter_nats.yaml`

See [Kubernetes Examples](../k8s/README.md) for detailed Kubernetes-specific documentation, deployment instructions, and performance optimization tips.
