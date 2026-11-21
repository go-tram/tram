# Tram Example Configurations for Kubernetes

This directory contains Kubernetes YAML configurations for deploying Tram streams in Kubernetes environments. Each example has a corresponding JSON configuration in the `../config/` directory with the same base filename.

Every example runs on the Engine `demo` from [`examples/engine.yaml`](examples/engine.yaml): its `spec.engineRefs` names it. The OTLP examples listen on ports of their own, 4318 to 4327, which the Engine declares, so all examples can run on it at once.

## Quick Start

### Prerequisites
- Kubernetes cluster
- NATS server or OpenTelemetry Collector deployment (depending on the example)

### Install the Manager and the Engine

From the repository root, with the Helm chart [`charts/tram`](../../charts/tram/), which installs the CRDs, the roles and the manager:

```bash
helm install tram charts/tram --namespace tram-system --create-namespace

# The Engine that runs the examples
kubectl apply -f examples/k8s/examples/engine.yaml
kubectl wait engine/demo --for=condition=Ready
```

Or without Helm, with the kustomization in [`manifests/`](../../manifests/): the CRDs in `manifests/crds/`, and the manager with its roles in `manifests/tram-manager/`, in the namespace `tram-system`. Apply it server-side, since the CRDs are too large for a client-side apply. From the repository it deploys the image `latest`; every [GitHub Release](https://github.com/GO-TRAM/TRAM/releases) has the same manifests with the release's image as `tram.yaml`:

```bash
kubectl apply --server-side -k manifests/
# or a release
kubectl apply --server-side -f https://github.com/GO-TRAM/TRAM/releases/download/v0.0.20/tram.yaml

# The Engine that runs the examples
kubectl apply -f examples/k8s/examples/engine.yaml
kubectl wait engine/demo --for=condition=Ready
```

### Deploy a Stream

```bash
# Deploy a basic OTLP forwarding stream
kubectl apply -f examples/k8s/examples/otlp_basic.yaml

# Check the stream: labeled engine.tram.io/demo and active
kubectl get streams --show-labels
kubectl get engines

# Or deploy every example at once, with the Engine
kubectl apply -f examples/k8s/examples/
```

## Layout

- [`examples/engine.yaml`](examples/engine.yaml): the Engine `demo`, which runs every example
- [`examples/`](examples/): the Stream examples below

## Available Examples

### Basic OTLP Examples

These examples demonstrate using the OTLP sink in Tram for various use cases.

#### [`otlp_basic.yaml`](examples/otlp_basic.yaml)
**Purpose**: Basic OTLP to OTLP forwarding
- Receives OTLP data on port 4318
- Forwards to a central OTLP collector
- No processing stages
- **JSON version**: [../config/otlp_basic.json](../config/otlp_basic.json)

#### [`otlp_debug.yaml`](examples/otlp_debug.yaml)
**Purpose**: OTLP into the debug sink, to see what reaches an Engine
- Receives OTLP data on port 4327
- Logs every record it receives; see them with `stern -l tram.io/engine=demo`
- No JSON version

#### [`otlp_secure.yaml`](examples/otlp_secure.yaml)
**Purpose**: Secure OTLP forwarding with authentication
- Receives OTLP data on port 4321
- HTTPS endpoint
- Bearer token authentication
- Custom headers for metadata
- **JSON version**: [../config/otlp_secure.json](../config/otlp_secure.json)

#### [`otlp_insecure_dev.yaml`](examples/otlp_insecure_dev.yaml)
**Purpose**: Development configuration with insecure TLS
- HTTPS endpoint with invalid certificate acceptance
- Useful for local development and testing
- Generator source for testing
- **JSON version**: [../config/otlp_insecure_dev.json](../config/otlp_insecure_dev.json)

### Processing Pipeline Examples

#### [`otlp_batch.yaml`](examples/otlp_batch.yaml)
**Purpose**: High-throughput OTLP forwarding with batching
- Receives OTLP data on port 4319
- Batches events for better throughput
- 1000 records max per batch
- 200ms timeout for batch flush
- **JSON version**: [../config/otlp_batch.json](../config/otlp_batch.json)

#### [`otlp_with_filters.yaml`](examples/otlp_with_filters.yaml)
**Purpose**: OTLP forwarding with CEL filtering
- Receives OTLP data on port 4322
- Filters logs based on customer attribute
- Sends filtered data to customer-specific endpoint
- **JSON version**: [../config/otlp_with_filters.json](../config/otlp_with_filters.json)

#### [`otlp_with_validation.yaml`](examples/otlp_with_validation.yaml)
**Purpose**: Validated OTLP forwarding with CEL validation
- Receives OTLP data on port 4323
- Validates that all resources have `service.name` attribute
- Rejects invalid data
- **JSON version**: [../config/otlp_with_validation.json](../config/otlp_with_validation.json)

### Multi-Sink Examples

#### [`otlp_multi_sink.yaml`](examples/otlp_multi_sink.yaml)
**Purpose**: Multiple OTLP sinks from a single source (fan-out)
- Receives OTLP data on port 4320
- Splits data into different streams using filters
- Sends error/warn logs to one endpoint
- Sends all data to another endpoint
- **JSON version**: [../config/otlp_multi_sink.json](../config/otlp_multi_sink.json)

### Integration Examples

#### [`otlp_nats_bridge.yaml`](examples/otlp_nats_bridge.yaml)
**Purpose**: Bridges NATS JetStream to OTLP HTTP
- Consumes OTLP logs from NATS JetStream with the broker source
- Uses durable consumer with multiple workers
- Batches messages for efficiency
- Forwards to central OTLP collector
- **JSON version**: [../config/otlp_nats_bridge.json](../config/otlp_nats_bridge.json)

#### [`otlp_benchmark.yaml`](examples/otlp_benchmark.yaml)
**Purpose**: High-performance benchmark configuration
- Generator source for load testing
- Large batch sizes (8192 records)
- Short batch timeout (100ms)
- **JSON version**: [../config/otlp_benchmark.json](../config/otlp_benchmark.json)

### Benchmark Configurations

These configurations match the exact scenarios used in the Tram benchmarks (see [BENCHMARK.md](../../BENCHMARK.md)). They demonstrate pipelines with OTLP sources, various processing stages, and broker sinks over NATS. The JetStream streams have to exist, since Tram creates none.

#### [`benchmark_otlp_nats.yaml`](examples/benchmark_otlp_nats.yaml)
**Purpose**: Exact configuration for the `otlp-nats` benchmark scenario: OTLP → NATS
- Receives OTLP data on port 4324
- Matches the official benchmark configuration
- **JSON version**: [../config/benchmark_otlp_nats.json](../config/benchmark_otlp_nats.json)

#### [`benchmark_otlp_batch_nats.yaml`](examples/benchmark_otlp_batch_nats.yaml)
**Purpose**: Exact configuration for the `otlp-batch-nats` benchmark scenario: OTLP → batch → NATS
- Receives OTLP data on port 4325
- Uses batch with `max_records: 1000`, `timeout: "10ms"` as in benchmarks
- **JSON version**: [../config/benchmark_otlp_batch_nats.json](../config/benchmark_otlp_batch_nats.json)

#### [`benchmark_otlp_split_nats.yaml`](examples/benchmark_otlp_split_nats.yaml)
**Purpose**: Exact configuration for the `otlp-split-nats` benchmark scenario: OTLP → 2 × filter → 2 × NATS
- Receives OTLP data on port 4326
- Splits into team-0 and other teams as specified in benchmarks
- **JSON version**: [../config/benchmark_otlp_split_nats.json](../config/benchmark_otlp_split_nats.json)

#### [`benchmark_nats_filter_nats.yaml`](examples/benchmark_nats_filter_nats.yaml)
**Purpose**: Exact configuration for the `nats-filter-nats` benchmark scenario: NATS → filter → NATS
- Uses durable consumer with workers as in benchmarks
- **JSON version**: [../config/benchmark_nats_filter_nats.json](../config/benchmark_nats_filter_nats.json)

## Configuration Reference

### Service Discovery

For production environments, consider using Kubernetes service discovery:

```yaml
spec:
  sinks:
    - type: otlp
      name: my_sink
      from: my_source
      spec:
        url: "http://otel-collector-service.namespace.svc.cluster.local:4318"
```

### NATS Configuration

For NATS-based examples, configure the NATS driver of the broker source. The JetStream stream that holds the subject has to exist; Tram creates none:

```yaml
source:
  type: broker
  name: my_source
  spec:
    driver: nats
    signal: logs
    workers: 8
    nats:
      url: "nats://nats-server:4222"
      subject: "my-stream.logs"
      durable: "my-durable-consumer"
```

### Secrets

Keep sensitive values in a `Secret` in the namespace of the `Stream` and refer to it; the value replaces the whole field, see [Secrets](../../internal/adapters/k8scrd/README.md#secrets):

```yaml
spec:
  sinks:
    - type: otlp
      name: secure_sink
      from: my_source
      spec:
        url: "https://otel-collector:443"
        headers:
          Authorization:
            valueFrom:
              secretKeyRef:
                name: otel-secrets
                key: authorization
```

Deploy with:
```bash
kubectl create secret generic otel-secrets --from-literal=authorization="Bearer your-api-key-here"
kubectl apply -f your-stream.yaml
```

### Resource Management

Streams run on the instances of an Engine, so resources and scaling are set on the Engine, for all of its Streams.

#### CPU/Memory Requests

```yaml
apiVersion: io.tram.core/v1alpha1
kind: Engine
metadata:
  name: high-volume
spec:
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      memory: 1Gi
```

#### Horizontal Pod Autoscaling

The manager creates the HorizontalPodAutoscaler of an Engine:

```yaml
apiVersion: io.tram.core/v1alpha1
kind: Engine
metadata:
  name: high-volume
spec:
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
```

## Integration Patterns

### OpenTelemetry Collector Integration

The most common use case is forwarding to an OpenTelemetry Collector:

```yaml
spec:
  sinks:
    - type: otlp
      name: otel_collector_sink
      from: source_or_stage
      spec:
        url: "http://otel-collector:4318"
```

### NATS JetStream Integration

For NATS-based pipelines, use the broker source and sink with the NATS driver. Set up the streams first, e.g. `nats stream add input --subjects 'input.>' --defaults` and `nats stream add output --subjects 'output.>' --defaults`:

```yaml
source:
  type: broker
  name: nats_source
  spec:
    driver: nats
    signal: logs
    workers: 4
    nats:
      url: "nats://nats-server:4222"
      subject: "input.logs"
      durable: "tram-consumer"

sinks:
  - type: broker
    name: nats_sink
    from: nats_source
    spec:
      driver: nats
      nats:
        url: "nats://nats-server:4222"
      destination:
        logs: "'output.logs'"
```

### Multi-Region Deployment

Deploy different streams for different regions:

```yaml
# region-a.yaml
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: my-stream-region-a
  labels:
    region: a
# ...

# region-b.yaml  
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: my-stream-region-b
  labels:
    region: b
# ...
```

### Canary Deployment

Deploy a canary stream for testing new configurations:

```bash
# Main production stream
kubectl apply -f otlp_secure.yaml

# Canary stream with new configuration
kubectl apply -f otlp_benchmark.yaml
```

## Monitoring and Observability

### Health Checks

Check stream health:
```bash
kubectl get stream my-stream -o yaml
kubectl describe stream my-stream
```

### Logs

View the logs of the instances of an Engine, which run its streams:
```bash
kubectl logs -l tram.io/engine=demo --prefix
```

### Metrics

Every instance serves Prometheus metrics on `/metrics` of its HTTP API, which the Engine's Service exposes as the port `http`:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: tram-examples
spec:
  selector:
    matchLabels:
      tram.io/engine: demo
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
```

## Performance Optimization

### Connection Pooling

The OTLP sink uses HTTP connection pooling by default:
- Max idle connections: 100
- Idle connection timeout: 90 seconds
- Max idle connections per host: 100

### Batch Size Tuning

Adjust batch sizes based on your workload:

```yaml
stages:
  - type: batch
    name: batch
    from: source
    spec:
      max_records: 5000    # Increase for high throughput
      timeout: "500ms"    # Increase to allow larger batches
```

### Timeout Configuration

Set appropriate timeouts:

```yaml
sinks:
  - type: otlp
    name: my_sink
    from: batch
    spec:
      url: "http://collector:4318"
      timeout: "60s"  # Longer timeout for large batches
```

## Troubleshooting

### Connection Issues

Check connectivity to your endpoints. The Tram image has no shell, so use a temporary pod in the namespace of the Engine:
```bash
kubectl run curl --rm -it --restart=Never --image=curlimages/curl -- curl -v http://otel-collector:4318/v1/logs
```

### Certificate Issues

For HTTPS endpoints with self-signed certificates:

```yaml
sinks:
  - type: otlp
    name: insecure_sink
    from: source
    spec:
      url: "https://collector:443"
      insecure: true  # Skip certificate validation
```

Or properly configure certificates in your cluster.

### Authentication Issues

Verify that the secrets a Stream refers to exist; a missing secret or key keeps the stream from starting and sets `status.active` to `false`:
```bash
kubectl get secret otel-secrets -o jsonpath='{.data}'
kubectl get stream my-stream -o jsonpath='{.status}'
```

## Security Best Practices

1. **Use HTTPS**: Always use HTTPS in production for external endpoints
2. **TLS Certificates**: Use proper TLS certificates, not `insecure: true`
3. **Secrets Management**: Use Kubernetes Secrets for sensitive data
4. **Network Policies**: Restrict access to your Tram streams
5. **RBAC**: Use appropriate RBAC roles for the Tram operator

Example NetworkPolicy:
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: tram-allow-endpoints
spec:
  podSelector:
    matchLabels:
      tram.io/engine: demo
  policyTypes:
    - Egress
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: otel-collector
      ports:
        - protocol: TCP
          port: 4318
    - to:
        - podSelector:
            matchLabels:
              app: nats
      ports:
        - protocol: TCP
          port: 4222
```

## JSON Configuration Equivalents

All Kubernetes YAML examples in `examples/` have corresponding JSON configurations in the `../config/` directory with the same base filename. The JSON versions have no `engineRefs`, and their OTLP sources all listen on port 4318:

- `k8s/examples/otlp_basic.yaml` ↔ `config/otlp_basic.json`
- `k8s/examples/otlp_secure.yaml` ↔ `config/otlp_secure.json`
- `k8s/examples/otlp_batch.yaml` ↔ `config/otlp_batch.json`
- `k8s/examples/otlp_with_filters.yaml` ↔ `config/otlp_with_filters.json`
- `k8s/examples/otlp_with_validation.yaml` ↔ `config/otlp_with_validation.json`
- `k8s/examples/otlp_multi_sink.yaml` ↔ `config/otlp_multi_sink.json`
- `k8s/examples/otlp_nats_bridge.yaml` ↔ `config/otlp_nats_bridge.json`
- `k8s/examples/otlp_benchmark.yaml` ↔ `config/otlp_benchmark.json`
- `k8s/examples/otlp_insecure_dev.yaml` ↔ `config/otlp_insecure_dev.json`
- `k8s/examples/benchmark_otlp_nats.yaml` ↔ `config/benchmark_otlp_nats.json`
- `k8s/examples/benchmark_otlp_batch_nats.yaml` ↔ `config/benchmark_otlp_batch_nats.json`
- `k8s/examples/benchmark_otlp_split_nats.yaml` ↔ `config/benchmark_otlp_split_nats.json`
- `k8s/examples/benchmark_nats_filter_nats.yaml` ↔ `config/benchmark_nats_filter_nats.json`

See [JSON Examples](../config/README.md) for detailed JSON configuration documentation.