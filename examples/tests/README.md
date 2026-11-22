# Examples with Test Cases

Every stream file in this directory carries test cases in the `tests` field of its stream. They are never run by loading the stream; run them explicitly:

```bash
tram test examples/tests/otlp_with_filters.json
```

See [Testing a Stream](../../README.md#testing-a-stream) for the format of test cases, and for running them in CI. The corresponding Kubernetes manifests of the examples live in [../k8s/examples/](../k8s/README.md).
