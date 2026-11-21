# Contributing to TRAM

Thank you for your interest in contributing to TRAM! This document provides guidelines for contributing to the project.

---

## 📋 Table of Contents

- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Project Structure](#project-structure)
- [Adding New Components](#adding-new-components)
- [Pull Request Guidelines](#pull-request-guidelines)
- [Code Style Guidelines](#code-style-guidelines)
- [Testing](#testing)
- [Documentation](#documentation)
- [Continuous Integration and Releases](#continuous-integration-and-releases)

---

## 🚀 Getting Started

### Prerequisites

- Go 1.26+
- [Task](https://taskfile.dev), recommended, to run the common commands (see [Tasks](#tasks))
- Docker or Podman, optional, for the container image, a local NATS server and the tests and benchmarks that use Testcontainers

Only needed to regenerate code (`task generate`), installed with `task tools`:

- [buf](https://buf.build/) and the protoc plugins used in `buf.gen.yaml`
- [controller-gen](https://book.kubebuilder.io/reference/controller-gen) for the Kubernetes CRD and deepcopy code

Only needed to check the Helm chart (`task chart:lint`): [Helm](https://helm.sh/docs/intro/install/), and [kubeconform](https://github.com/yannh/kubeconform), installed with `task tools`.

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/GO-TRAM/TRAM.git tram
cd tram

# Install Go dependencies
go mod download

# Code generators, golangci-lint and benchstat, in the versions the
# generated code was made with (see the versions in Taskfile.yml)
task tools

# Optional: NATS with JetStream for the broker source and sink
task nats:up
```

---

## 💻 Development Environment

### Tasks

`Taskfile.yml` holds the common commands. `task` lists them, `task --summary <name>` explains one, and arguments after `--` are passed on, e.g. `task test:unit -- -run TestBatch -v`.

| Task | What it does |
|------|--------------|
| `task build` / `task run` | Build the binary, or run the server (`task run -- --enable-file-sync`) |
| `task check` | Lint, unit and end-to-end tests: what a change should pass |
| `task ci` / `task ci:lint` / `task ci:test` | What the CI workflows run: the pinned golangci-lint (`ci:lint`), then unit and end-to-end tests (`ci:test`) |
| `task test:all` | `task check` plus the container and operator (envtest) tests; fails instead of skipping if Podman/Docker or setup-envtest is missing |
| `task test` | All tests; tests that need containers are skipped without Docker or Podman |
| `task test:unit` / `task test:e2e` | Unit tests only, or the end-to-end tests that need no containers |
| `task test:containers` | End-to-end tests against NATS, Kafka and Artemis (AMQP) in containers; fails if no container runtime runs |
| `task test:envtest` | Kubernetes operator tests against a real API server and etcd; downloads them on first use |
| `task test:cover` | All tests with coverage, written to `coverage.out` |
| `task lint` / `task fmt` | golangci-lint (including go vet), or format with its configured formatters |
| `task generate` | Regenerate protobuf, gateway and swagger code and the Kubernetes CRD |
| `task bench` | Benchmarks, e.g. `task bench BENCH=Batch PKG=./internal/adapters/stage/batch/` |
| `task bench:compare` | Compare two benchmark outputs with benchstat |
| `task bench:report` | Run all benchmarks and regenerate [BENCHMARK.md](BENCHMARK.md) |
| `task image:build` / `task image:run` | Build the container image `tram:dev`, or run it with the APIs on localhost |
| `task nats:up` / `task nats:down` | Start or stop a local NATS server with JetStream |
| `task k8s:crd` | Install the Stream CRD into the current Kubernetes context |
| `task tools` | Install the code generators, golangci-lint, benchstat and setup-envtest |
| `task release:next` / `release:patch` / `release:minor` / `release:major` | Tag the next version on main and push it, which publishes the image (see [Releases](#releases)) |

Container tasks use Podman if it is installed and Docker otherwise; set `CONTAINER_TOOL=docker` to choose. With Podman, the test and benchmark tasks also set up Testcontainers for the Podman machine (see [Benchmarks](#benchmarks)).

The sections below show the underlying commands.

### Build the Project

```bash
# Build the main application
go build -o tram ./cmd/tram

# Build all packages
go build ./...
```

### Run the Service

```bash
# Start the service
./tram server

# Or with file-based config sync
./tram server --enable-file-sync -c config
```

### Generate Code

```bash
# Generate all code: protobuf and swagger (buf), Kubernetes CRD and deepcopy (controller-gen)
go generate ./...
```

### Update API Definitions

When updating the API:

1. Modify the `.proto` files in `api/proto/stream/v1/`
2. Regenerate code:
   ```bash
   go generate ./...
   ```
3. The swagger file in `api/swagger/` is updated automatically

---

## 🗂️ Project Structure

```
tram/
├── api/
│   ├── proto/stream/v1/stream.proto  # API definition
│   └── swagger/                      # Generated OpenAPI description
├── cmd/tram/                         # CLI entry point and `tram server`
├── internal/                         # Private application code
│   ├── adapters/
│   │   ├── api/                      # gRPC API and HTTP gateway
│   │   ├── broker/                   # Broker driver contract, drivers (nats/, kafka/, amqp/), conformance tests
│   │   ├── configsync/               # File sync (--enable-file-sync)
│   │   ├── driver/                   # Resolves the `driver` of a component spec and splits off its options
│   │   ├── k8scrd/                   # Kubernetes operator (--enable-k8s-operator)
│   │   ├── source/                   # Sources, base/ and factory.go
│   │   ├── stage/                    # Stages, base/ and factory.go; filter/ and validation/ with their drivers (cel/)
│   │   └── sink/                     # Sinks, base/ and factory.go
│   ├── core/                         # Builds, runs and stops streams
│   │   └── domain/                   # Events, specs, the stream tree, record filtering
│   ├── ports/                        # Interfaces between core and adapters
│   ├── e2e/                          # End-to-end pipeline tests
│   ├── scenario/                     # End-to-end pipeline benchmarks
│   └── testutil/                     # Test helpers: OTLP payloads (otlpgen), broker containers (natstest, kafkatest, amqptest), tcpproxy
├── pkg/gen/go/api/proto/stream/v1/   # Generated protobuf, gRPC and gateway code
├── examples/                            # Example configurations: config/, k8s/, nats/
├── tools/benchreport/                # Generates BENCHMARK.md
├── docs/assets/                      # Images used in the documentation
├── buf.yaml, buf.gen.yaml, buf.lock  # Protobuf generation
├── generate.go                       # Entry point for go generate
└── README.md
```

---

## 🧩 Adding New Components

TRAM is designed to be easily extensible. Each component is a package in `internal/adapters/{source|stage|sink}/<name>/` that embeds the matching base type and is registered in the factory next to it.

### Common Patterns

1. **Define a `Spec` type** for the configuration. `base.New` decodes the stream's `spec` into it and checks its `validate` tags ([go-playground/validator](https://github.com/go-playground/validator)); there is no `Validate()` method
2. **Implement the component** by embedding `base.Source[Spec]`, `base.Stage[Spec]` or `base.Sink[Spec]`. Checks the tags cannot express, like parsing durations, belong into `New`, so an invalid spec fails when the stream is created
3. **Register it** in the `switch` of `factory.go` in a new branch
4. **Document it** in [COMPONENTS.md](COMPONENTS.md)

Rules all components follow:

- **Events are read-only.** One event is passed to all branches concurrently. To change data, build a new event, e.g. with `domain.FilterLogs`, `FilterMetrics` and `FilterSpans`, which share unchanged records instead of copying them
- **`Notify` returns the delivery result.** Return an error wrapping `domain.ErrEventRejected` when retrying the same data cannot succeed; sources then reject it (OTLP: HTTP 422, broker: reject the message). Any other error is treated as temporary (OTLP: HTTP 503, broker: redelivery)
- **`Notify` may block** while it delivers the event downstream, or when waiting is its documented purpose (`batch`, `delay`). It has to return when `ctx` is done
- **Lifecycle:** `New` may acquire resources such as connections. `Stop` is called exactly once and must release them; it may be called without `Run`, e.g. when building the stream failed. The core stops the source first, waits for its `Run` to return, and then stops stages and sinks from the root down
- **Updates:** when an update changes only stages or sinks, the source keeps running. The core builds the new stages and sinks, switches the source over with `SetSubscribers`, which waits until no event is delivered to the old ones anymore, and then stops the old stages and sinks. Stages and sinks therefore have to be independent of a particular source instance

### Adding a New Source

Sources have no `Notify`; they implement `Run` and `Stop`. The base type provides `Name`, `Spec`, `Subscribe`, `SetSubscribers` and `NotifySubscribers`.

#### 1. Create the Spec

```go
// internal/adapters/source/heartbeat/spec.go
package heartbeat

type Spec struct {
	Interval string `json:"interval" validate:"required"`
	Message  string `json:"message" validate:"required"`
}
```

#### 2. Implement the Source

```go
// internal/adapters/source/heartbeat/heartbeat.go
package heartbeat

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-tram/tram/internal/adapters/source/base"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

const Type = "heartbeat"

type Heartbeat struct {
	*base.Source[Spec]
	logger   *slog.Logger
	interval time.Duration
	shutdown chan struct{}
	stopOnce sync.Once
}

// New takes a logger because Run logs; components that do not log leave it out.
func New(source domain.SourceSpec, validator *validator.Validate, logger *slog.Logger) (*Heartbeat, error) {
	// Decodes the spec and checks its validate tags.
	base, err := base.New[Spec](source.Name, source.Spec, validator)
	if err != nil {
		return nil, err
	}

	// Checks the tags cannot express belong here too, so an invalid spec
	// fails when the stream is created.
	interval, err := time.ParseDuration(base.Spec().Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %w", err)
	}

	return &Heartbeat{
		Source:   base,
		logger:   logger,
		interval: interval,
		shutdown: make(chan struct{}),
	}, nil
}

// Run produces events until Stop is called or ctx is cancelled.
func (h *Heartbeat) Run(ctx context.Context) error {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.shutdown:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			event := &domain.Event{Logs: &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{
				ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
					TimeUnixNano: uint64(time.Now().UnixNano()),
					Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: h.Spec().Message}},
				}}}},
			}}}}

			// Returns once all children have processed the event.
			if err := h.NotifySubscribers(ctx, event); err != nil {
				h.logger.ErrorContext(ctx, "Could not deliver event",
					slog.String("source", h.Name()), slog.Any("error", err))
			}
		}
	}
}

// Stop makes Run return. It may be called without Run, e.g. when building
// the stream failed, and must release everything New acquired.
func (h *Heartbeat) Stop() error {
	h.stopOnce.Do(func() { close(h.shutdown) })
	return nil
}

var _ domain.Source = (*Heartbeat)(nil)
```

#### 3. Register in Factory

```go
// internal/adapters/source/factory.go
func (f *Factory) New(spec domain.SourceSpec) (domain.Source, error) {
	switch spec.Type {
	// ... existing sources
	case heartbeat.Type:
		return heartbeat.New(spec, f.validator, f.logger)
	default:
		return nil, fmt.Errorf("%w: %q", domain.ErrInvalidSourceType, spec.Type)
	}
}
```

### Adding a New Stage

Stages implement `Notify` and pass events on with `NotifySubscribers`. The base type provides `Name`, `From`, `Spec`, `Subscribe` and a `Stop` that does nothing.

#### 1. Create the Spec

```go
// internal/adapters/stage/severity/spec.go
package severity

type Spec struct {
	Min int `json:"min" validate:"min=0,max=24"`
}
```

#### 2. Implement the Stage

```go
// internal/adapters/stage/severity/severity.go
package severity

import (
	"context"

	"github.com/go-tram/tram/internal/adapters/stage/base"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

const Type = "severity"

type Severity struct {
	*base.Stage[Spec]
}

func New(stage domain.StageSpec, validator *validator.Validate) (*Severity, error) {
	base, err := base.New[Spec](stage.Name, stage.From, stage.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &Severity{Stage: base}, nil
}

// Notify keeps the log records with at least the configured severity. The
// event is shared with sibling branches, so it is never modified; FilterLogs
// builds a new payload and shares the kept records.
func (s *Severity) Notify(ctx context.Context, event *domain.Event) error {
	logs, err := domain.FilterLogs(event.Logs, func(_ *resourcepb.Resource, _ *commonpb.InstrumentationScope, record *logspb.LogRecord) (bool, error) {
		return int(record.SeverityNumber) >= s.Spec().Min, nil
	})
	if err != nil {
		return err
	}

	filtered := *event
	filtered.Logs = logs
	if filtered.RecordCount() == 0 {
		return nil
	}
	return s.NotifySubscribers(ctx, &filtered)
}

var _ domain.Stage = (*Severity)(nil)
```

#### 3. Register in Factory

```go
// internal/adapters/stage/factory.go
	case severity.Type:
		return severity.New(spec, f.validator)
```

### Adding a New Sink

Sinks implement `Notify`. The base type provides `Name`, `From`, `Spec` and a `Stop` that does nothing.

#### 1. Create the Spec

```go
// internal/adapters/sink/webhook/spec.go
package webhook

type Spec struct {
	URL string `json:"url" validate:"required,url"`
}
```

#### 2. Implement the Sink

```go
// internal/adapters/sink/webhook/webhook.go
package webhook

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-tram/tram/internal/adapters/sink/base"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-playground/validator/v10"
	"google.golang.org/protobuf/proto"
)

const Type = "webhook"

type Webhook struct {
	*base.Sink[Spec]
	client *http.Client
}

func New(sink domain.SinkSpec, validator *validator.Validate) (*Webhook, error) {
	base, err := base.New[Spec](sink.Name, sink.From, sink.Spec, validator)
	if err != nil {
		return nil, err
	}
	return &Webhook{
		Sink:   base,
		client: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Notify posts the logs of an event as OTLP/HTTP protobuf. The returned
// error tells the source whether retrying can help.
func (w *Webhook) Notify(ctx context.Context, event *domain.Event) error {
	if event.Logs == nil {
		return nil
	}
	body, err := proto.Marshal(event.Logs)
	if err != nil {
		return fmt.Errorf("marshal logs: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.Spec().URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("send logs: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 500:
		// Temporary: the source may retry, e.g. the broker source has the message redelivered.
		return fmt.Errorf("send logs: %s", resp.Status)
	case resp.StatusCode >= 400:
		// Permanent: retrying the same data cannot succeed.
		return fmt.Errorf("%w: %s", domain.ErrEventRejected, resp.Status)
	}
	return nil
}

func (w *Webhook) Stop() error {
	w.client.CloseIdleConnections()
	return nil
}

var _ domain.Sink = (*Webhook)(nil)
```

#### 3. Register in Factory

```go
// internal/adapters/sink/factory.go
	case webhook.Type:
		return webhook.New(spec, f.validator)
```

### Tests and Benchmarks

- Test valid and invalid specs, and `Notify` with payloads from `internal/testutil/otlpgen`
- Check that `Notify` leaves the incoming event unchanged
- Add a benchmark for the per-event path (see [Benchmarks](#benchmarks))

---

## 📝 Pull Request Guidelines

### Before Submitting

1. **Generate code**: Make sure all code is generated
   ```bash
   task generate
   ```

2. **Format code**: Use the formatters configured for golangci-lint
   ```bash
   task fmt
   ```

3. **Lint and test**: Ensure linting and all tests pass
   ```bash
   task check
   ```

4. **Test against NATS**: If you changed the broker source or sink or a driver, also run the container tests
   ```bash
   task test:containers
   ```

5. **Test the operator**: If you changed the Kubernetes operator, also run the envtest tests
   ```bash
   task test:envtest
   ```

   `task test:all` runs steps 3 to 5 in one go.

### Pull Request Requirements

1. **Descriptive title**: Clearly describe what the PR does
2. **Detailed description**: Explain the changes and why they're needed
3. **Small, focused changes**: One logical change per PR
4. **Proper commit messages**: Follow [Conventional Commits](https://www.conventionalcommits.org/)
5. **Updated documentation**: Update relevant docs for any API changes
6. **Tests**: Add tests for new functionality
7. **Signed-off commits**: Sign off your commits (`git commit --signoff`) to accept the [Contributor License Agreement](CLA.md)

### Commit Message Format

```
feat: add new my-source component

- Add MySource implementation with base.Source
- Register in factory
- Add unit tests for spec validation

Closes #123
```

---

## 🎨 Code Style Guidelines

### General Principles

- Follow [Effective Go](https://go.dev/doc/effective_go)
- Use idiomatic Go patterns
- Keep code simple and readable
- Comment only what's not obvious

### Naming Conventions

- Use **camelCase** for variables and functions
- Use **PascalCase** for types and exported identifiers
- Use **snake_case** for JSON tags
- Type names should be nouns: `UserService`, not `GetUser`
- Interface names should end with `er`: `Logger`, `Writer`

### Formatting

- Use `gofmt` for formatting
- Line length: aim for 80-100 characters
- Indent with tabs, not spaces
- Group related imports and separate with blank lines

### Structure

- Keep functions short (aim for < 50 lines)
- Group related functions together
- Use receiver methods appropriately

### Error Handling

- Always check errors
- Provide context in error messages
- Use `fmt.Errorf` with `%w` for error wrapping
- Return errors, don't panic (except in truly exceptional cases)

```go
// Good
if err := doSomething(); err != nil {
    return fmt.Errorf("failed to do something: %w", err)
}

// Bad
if err := doSomething(); err != nil {
    panic(err)
}
```

---

## 🧪 Testing

### Unit Tests

- Place test files next to the code they test (`*_test.go`)
- Use table-driven tests for similar cases
- Test both happy paths and error cases

```go
func TestMyFunction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		err      bool
	}{
		{"success", "valid", "result", false},
		{"error", "invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := myFunction(tt.input)
			if (err != nil) != tt.err {
				t.Errorf("expected error: %v, got: %v", tt.err, err)
			}
			if !tt.err && result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}
```

### End-to-End Tests

`internal/e2e` tests complete pipelines without external dependencies: streams are built by the core from the real source and stage factories, OTLP data is sent over HTTP, and capturing sinks record exactly which records arrive where. They cover batching, splitting, cancelled requests, the status codes clients receive, and updates under load, and run with `go test ./...`.

`TestBrokerNATS` in the same package runs pipelines against a NATS server in a container (see `internal/testutil/natstest`) and checks what actually ends up in JetStream: subjects, plain OTLP bodies without headers, and records with their resource and scope. It also sends data from the broker sink through the broker source, reads plain OTLP that another producer published, and checks partitioning, buffering and the answers clients get. `TestBrokerKafka` and `TestBrokerAMQP` do the same against Kafka and against ActiveMQ Artemis over AMQP 1.0 (see `internal/testutil/kafkatest` and `amqptest`). They are skipped without Docker or Podman; with Podman on macOS, set the variables described under [Benchmarks](#benchmarks).

When adding a stage or changing how events flow, add a case there that checks which records arrive in which sink.

### Broker Drivers

A broker driver implements the contract in `internal/adapters/broker` and has to pass the conformance suite in `broker/brokertest` (`brokertest.Run`): publish and receive, acknowledge, retry after a delay, reject, a durable position across subscribers, cancellation, invalid destinations and the maximum message size. Run it against the broker in a container, as `broker/nats` does. Register the driver in `broker/drivers`, the only package that knows the concrete drivers; its options sit under its name in the component spec, which `driver.Split` takes apart. The broker sink only sees a driver's `PublisherDriver` side and the broker source its `SubscriberDriver` side, so neither can do the other's job. Unit tests of the broker source and sink inject the in-memory driver of `brokertest` through `NewWithDrivers` (see their `export_test.go`) and need no container.

### Stage Drivers

The `filter` and `validation` stages are concepts whose rules come from a driver, as the broker components' messaging does: the spec names it in `driver` and holds its options under its name, e.g. `driver: cel` and `cel: {...}`. `driver.Split` takes the spec apart, so the stage parses only its own fields (`on_mismatch` for validation) and the driver only its options.

The stage owns what every driver shares: walking the records and pruning what becomes empty (filter), dropping or rejecting (validation), and turning a failing rule into a rejection. A driver only compiles its options into rules:

- **filter:** `filter.Driver` returns `filter.Rules`, whose `Predicates` give a predicate per signal for one event. A nil predicate keeps all records of its signal
- **validation:** `validation.Driver` returns `validation.Rules`, which decide per signal whether the whole signal matches. A signal without a rule matches

Each stage is laid out like the broker components: the contract package (`rules`, `persist`), one package per driver (`cel`, `sqlite`) that knows only the contract, and a `drivers` package, the only one that knows the concrete drivers. The stage creates its `drivers` factory itself in `New`; `stage/factory.go` knows no drivers. To add a driver, e.g. OTTL, create `stage/filter/ottl` next to `stage/filter/cel` with a `Name` and a `Driver` and register it in `stage/filter/drivers`. Test it through the stage as `filter/cel` does, so the rules are checked with the stage's pruning and rejection. The depguard rules in `.golangci.yml` enforce the layout: factories import no drivers, and only a `drivers` package imports concrete drivers (tests excepted).

### Kubernetes Operator Tests

The `TestOperator*` tests in `internal/adapters/k8scrd` run the operator against a real kube-apiserver and etcd from [envtest](https://book.kubebuilder.io/reference/envtest), one per test. They check which `Stream` resources an instance runs with `--watch-namespace` and `--engine`, and that the generated RBAC role works with a namespaced `RoleBinding`. They are skipped unless `KUBEBUILDER_ASSETS` is set; `task test:envtest` downloads the binaries with `setup-envtest` and sets it.

### Integration Tests

- Use test containers for external dependencies (NATS, etc.), see `internal/testutil/natstest`
- Clean up resources after tests

### Benchmarks

Benchmarks cover the per-event hot path:

| Benchmark | Package | Measures |
|-----------|---------|----------|
| `BenchmarkNotifySubscribers` | `source/base`, `stage/base` | Fan-out cost per event and child |
| `BenchmarkPipeline` | `core` | A full stream with real stages, in records/s |
| `BenchmarkCELValidator` | `stage/validation/cel` | Expression evaluation per payload size |
| `BenchmarkCELFilter` | `stage/filter/cel` | Per-record filtering for different keep ratios |
| `BenchmarkFilter` | `core/domain` | The record filtering helpers without CEL |
| `BenchmarkNotify` | `sink/broker` | Grouping and publishing to NATS through the NATS driver (container) |
| `BenchmarkConsume` | `source/broker` | Receiving, decoding and acknowledging from NATS through the NATS driver (container) |
| `BenchmarkBatch` | `stage/batch` | Batch overhead and merging with concurrent callers |
| `BenchmarkHandleLogs` | `source/otlp` | Reading and decoding OTLP requests |
| `BenchmarkOTLPDataRoundTrip` | `core/domain` | Serializing events as OTLP data, as the broker sink and source do |
| `BenchmarkScenario` | `scenario` | Complete pipelines end to end: throughput, CPU per record, latency (container) |
| `BenchmarkStreams` | `scenario` | Cost of idle streams for 10 to 1,000 streams per instance: build time, heap, goroutines, NATS connections and requests of the broker sources and sinks, idle CPU (container; each case takes about 20 seconds, run it with `-benchtime 1x`) |

[BENCHMARK.md](BENCHMARK.md) presents these results. After performance-relevant changes, regenerate it with `task bench:report` (see the page for details); add new scenarios to `internal/scenario` and new components to the lists in `tools/benchreport`.

Benchmarks marked "container" start a NATS server with [Testcontainers](https://golang.testcontainers.org/) via `internal/testutil/natstest`. They need Docker or Podman and are skipped if neither is running. With Podman, Testcontainers needs the socket of the Podman service; the tasks set it up from `podman info` when Podman is installed. To run `go test` directly, set the same:

```bash
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=$(podman info --format '{{.Host.RemoteSocket.Path}}')
export TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED=true
# Podman on Linux only, not in a Podman machine on macOS or Windows:
export DOCKER_HOST=unix://$TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
```

Build payloads with `internal/testutil/otlpgen` and use `otlpgen.Shapes` so results are comparable across benchmarks. Use `b.Loop()`, call `b.ReportAllocs()`, and report `records/s` for payload-dependent benchmarks.

Compare a change against `main` with [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
git switch main && task bench COUNT=10 > old.txt
git switch my-branch && task bench COUNT=10 > new.txt
task bench:compare OLD=old.txt NEW=new.txt
```

`-p 1` runs one package at a time. Without it, `go test` benchmarks several packages in parallel, which then compete for CPU and distort each other's results. Other load on the machine, like a container VM that is just starting, has the same effect; check the spread between runs before trusting a result.

---

## 📚 Documentation

### Code Documentation

- Document exported types, functions, and methods
- Use complete sentences for doc comments
- Include examples where helpful

```go
// User represents a system user.
// It contains basic user information and access permissions.
type User struct {
    Name string
    Age  int
}

// Validate checks if the user data is valid.
// Returns an error if any field is invalid.
func (u *User) Validate() error {
    // implementation
}
```

### API Documentation

- The swagger file is automatically generated from protobuf definitions
- No manual updates needed when changing API endpoints

### Component Documentation

When adding new components:

1. Add to [COMPONENTS.md](COMPONENTS.md)
2. Include:
   - Configuration options
   - Usage examples
   - Behavior description
   - Error cases

---

## 🚢 Continuous Integration and Releases

### Checks on Pull Requests

The CI workflow (`.github/workflows/ci.yml`) runs on GitHub-hosted runners (`ubuntu-latest`) for every pull request and every push to main, as three parallel jobs: `task ci:lint` runs golangci-lint in the version pinned in `Taskfile.yml`, `task ci:test` the unit and end-to-end tests, and `task ci:chart` checks the Helm chart. That job regenerates the CRDs and roles with controller-gen and fails if they differ from the ones committed in `manifests/` and `charts/tram`, then runs `task chart:lint`: `helm lint --strict`, and the chart rendered with the default and other values validated against the Kubernetes schemas with kubeconform.

The actions the workflows use are pinned to the commit of a release, with the version in a comment; update both together.

### Releases

A release is a version tag on main. The release workflow (`.github/workflows/release.yml`) refuses tags that are not on main, runs the same lint, test and chart jobs, and then builds the image with Docker Buildx for `linux/amd64` and `linux/arm64` and pushes it to `ghcr.io/go-tram/tram` as the version without the leading `v` (e.g. `0.1.0`) and as `latest`. Then it publishes the Helm chart `charts/tram` to `oci://ghcr.io/go-tram/charts/tram` with that version as chart and app version, renders `manifests/` with the image of that version into `tram.yaml`, and attaches both to the GitHub Release of the tag, which it creates if it does not exist. In the repository the chart keeps the version `0.0.0` and the manifests the image tag `latest`; do not bump them.

Tag releases with the release tasks rather than by hand. They use [svu](https://github.com/caarlos0/svu) to compute the version from the existing tags, check that you are on main, that it matches `origin/main` and has no uncommitted changes, run `task check`, and then create and push the annotated tag:

```bash
task release:next    # from the Conventional Commits since the last tag: feat → minor, fix → patch, at least a patch
task release:patch   # v0.0.14 → v0.0.15
task release:minor   # v0.0.14 → v0.1.0
task release:major   # v0.0.14 → v1.0.0
```
---

## 🤔 Getting Help

If you have questions or need help:

1. Check existing issues and discussions
2. Open a new discussion for general questions
3. Open an issue for bugs or feature requests

We're happy to help you get started with contributing!

---

## 📜 License

TRAM is licensed under the [Mozilla Public License 2.0](LICENSE) (MPL-2.0). In short:

- You may use, modify, and distribute TRAM, including in commercial and proprietary products.
- If you distribute a modified version of TRAM's source files, your modifications to those files must be made available under the MPL-2.0.
- You may combine TRAM with your own code — including proprietary code — in a larger work without having to open your own code (MPL-2.0, Section 3.3 "Larger Work").

### Licensing your contributions (inbound = outbound)

By contributing to TRAM, you agree that:

1. Your contributions are licensed to everyone under the same [MPL-2.0](LICENSE) as the project.
2. You accept the [Contributor License Agreement (CLA)](CLA.md), which additionally grants the maintainer the right to relicense your contributions — for example for commercial licenses and premium features built on TRAM. You accept the CLA by signing off your commits (`git commit --signoff`) or by stating "I have read and agree to the TRAM CLA" in your pull request.
3. Submitting a contribution does not oblige the maintainer to accept or merge it.

### File headers

New source files must carry the MPL-2.0 header:

```go
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0
```

The header is enforced by the `goheader` linter (`task lint`). Generated files (marked `Code generated ... DO NOT EDIT`) are exempt.
