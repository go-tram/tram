# Releases

The changes of every TRAM release, newest first.

## [v0.0.19](https://github.com/GO-TRAM/TRAM/releases/tag/v0.0.19) - 2026-11-21

### Dependencies

- Bump the go-dependencies group across 1 directory with 17 updates ([#7](https://github.com/GO-TRAM/TRAM/pull/7))
- Update zensical requirement in /docs in the docs-dependencies group ([#6](https://github.com/GO-TRAM/TRAM/pull/6))
- Bump grafana/grafana from 11.5.2 to 13.3.1 in /examples/demo/grafana ([#5](https://github.com/GO-TRAM/TRAM/pull/5))

### Other

- Add buffer source and sink with SQLite driver ([#8](https://github.com/GO-TRAM/TRAM/pull/8))

[All changes since v0.0.18](https://github.com/GO-TRAM/TRAM/compare/v0.0.18...v0.0.19)

## [v0.0.18](https://github.com/GO-TRAM/TRAM/releases/tag/v0.0.18) - 2026-11-20

### Other

- Feature/dependabot pages ssh ([#4](https://github.com/GO-TRAM/TRAM/pull/4))
- Make the CI and release workflows work on GitHub Actions ([#3](https://github.com/GO-TRAM/TRAM/pull/3))

[All changes since v0.0.17](https://github.com/GO-TRAM/TRAM/compare/v0.0.17...v0.0.18)

## [v0.0.17](https://github.com/GO-TRAM/TRAM/releases/tag/v0.0.17) - 2026-11-20

### Other

- Update release.yml

[All changes since v0.0.16](https://github.com/GO-TRAM/TRAM/compare/v0.0.16...v0.0.17)

## [v0.0.16](https://github.com/GO-TRAM/TRAM/releases/tag/v0.0.16) - 2026-11-20

### Maintenance

- Move module to github.com/go-tram/tram
- Move module to codeberg.org/tram/tram

### Other

- Update pages.yml
- Update Repo path
- Move from forgejo to github actions

[All changes since v0.0.15](https://github.com/GO-TRAM/TRAM/compare/v0.0.15...v0.0.16)

## [v0.0.15](https://github.com/GO-TRAM/TRAM/releases/tag/v0.0.15) - 2026-11-20

### Documentation

- Fix unclosed quote in README component diagram

### Maintenance

- Run lint and tests as parallel jobs on codeberg-medium
- Add Forgejo Actions CI and release workflows

### Other

- Update .golangci.yml
- Update zensical.toml
- Read test stream files like file sync and fix lint findings
- Move test example to examples/tests directory
- Add stream testing support with tram test command
- Add dry run support for stream upserts
- Use the shared driver package for crypto drivers and fix lint findings
- Add decryption stage with local and STACKIT KMS drivers
- Add encryption stage with local and STACKIT KMS drivers
- Accept redeliveries in the AMQP reconnect test
- Wait for the ack in the broker source partition test
- Use package paths relative to the module in the Taskfile
- Split filter and validation stages into concept and driver
- Keep loadgen out of lint and tests and silence two lint findings
- Remove stream test references that slipped into the logo commit
- Remove broker technology references from sink documentation
- Use the PNG wordmark for the docs header logo and favicon
- Update .gitignore
- Harmonize the docs color scheme with the brand palette
- Actually lighten code blocks in dark mode
- Hide the permalink anchor on the docs hero wordmark
- Bring the docs hero wordmark back to 16rem
- Center the docs hero and calm the wordmark down
- Stack the docs hero wordmark above the text, lighten dark-mode code blocks, split Engine/Stream examples
- Show the TRAM wordmark instead of the TRAM heading on the docs start page
- Fix Pages workflow clone URL casing for the allowlist
- Restore TRAM logo assets referenced by the docs
- Update logo assets and references
- Switch documentation tooling from MkDocs to Zensical
- Delete 1
- Remove architecture lint rules and add logger injection to stage
- Add constants for repeated strings in tests and docs
- Add tests for stream hot swapping with sources stages and sinks

[All changes since v0.0.14](https://github.com/GO-TRAM/TRAM/compare/v0.0.14...v0.0.15)

## [v0.0.14](https://github.com/GO-TRAM/TRAM/releases/tag/v0.0.14) - 2026-11-19

### Features

- Delete stream-selector
- Add engine ref
- Move stream to k8s examples folder

### Bug Fixes

- Examples

### Other

- Add Engine resources run by tram manager
- Add tests for event reference sharing and fanout hot-swapping
- Split broker driver interface into publishing and subscribing sides
- Document Solace through the AMQP driver
- Add AMQP 1.0 broker driver
- Wait for new topics in kafkatest
- Add Kafka broker driver
- Update benchmark results for the broker source and sink
- Remove the nats source and sink in favor of broker
- Add broker source and sink with the NATS driver
- Add NATS JetStream broker driver
- Add broker driver contract, partitioning and test kit
- Revert "Add cloudevent sink and source"
- Add cloudevent sink and source
- Add config map selectors for Stream resources
- Add max_pending configuration to NATS sink
- Add value_map stage for attribute value lookup and mapping
- Update benchmark results and documentation
- Add secret reference resolution for Kubernetes operator
- Add OTLP URL routing with CEL expressions
- Update NATS connection handling and add batch size limiting
- Add NATS partitioning support
- Add NATS partitioning support
- Add NATS stream configuration options for replicas, discard policy and

[All changes since v0.0.7](https://github.com/GO-TRAM/TRAM/compare/v0.0.7...v0.0.14)

## [v0.0.7](https://github.com/GO-TRAM/TRAM/releases/tag/v0.0.7) - 2026-10-15

### Other

- Update benchmark results and increase NATS workers limit
- Add noop sink implementation
- Update benchmark results after performance improvements
- Update Taskfile for Podman remote detection and Go 1.27.2
- Add metrics configuration to streams
- Add pprof endpoint for diagnostics
- Update benchmark documentation with NATS load test results
- Update documentation URLs to gotram.dev
- Add documentation site configuration
- Add CLA and ignore tenant-routing-k8s setup
- Switch license to MPL 2.0
- Add CRD printer columns for Active and Age
- Update stream status with Patch instead of Update
- Update benchmark results from latest run
- Update gitignore to exclude loadgen tool output
- Remove loadgen tooling
- Refactor loadgen to validate data points per metric type
- Add load generator tool for tenant routing load test
- Update .gitignore
- Add routing by record attribute test
- Refactor partitioning logic into domain package and add fuzz tests
- Update NATS sink documentation and metrics routing
- Add duplicate key detection to OTLP source
- Fix trailing whitespace in test files
- Update OTLP source to use HTTP 422 for client errors and add retry-after
- Add hold mechanism to capture sink for testing
- Remove loadgen tooling
- Update benchmark results
- Update NATS sink retention policy and max age for benchmarking
- Add retry logic for failed stream syncs
- Add load generator tool with Dockerfile and main program
- Add request size metrics for OTLP source
- Fix NATS sink retention policy handling
- Update NATS sink benchmark to use max_age instead of retention
- Update NATS sink documentation with retention policy and max_age
- Add NATS stream retention policy and max_age support
- Update NATS sink to publish all messages before waiting for
- Remove redundant comments
- Add NATS JetStream compression support
- Update NATS source workers limit and documentation
- Add resource and scope accessors to NATS sink helper
- Add NATS subject variables for direct record access
- Add streams idle cost benchmark
- Update Grafana dashboard queries to include stream dimension
- Add test:all task and document it
- Add Kubernetes operator envtest support
- Add Kubernetes operator namespace and label selection
- Update batch sizes in demo configuration
- Add cancelled outcome for context cancellation
- Update demo configuration and documentation with load test results
- Simplify comments to clarify intent
- Add hold time validation to stream creation
- Add hold time validation for pipeline streams
- Update Docker volume mount options for demo config
- Add metrics for records passed through nodes
- Update metrics to measure sources
- Add Prometheus metrics endpoint and instrumentation
- Add traces support to demo pipeline and dashboards
- Add demo setup for OpenObserve integration
- Rename Loki demo setup directory to demo
- Add .claude/ to gitignore
- Remove .claude/settings.json
- Add Loki demo setup with OTLP pipeline
- Add Taskfile and container tooling
- Update documentation and add new API endpoints
- Add delivery timeout to OTLP source
- Add subject validation to NATS sink
- Update NATS sink and source components with stream management and retry
- Fix failed stream update handling
- Add API server error handling and driver cancellation
- Update OTLP source to support JSON payloads and compression
- Update CEL validator and NATS sink to reject invalid expressions
- Add automatic restart for crashed sources
- Add stream name validation to prevent directory escape
- Update configsync to handle unreadable files gracefully
- Update OTLP sink to handle multiple signals per event
- Update documentation to reflect Tram's broader scope beyond OTLP sinks
- Update documentation links to consolidated READMEs
- Rename example files to README
- Add OTLP and benchmark example configurations
- Add OTLP sink for OpenTelemetry endpoint delivery
- Optimize filter functions for OTLP signal processing
- Add export test helpers for components
- Update project configuration and logging to use slog
- Update NATS source to interrupt fetches immediately on Stop
- Add lint configuration and NATS integration tests
- Update end-to-end test documentation and add e2e package
- Add benchmark infrastructure and documentation
- Add `-p 1` flag to benchmark commands for accurate results
- Update NATS sink subject handling and grouping
- Add NATS benchmarks and containerized test utilities
- Update stream handling to support live updates
- BBAIC
- Add batch stage and benchmarks
- Remove macOS cruft
- Update project references from Stream to Tram
- Update error handling in gRPC API and improve Kubernetes CRD spec
- Update dependencies and improve core stream handling
- WIP k8s controller
- WIP first version of a k8s controller
- Update configsync.go
- Add stream existence check before upsert
- Fix configsync resource handling and cleanup backup files
- Add configuration sync with file watcher
- Fix CEL expression documentation in CEL Filter Stage
- Add Bruno API collection
- Add NATS sink with subject routing and component stop methods
- Add NATS sink documentation to COMPONENTS.md
- Add NATS sink (WIP) and refactor OTLP parsing
- Update OTLP source documentation to reflect HTTP-only protocol support
- Add NATS source component and update documentation
- Update CEL validator spec field names and references
- Add NATS source adapter and simplify base source
- Rename CELFilter to CELValidator and improve error handling
- Refactor CEL validator environment creation
- Fix CEL validator to use correct OTLP data structure
- Rename celfilter stage to celvalidator
- Update colored-logo.svg
- Add small colored logo asset
- Update logo reference in README
- Add example webp asset
- Rename cel_filter component to cel_validator
- Add project logo to README and docs
- Update CEL filter to support separate expressions for logs, metrics, and
- Add stream architecture diagram
- Support filtering logs metrics and traces separately in CEL filter
- Update diagram references and add example setup
- Add component documentation and update README
- Update CEL filter to use native domain.Event type
- Add error handling to OTLP source handlers
- Rename project from Stream to Tram
- Remove GRPC port validation
- Update event passing to use pointers consistently
- Update OTLP source to properly handle context cancellation
- Add error handling to subscriber notifications and introduce OTLP source
- Update README pipeline structure examples
- Update documentation with diagrams and remove CONTRIBUTION.md
- Use typed context keys for path tracking
- Add OTLP support to Event type
- Move Event type to dedicated file
- Add Node domain type for pipeline tree structure
- Update domain model to use tree structure
- Add parent references and processor field to Node struct
- Refactor tree structure to use dedicated Tree type
- Add tree domain model and utilities
- Rename DAG terminology to Tree terminology
- Add project documentation files
- Add spec fields to Source Stage and Sink messages
- Add project documentation files
- Add base adapters for sources, stages and sinks
- Add core shutdown and noop stage implementation
- Update core.go
- First working MVP
- Implement factory pattern
- Adjust core implementation
- Add tests and improve software architecture
- Code review comments
- Adjust api definition
- Initial API setup
- Create go.mod



