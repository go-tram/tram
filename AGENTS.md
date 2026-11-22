# AI Assistant Guidelines for Tram Project

This file contains project-specific guidelines for AI coding assistants working on the Tram project. All AI agents should follow these instructions to maintain consistency and quality.

## Project Context

Tram is a cloud-native, in-memory data pipeline framework that:
- Uses tree-structured pipelines (1 source → N stages → M sinks)
- Manages streams through the API, optionally synced with JSON files (`configsync`) and Kubernetes custom resources (`k8scrd`); all processing is in memory
- Uses protobuf for API definitions (swagger auto-generated)
- Follows clean architecture with clear separation between core and adapters

## Instructions for All AI Agents

### General Guidelines

1. **Architecture First**: Always consider the impact on the clean architecture
2. **API-First**: Changes to the API must start with protobuf definitions
3. **Type Safety**: Prefer strongly-typed solutions over dynamic ones
4. **Cloud-Native**: Remember this is a cloud-native, in-memory system

### Code Style

- Follow existing patterns in the codebase
- Use `base.Source[Spec]`, `base.Stage[Spec]`, `base.Sink[Spec]` for components
- Use `base.New[Spec]()` pattern for base instantiation
- Pass `validator *validator.Validate` to New functions
- Register new components in the `switch` of the matching `factory.go` (in feature branches)

### Development Workflow

1. For API changes:
   - Modify `.proto` files first
   - Run `go generate ./...` to regenerate code
   - Swagger file updates automatically

2. For new components:
   - Create spec.go with typed configuration
   - Implement component using base classes
   - Add to factory in a new branch
   - Document in COMPONENTS.md

3. Always:
   - Run `go generate ./...` before committing
   - Run `go test ./...` to verify changes
   - Use `gofmt` for formatting

### Follow-ups (do in separate PRs, after the branch that introduced them merges)

- **Metric naming contract:** done on `feature/metric_validation`. `internal/metrics` holds the prefix, attribute keys and kind values; `internal/core/metrics.go` and `internal/adapters/stage/valuemap/valuemap.go` build their names and labels from it; docs/content/guide/metrics.md documents the schema; `TestMetricsFollowTheNamingContract` in `internal/e2e/` collects every instrument of a running pipeline and asserts name grammar, label keys, the stream label, and description and unit, so a component that ignores the contract fails a test.
  Context: introduced with the stream-labeled value_map metrics on the buffer-stage branch.
- Declarative on/off for the metrics of components (like the lookup outcomes of `value_map`) would need a proto/API/CRD change; do it once a second component records its own.

### Review Focus Areas

When reviewing or generating code, all AI agents should pay special attention to:

1. **Architecture Compliance**:
   - Components use base classes correctly
   - Factory registration follows the pattern
   - No direct dependencies between adapters

2. **API Consistency**:
   - Protobuf definitions are clean
   - Swagger documentation is auto-generated
   - No manual API documentation needed

3. **Error Handling**:
   - Errors are properly wrapped with context
   - Validation happens at component creation
   - Failures don't crash the pipeline

4. **Thread Safety**:
   - Shared state is properly protected
   - Goroutines are properly managed
   - No race conditions in concurrent access

### Component Development

When creating new components (sources, stages, sinks), all AI agents should:

1. **Spec Definition**:
   ```go
   type Spec struct {
       Field1 string `json:"field1" validate:"required"`
       Field2 int    `json:"field2" validate:"min=1"`
   }
   ```

2. **Component Implementation** (a stage; sources use `base.New[Spec](spec.Name, spec.Spec, validator)` without `From` and implement `Run` and `Stop`):
   ```go
   type MyStage struct {
       *base.Stage[Spec]
       // Additional fields
   }

   func New(spec domain.StageSpec, validator *validator.Validate) (*MyStage, error) {
       base, err := base.New[Spec](spec.Name, spec.From, spec.Spec, validator)
       if err != nil {
           return nil, err
       }
       return &MyStage{Stage: base}, nil
   }

   // Notify must not modify event; it is shared with sibling branches.
   func (s *MyStage) Notify(ctx context.Context, event *domain.Event) error {
       return s.NotifySubscribers(ctx, event)
   }
   ```

3. **Factory Registration**:
   ```go
   // internal/adapters/stage/factory.go (in feature branch)
   func (f *Factory) New(spec domain.StageSpec) (domain.Stage, error) {
       switch spec.Type {
       // ... existing stages
       case mystage.Type:
           return mystage.New(spec, f.validator)
       default:
           return nil, fmt.Errorf("%w: %q", domain.ErrInvalidStageType, spec.Type)
       }
   }
   ```

4. **Documentation**:
   - Add to COMPONENTS.md
   - Include configuration options
   - Add usage examples

### Testing Guidelines

All AI agents should ensure:
- Use table-driven tests for component logic
- Test both valid and invalid configurations
- Mock external dependencies in unit tests
- Test pipeline wiring and data flow end to end in `internal/e2e/`: which records arrive in which sink

### Protobuf Guidelines

All AI agents must follow these protobuf rules:
1. Always use the latest protobuf syntax (proto3)
2. Use `google.protobuf.Struct` for map[string]any fields
3. Include proper validation annotations
4. Keep message definitions clean and focused

### File Structure

All AI agents should respect this project structure:
- All new components go in `internal/adapters/{source|stage|sink}/`
- Base classes are in `internal/adapters/{source|stage|sink}/base/`
- Core logic is in `internal/core/`, domain types and record filtering in `internal/core/domain/`
- Interfaces between core and adapters are in `internal/ports/`
- Configuration drivers are in `internal/adapters/api/`, `configsync/` and `k8scrd/`
- Test helpers are in `internal/testutil/`, end-to-end pipeline tests in `internal/e2e/`, end-to-end pipeline benchmarks in `internal/scenario/`
- Development tools are in `tools/`; `tools/benchreport` generates `BENCHMARK.md`
- API definitions are in `api/proto/`
- Generated code goes in `pkg/gen/`

### Common Pitfalls to Avoid

All AI agents should be aware of these common issues:
1. **Circular Dependencies**: Don't create circular imports between packages
2. **Global State**: Avoid global variables; use dependency injection. This includes logging: components that log take a `*slog.Logger` in `New` (passed on by their factory) and log with the `...Context` methods; only `cmd/tram/main.go` creates the logger
3. **Blocking Operations**: Only block in Notify while delivering downstream or when waiting is the component's documented purpose (`batch`, `delay`); always return when the context is done
4. **Shared Events**: Never modify an incoming event; build a new one (e.g. with `domain.FilterLogs`) instead
5. **Live Updates**: Stages and sinks can be replaced below a running source (`Source.SetSubscribers`); never keep state in a stage or sink that assumes it lives as long as the source
6. **Memory Leaks**: Always clean up goroutines and resources in Stop
7. **Inconsistent Error Handling**: Always wrap errors with context; wrap `domain.ErrEventRejected` for permanent failures

## Example Commands

```bash
# Build the project
go build -o tram ./cmd/tram

# Run tests
go test ./...

# Generate code (protobuf, etc.)
go generate ./...

# Format code
gofmt -w .

# Run service
./tram server
```

## Response Guidelines for All AI Agents

When generating code or providing feedback, all AI agents should:
1. Point out architectural concerns first
2. Suggest idiomatic Go patterns
3. Ensure thread safety is considered
4. Verify error handling is consistent
5. Check that the factory pattern is followed for new components
6. Maintain the existing code style and patterns
7. Provide complete, working examples
8. Explain the reasoning behind suggestions
