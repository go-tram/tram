# Kubernetes Operator - AI Agent Guide

This adapter embeds a controller-runtime operator into Tram. It was scaffolded with Kubebuilder, but it is not a standalone Kubebuilder project: there is no `Makefile`, no `PROJECT` file, no `cmd/main.go` and no separate deployment. The operator starts inside `tram server --enable-k8s-operator` and passes `Stream` resources to the core through `ports.Core`. The repository-wide rules in the root `AGENTS.md` apply as well.

## Project Structure

```
operator.go                                Creates and runs the controller manager inside Tram; limits its cache to the selected namespaces and labels
manager.go                                 Creates and runs the controller manager of `tram manager` (leader election, runs no streams)
api/v1/groupversion_info.go                API group io.tram.core/v1
api/v1/stream_types.go                     Stream schema (+kubebuilder markers)
api/v1/zz_generated.deepcopy.go            Generated (DO NOT EDIT)
api/v1/engine_labels.go                    engine.tram.io/<name> labels derived from spec.engineRefs
api/v1alpha1/                              API group io.tram.core/v1alpha1: Engine schema (+kubebuilder markers)
internal/controller/stream_controller.go   Reconciliation logic and RBAC markers
internal/controller/secrets.go             Resolves secret references in component specs
internal/engines/                          Controllers of `tram manager`: Engines as Deployments or, with storage, StatefulSets; drain Jobs for volumes without an instance; finalizers that keep buffered events until they are drained or expired; engine labels and status.engines of Streams
hack/boilerplate.go.txt                    Header for generated files
```

The generated CRDs and roles live in `manifests/` at the repository root (`crds/`, and `tram-manager/rbac/engine/` and `tram-manager/rbac/manager/`), with kustomizations, and the same files in the templates of the Helm chart `charts/tram` (`templates/crds/` and `templates/tram-manager/rbac/`). Example resources live in `examples/k8s/`.

## Critical Rules

### Never Edit Generated Files

`api/v1/zz_generated.deepcopy.go` and the CRDs and roles in `manifests/crds/` and `manifests/tram-manager/rbac/` are produced by controller-gen v0.22.0 from the `//go:generate` directives in `operator.go`, which write the CRDs and roles a second time into the chart's `charts/tram/templates/crds/` and `charts/tram/templates/tram-manager/rbac/`. After editing `*_types.go` or any marker, regenerate them:

```sh
go generate ./internal/adapters/k8scrd/...
```

Do not use `make manifests`, `make generate` or the `kubebuilder` CLI; they are not set up in this repository.

### Keep the Operator a Driver of the Core

- The reconciler only translates `Stream` resources into `domain.StreamSpec` and calls `ports.Core`. Building, running and stopping streams is the core's job
- Core stream names are `<namespace>.<name>` (see `StreamName`). Namespaces cannot contain dots, so this stays unambiguous
- Component specs are `*runtime.RawExtension` and are passed on unchanged as `map[string]any`, the same way the API and the file sync do

## Reconciliation Rules

- **Idempotent:** reconciling the same resource again must not restart its stream. Compare the desired spec with `Core.GetStream` and only call `UpsertStream` when they differ, because an upsert restarts the stream. A stream that refers to secrets or config maps is always passed on with `UpsertResolvedStream`: the core compares the resolved values too and keeps the stream if nothing changed
- **References (secrets, config maps):** resolve references only here, never in the core, so streams from the API cannot read secrets or config maps. Pass the resolved spec only as the second argument of `UpsertResolvedStream`; everything the core shows, including the file sync, gets the spec with the references. Read values through the API reader (`References`), not the cache; only the metadata of secrets and config maps is watched. Never write a resolved value back to a Kubernetes object: merged config map tables may exceed the size limit of one
- **Trigger on spec changes only:** the controller uses `GenerationChangedPredicate`, so status writes and resyncs do not reconcile. Keep it that way
- **Status:** only write it when it changes (`setActive`)
- **Deletion:** when the resource is gone, delete the stream and treat `domain.ErrStreamNotFound` as success
- **Selection:** `--watch-namespace` and `--engine` only limit the manager's cache (`cacheOptions`). Read `Stream` resources through the manager's client, never an uncached reader, so a resource that leaves the selection is reconciled as deleted
- **Permanent errors:** wrap errors that retrying cannot fix with `reconcile.TerminalError`; other errors are retried with backoff
- **RBAC:** declare permissions with `+kubebuilder:rbac` markers on the reconciler and regenerate
- **Manager RBAC:** markers in `internal/controller/` end up in the role of Tram instances, markers in `internal/engines/` in the role of `tram manager` (`manifests/tram-manager/rbac/manager/role.yaml`); keep them apart
- **Engine labels:** only `tram manager` writes labels with the prefix `engine.tram.io/`, derived from `spec.engineRefs` with `tramv1.WithEngineLabels`. Tram instances only select them (`--engine`) and never write labels. The labels are just an index for the cache: the instances' reconciler checks `spec.engineRefs` (`Stream.RefersToEngine`) before running a Stream, so never rely on the label alone
- **Status ownership:** Tram instances write `Stream.status.active`; `tram manager` writes `Stream.status.engines` and the whole `Engine.status`
- **Engine resources:** apply them server-side with `client.Apply` and field owner `engines.FieldOwner`, built as apply configurations in `internal/engines/resources.go`. Every generated resource carries `engines.ManagedByLabel`, since the manager's cache only holds resources with it
- **Buffered events:** never delete a volume claim of an Engine unless its drain Job completed, the drain deadline (largest buffer `max_age` + 1h) passed, or the discard annotation is set. Every claim either belongs to a running instance or has a drain Job (`EngineReconciler.drain`); keep that rule level-based
- **Replicas and autoscaling:** never drop `spec.replicas` from the Deployment's apply while the manager owns it alone; `EngineReconciler.replicas` hands it over to a second field manager first, or the API server resets it to 1

## API Design

Markers for `api/v1/*_types.go`:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// On fields:
// +kubebuilder:validation:Required
// +kubebuilder:validation:Minimum=1
// +kubebuilder:pruning:PreserveUnknownFields
```

- Follow the [Kubernetes API conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md): `spec`, `status`, `metadata`
- Prefer `metav1.Condition` for new status fields

## Logging

Use the controller-runtime logger from the context, which Tram routes to `slog`:

```go
log := logf.FromContext(ctx)
log.Info("Upserted Stream into core", "stream", name)
log.Error(err, "Failed to update status", "stream", name)
```

Follow the [Kubernetes logging message style](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md#message-style-guidelines): start with a capital letter, no trailing period, past tense, name the object type (`"Deleted Stream"`), balanced key-value pairs.

## Testing

Unit tests use the fake client from `sigs.k8s.io/controller-runtime/pkg/client/fake` with `WithStatusSubresource`, and a fake `ports.Core`; see `internal/controller/stream_controller_test.go`.

`operator_envtest_test.go` runs the whole operator against a real API server from envtest, connected through a kubeconfig file like `--kubeconfig`. Use it for behavior the fake client does not have: cache selection, watch events and RBAC. Each test starts its own API server, so tests stay parallel and do not need globals. Run them with `task test:envtest`; without `KUBEBUILDER_ASSETS` they are skipped. Several operators in one test binary need `WithoutNameValidation`, since controller-runtime requires unique controller names per process.
