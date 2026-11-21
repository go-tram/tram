# Kubernetes Operator

Lets TRAM run streams described by `Stream` custom resources (`io.tram.core/v1`). The operator is part of the TRAM binary and starts together with the server; it is not deployed separately.

## Running

1. Install the custom resource definition:

   ```sh
   kubectl apply -f manifests/crds/io.tram.core_streams.yaml
   ```

2. Start TRAM with the operator enabled:

   ```sh
   ./tram server --enable-k8s-operator
   ```

   TRAM connects with the in-cluster configuration when it runs in a pod, otherwise with the `KUBECONFIG` environment variable or `~/.kube/config`. `--kubeconfig <file>` selects a specific file.

3. Create a `Stream`, e.g. the example in `examples/k8s/`:

   ```sh
   kubectl apply -f examples/k8s/examples/otlp_basic.yaml
   ```

When TRAM runs in a cluster, its service account needs the permissions in [`manifests/tram-manager/rbac/engine/role.yaml`](../../../manifests/tram-manager/rbac/engine/role.yaml) (a `ClusterRole` named `manager-role`), bound with a `ClusterRoleBinding`. With `--watch-namespace`, a `RoleBinding` to that `ClusterRole` in each watched namespace is enough. The role includes reading `Secret` and `ConfigMap` resources, for [references to secrets](#secrets) and [config maps](#config-maps).

## Selecting Streams

By default every TRAM instance with the operator enabled runs every `Stream` resource in the cluster. Two flags limit an instance to some of them; when both are set, a resource has to match both:

| Flag | Description |
|------|-------------|
| `--watch-namespace` | Comma-separated namespaces whose `Stream` resources the instance runs, e.g. `tram-validation,tram-bridge` |
| `--engine` | [Engine](#engines) whose `Stream` resources the instance runs: those whose `spec.engineRefs` name it. `tram manager` sets it on the instances of each Engine |

- A resource that no instance selects runs nowhere, and its `status.active` is not set
- With `--engine`, the instance watches only the resources labeled `engine.tram.io/<engine>`, which `tram manager` derives from `spec.engineRefs`, and runs only those whose `spec.engineRefs` still name the engine. Removing the reference stops the stream on the engine's instances; adding it starts the stream
- `--engine` only assigns streams to instances, it is no access control. Anyone who can create a `Stream` in a watched namespace can name any engine. Use `--watch-namespace` and RBAC to restrict who can run streams on an instance

## Engines

An `Engine` (`io.tram.core/v1alpha1`) is a group of TRAM instances that `tram manager` runs as a Deployment, or as a StatefulSet if it has [storage for buffers](#storage-for-buffers). A `Stream` names the engines that run it in `spec.engineRefs`, and every instance of each of them runs it:

```yaml
apiVersion: io.tram.core/v1alpha1
kind: Engine
metadata:
  name: telemetry-hub-mapper
  namespace: tram-nats-13
spec:
  replicas: 3                       # used while autoscaling is disabled
  image: registry.example.com/tram:1.14.0   # optional, defaults to the manager's --engine-image
  imagePullPolicy: IfNotPresent
  resources:
    requests: { cpu: "4", memory: 8Gi }
    limits: { memory: 12Gi }
  autoscaling:                      # optional
    enabled: true                   # default false
    minReplicas: 3                  # required while enabled
    maxReplicas: 24                 # required while enabled, >= minReplicas
    targetCPUUtilizationPercentage: 70   # default 80
  disruptionBudget:                 # optional, one of minAvailable and maxUnavailable
    maxUnavailable: 1
  ports:                            # ports the HTTP sources of its Streams may listen on
    - name: otlp-http
      port: 4318
  service:
    type: ClusterIP                 # default
  runtime:                          # maps to flags of tram server
    logLevel: info                  # --log-level, default info
    api:
      httpPort: 8080                # --http-endpoint, also serves /metrics; default 8080
      grpcPort: 50051               # --grpc-endpoint; default 50051
    pprof:                          # optional, --pprof-endpoint on the pod's localhost
      port: 6060
  podTemplate:                      # pod-level settings only
    metadata:
      labels: { team: telemetry }
      annotations: { prometheus.io/scrape: "true", prometheus.io/port: "8080" }
    spec:
      terminationGracePeriodSeconds: 30
      priorityClassName: telemetry-high
      nodeSelector: { node-pool: telemetry }
      tolerations: [...]
      affinity: {...}
      topologySpreadConstraints: [...]
      securityContext: {...}
      imagePullSecrets: [...]
  storage:                          # optional, see Storage for buffers
    size: 20Gi                      # cannot be changed
    storageClassName: fast-ssd      # optional, cannot be changed
    mountPath: /var/lib/tram        # default
    drainResources:                 # default: requests 50m CPU, 128Mi; the instances' limits
      requests: { cpu: 50m, memory: 128Mi }
```

```yaml
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: resource-to-router-audit
  namespace: tram-nats-13
spec:
  engineRefs:
    - name: telemetry-hub-mapper
  source: ...
```

The Helm chart [`charts/tram`](../../../charts/tram/) or the kustomization in [`manifests/`](../../../manifests/) deploys the manager with the CRDs and roles, and `examples/k8s/examples/` holds an Engine and Streams that run on it.

### The Manager

`tram manager` runs no streams. It runs one Deployment or StatefulSet per `Engine`, and it labels every `Stream` `engine.tram.io/<name>` (with an empty value) for each engine its `spec.engineRefs` name; the instances of an engine select the key, e.g. `kubectl get streams -l engine.tram.io/engine-1`. Run it once per cluster, or as several replicas, of which one at a time works:

```sh
./tram manager --engine-image registry.example.com/tram:1.14.0
```

### RBAC

| Role | File | Bound to |
|------|------|----------|
| `tram-manager` | `manifests/tram-manager/rbac/manager/role.yaml` | The manager's service account, with a `ClusterRoleBinding` (a `RoleBinding` per namespace with `--watch-namespace`). Engines, Streams, the generated resources including StatefulSets, drain Jobs and volume claims, reading pods, leader election, and `bind` on `manager-role` |
| `manager-role` | `manifests/tram-manager/rbac/engine/role.yaml` | Each Engine's service account, by the manager, in the Engine's namespace |

Both `ClusterRole`s have to exist. With another `--engine-cluster-role`, grant the manager `bind` on that role instead.

### Storage for buffers

The [buffer sink and source](../../../COMPONENTS.md#buffers) keep their queues on the instance's disk. With `spec.storage`, the manager runs the Engine as a StatefulSet in which every instance has a volume of its own (`ReadWriteOnce`, claim `data-tram-engine-<engine>-<ordinal>`) mounted as its data directory. Without it, the Engine runs as a Deployment, and Streams with a buffer sink or source do not run on it. Every such Stream reports per Engine in `status.engines[].conditions` whether it can: `StorageAvailable` is `True`, or `False` with reason `EngineHasNoStorage`, or `StorageMigrating` while the instances move to a StatefulSet.

**Rule: buffered events are never discarded before their `max_age` because of a change in Kubernetes.** The manager keeps every volume either with a running instance or with a drain Job, and deletes a volume only when its drain Job finished, or once everything on it has expired.

| What happens | What the manager does |
|---|---|
| `storage` is added | Starts a StatefulSet with as many instances as the Deployment, next to it; both serve meanwhile. Once the StatefulSet is ready, the Service selects only its instances (label `tram.io/storage: "true"`), then the Deployment is deleted, so no request reaches an instance without storage. `Migrating` is `True` meanwhile |
| `storage` is removed | Waits while Streams with a buffer sink or source still run on the Engine: `Reconciled` is `False` with reason `StorageInUse`, naming them. Once none is left, starts a Deployment, and when it is ready, deletes the StatefulSet; its volumes stay and are drained |
| Scaling down, also by the autoscaler | The volume of an instance that goes stays and is drained |
| Scaling up while a volume is drained | Stops the drain Job; the instance continues with its volume |
| The Engine is deleted | Keeps it (finalizer `tram.io/drain-buffers`) until all its volumes are drained, after stopping its instances |

A **drain Job** (`tram-drain-<random>`, labels `tram.io/drain=<engine>` and `tram.io/ordinal=<n>`) runs `tram drain` with the volume mounted: it loads the Engine's Streams and runs only those that read from a buffer, treating all others as suspended, so nothing writes to the volume any more. It ends once every queue is empty or holds only expired events, also queues no Stream reads any more, so it takes at most about the largest `max_age` after the instance stopped. Its pod is not selected by the Engine's Service or disruption budget and uses `storage.drainResources`. If a drain Job fails after its retries, the volume is kept and the Engine's `Draining` condition says so; delete the Job to try again. If a drain Job never finishes, e.g. because its volume cannot be attached, the volume is deleted once the Job is older than the largest `max_age` of the Engine's buffer sinks (at least 24 hours) plus one hour, since everything on it has expired by then.

Deleting or suspending a Stream discards nothing: what its buffer sink wrote waits in the queue until a reading Stream delivers it or it expires, like messages of a broker without a consumer.

The Engine's status shows the workload (`status.workload`), the number of volumes (`status.volumes`) and of volumes being drained (`status.drainingVolumes`), and the conditions `Migrating` and `Draining`.

To give up buffered events on purpose, annotate the Engine with `tram.io/discard-buffers: "true"` before or while deleting it; its volumes are deleted at once.

Limits:

- **Deleting a namespace discards buffered events.** Kubernetes deletes everything in it, and nothing can be created in it any more, so no drain Job can run. Delete the Streams and Engines first and wait until they are gone
- `size` and `storageClassName` cannot be changed; resize the volumes by hand if the storage class allows it
- When a node fails, the StatefulSet does not start its instance elsewhere until the pod is gone; the volume is delivered once the instance runs again
- PVCs need Kubernetes 1.32 or later for `persistentVolumeClaimRetentionPolicy`; older clusters ignore it, and the manager's drains still delete volumes only when they are done

## The Stream Resource

```yaml
apiVersion: io.tram.core/v1
kind: Stream
metadata:
  name: test-stream
  namespace: default
spec:
  source:
    name: generator
    type: generator
    spec:
      interval: 2s
  stages:
    - name: delay
      type: delay
      from: generator
      spec:
        delay: 1s
  sinks:
    - name: debug
      type: debug
      from: delay
```

`spec` has the same structure as the `stream` object of the HTTP API, without `name`. Each component's `spec` is passed to the component as it is, so numbers, booleans and nested objects keep their types; see [COMPONENTS.md](../../../COMPONENTS.md) for the options of each component.

## Secrets

Any value in a component's `spec` can come from a `Secret` in the namespace of the `Stream`, like environment variables of a pod:

```yaml
  sinks:
    - name: otlp
      type: otlp
      from: in
      spec:
        url: https://collector.example.com:4318
        headers:
          Authorization:
            valueFrom:
              secretKeyRef:
                name: otlp-auth   # Secret in the namespace of the Stream
                key: token        # e.g. "Bearer ..."
```

- The value replaces the whole object as a string, so a header needs its complete value in the secret, e.g. `Bearer ...`
- The stream runs with the values, but TRAM shows only the references: the API and the files of the file sync never contain a secret's value. A component may still name a value in an error it logs, e.g. an invalid URL, so keep secrets out of values that components check, like URLs
- Changing a secret restarts the streams that refer to it with the new value. TRAM watches only the metadata of secrets and reads a value when it applies a stream, so values are not kept in its cache
- A missing secret or key keeps the stream from starting and sets `status.active` to `false`; the stream starts once the secret is there. A malformed reference, e.g. without `key`, fails for good until the resource is fixed
- References work only in `Stream` resources. Streams from the API or the file sync are passed on unchanged, so they cannot read secrets or config maps

## Config Maps

A value in a component's `spec` can also be the merged `data` of all `ConfigMap` resources in the namespace of the `Stream` that a label selector matches. This keeps large tables, e.g. the table of a [`value_map`](../../../COMPONENTS.md#value-map-stage) stage, out of the `Stream` resource and lets them grow beyond the size limit of a single Kubernetes object:

```yaml
  stages:
    - name: router
      type: value_map
      from: in
      spec:
        source_attr: tenant.id
        target_attr: router.id
        default: unassigned
        table:
          valueFrom:
            configMapSelector:        # like the selector of a Deployment
              matchLabels:
                tram.io/table: tenant-routing
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: tenant-routing-router-1
  labels:
    tram.io/table: tenant-routing
data:
  tenant-a: router-1
  tenant-b: router-1
```

- The selector takes `matchLabels` and `matchExpressions`, at least one of them. The value is an object with the keys and values of `data` of every matching config map; `binaryData` is ignored
- Split a table over as many config maps as needed, e.g. one per router. A key may be in several of them only with the same value. Different values are a conflict: the error names the key and both config maps
- Creating, changing or deleting a matching config map, or changing labels so that it starts or stops matching, applies the new table. Like with a changed spec, only stages and sinks are replaced; the source keeps running. As with secrets, TRAM watches only the metadata of config maps and reads their data when it applies a stream
- No matching config map, or a conflict, keeps the stream from starting and sets `status.active` to `false`. A stream that already runs keeps running with its last table until the config maps are fixed. A malformed selector, e.g. an empty one, which would match every config map of the namespace, fails for good until the resource is fixed
- The merged table exists only in TRAM's memory: it is never written to a Kubernetes object, so only each config map on its own is bound by the size limit. The API and the file sync show the selector, not the table
- Every TRAM instance that runs the stream holds the whole table, and every reconcile reads all matching config maps. Keep tables of hundreds of thousands of entries in mind when sizing memory

## Behavior

- **Names:** a resource runs as the TRAM stream `<namespace>.<name>`, e.g. `default.test-stream`, so resources with the same name in different namespaces do not collide. The stream can be read through the API under that name
- **Changes:** the stream is created or updated when the resource's `spec`, a secret it refers to, or a config map it selects changes. Status updates and periodic resyncs do not restart it
- **Status:** `status.active` is `true` once the stream runs, and `false` if building it failed, e.g. because of an invalid component spec. Failed builds are retried with increasing delays. If the stream's source stops later, TRAM restarts the stream on its own; `status.active` stays `true` meanwhile
- **Deletion:** deleting the resource deletes the stream
- **Scope:** the operator watches `Stream` resources in all namespaces, or in the namespaces from `--watch-namespace`, and with `--engine` only those whose `spec.engineRefs` name the engine; see [Selecting Streams](#selecting-streams). There is no leader election, so every TRAM instance with the operator enabled runs every `Stream` resource it selects
- **Access:** creating a `Stream` lets its author open ports on the TRAM pod and send data anywhere TRAM can reach, from any namespace. Its author can also have TRAM read any secret or config map in the `Stream`'s namespace and send the value along, e.g. as a header to their own URL. Grant `create` on `streams.io.tram.core` only to users you would also trust with the TRAM deployment and with the secrets and config maps of that namespace; see [Security](../../../README.md#-security)
- **File sync:** with `--enable-file-sync`, streams created by the operator are also written to the configuration directory

## Development

| Path | Content |
|------|---------|
| `api/v1/stream_types.go` | The `Stream` type and its kubebuilder markers |
| `api/v1/zz_generated.deepcopy.go` | Generated |
| `api/v1/engine_labels.go` | The engine labels derived from `spec.engineRefs` |
| `internal/controller/stream_controller.go` | Reconciliation and the RBAC markers |
| `api/v1alpha1/engine_types.go` | The `Engine` type and its kubebuilder markers |
| `internal/engines/` | The controllers of `tram manager`: `engine_controller.go` runs Engines, `resources.go` builds their resources, `stream_controller.go` derives engine labels and `status.engines`; with their RBAC markers |
| `operator.go` | Starts the controller manager inside TRAM |
| `manager.go` | Starts the controller manager of `tram manager`, with leader election; caches only the resources it generates |

After changing types or markers, regenerate the deepcopy code and the manifests with [controller-gen](https://book.kubebuilder.io/reference/controller-gen) v0.22.0. They write the CRDs to `manifests/crds/` and the roles to `manifests/tram-manager/rbac/` at the repository root, and the same files to the templates of the Helm chart `charts/tram` (`templates/crds/` and `templates/tram-manager/rbac/`):

```sh
go generate ./internal/adapters/k8scrd/...
```
