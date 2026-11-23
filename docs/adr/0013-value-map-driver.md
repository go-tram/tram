# 0013: A Driver for value_map Only When Needed

- **Status:** Proposed
- **Date:** 2026-11-23

## Context

`value_map` maps attribute values through a table in its spec, from config maps or from secrets. Mappings that change often, e.g. tenant to router, could come from an external store such as Redis.

## Decision

`value_map` does not get a `driver` field now. When an external store is needed, it becomes concept and driver ([0004](0004-components-as-concept-and-driver.md)): `driver: table` for today's tables, `driver: redis` with its options. The stage-level fields (`source_attr`, `target_attr`, `default`, `level`, metrics) stay. The change is breaking, since `driver` becomes required, which before 1.0 is a minor version.

## Open Before a Redis Driver

1. **Speed:** a lookup over the network per record would cost far more than today's ~125 ns; it needs a cache with a TTL and one batched lookup per event
2. **Store unavailable:** set `default`, or answer `503` so senders retry; a stage-level field such as `on_unavailable`
3. **Consistency:** a changed mapping applies only after the cache TTL, so data briefly follows the old one
