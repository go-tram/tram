# 0001: Record Architecture Decisions

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

TRAM has many contracts: configuration fields, the API, delivery semantics, status codes, metric names. The reasons behind them were spread over pull request descriptions and discussions, and an external architecture review asked for a place that keeps them. More people now work on TRAM, and they need to tell deliberate trade-offs from gaps.

## Decision

Decisions are recorded as Architecture Decision Records in `docs/adr/`, one Markdown file each, numbered, with status, date, context, decision and consequences. They are part of the documentation site under Development → Decisions.

## Consequences

- A pull request that makes such a decision adds or supersedes a record
- Records describe why; the README and COMPONENTS.md stay the reference for how TRAM behaves, and records link there instead of repeating it
