# Plan 001: Make terminal quarantine cleanup crash-safe and bounded

> Drift check: `git diff --stat 35d8daf..HEAD -- crates/content-vault/src crates/content-vault/migrations crates/content-vault/tests`.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: correctness/perf
- **Planned at**: commit `35d8daf`, 2026-08-30

## Why this matters

Completed cleanup must not cause repeated object-store deletes and PostgreSQL writes,
but a writer that may persist bytes after a sweep cannot be marked permanently clean.
Process termination between an immutable put and database reconciliation otherwise leaves
an object that no later bounded sweep can discover safely.

## Current state

Plugin-first `main` retains the exact-key quarantine engine and explicit bounded sweep.
The V1/V2 schema records cleanup attempts but has no durable writer-resolution marker,
so successful missing-object deletion can race a late or interrupted streaming writer.

## Scope

In scope: one additive Plugin-owned migration, exact attempt-key fencing, writer
resolution, bounded candidate/index predicates, Operator V3 setup and upgrade, and
PostgreSQL crash-window coverage. Out of scope: changing retention grace, protected
object deletion, or restoring the removed Host/cron surfaces.

## Steps

1. Add `writer_resolved_at` constraints and partial indexes while clearing legacy cleanup
   success for writers whose final state is unknown.
2. Mark successful, uncertain, and failed puts resolved only at a durable boundary;
   exact-delete fenced late writes and keep interrupted writes sweep-eligible.
3. Exclude truly completed cleanup while rotating unresolved rows through the existing
   batch order so one row cannot monopolize the bounded sweep.
4. Advance the Plugin Operator plan to V3 and verify fresh setup, V1/V2 upgrades,
   adoption, late writers, process abort, legacy migration, and repeated sweeps.

## Verification

- `lenso-cargo test --locked --workspace --features postgres-acceptance` -> all pass.
- `lenso-cargo check --locked --workspace --all-targets --all-features` -> exit 0.
- `lenso-cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` -> exit 0.
- `lenso-cargo fmt --all -- --check` -> exit 0.
- `git diff --check` -> no output.

## STOP conditions

Stop if a cleanup path cannot prove both its exact object key and durable writer
resolution; leave that row retryable rather than recording terminal cleanup success.
