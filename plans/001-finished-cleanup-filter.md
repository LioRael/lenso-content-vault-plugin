# Plan 001: Exclude successfully cleaned quarantine objects from future sweeps

> Drift check: `git diff --stat f21fca8..HEAD -- crates/content-vault/src/engine.rs crates/content-vault/migrations crates/content-vault/tests`.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `f21fca8`, 2026-08-30

## Why this matters

Successful cleanup records remain eligible forever, causing repeated object-store
deletes and database/WAL writes as history grows.

## Current state

- `engine.rs:679-707` builds session/part candidates without checking success time.
- `engine.rs:738-783` records success but does not make the row ineligible.

## Scope

In scope: cleanup queries, matching partial indexes through an additive migration if
needed, and sweeper tests. Out of scope: changing retention grace or object key format.

## Steps

1. Add a regression test that runs the sweeper twice and asserts the second run makes
   zero delete calls and zero cleanup-row updates.
2. Add `quarantine_cleanup_succeeded_at IS NULL` to both candidate arms and guarded
   update predicates so stale candidates cannot increment attempts after success.
3. Align partial indexes with the candidate predicates after inspecting existing plans.

## Verification

- `lenso-cargo test -p lenso-module-content-vault` -> all pass.
- `lenso-cargo check -p lenso-module-content-vault --all-targets` -> exit 0.
- `git diff --check` -> no output.

## STOP conditions

Stop if any documented workflow intentionally re-deletes successful objects; identify
that workflow before changing eligibility.
