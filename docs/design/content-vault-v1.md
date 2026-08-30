# Content Vault V1

## Decision

Content Vault is a removable Lenso Plugin because it owns a cohesive, durable business lifecycle: untrusted upload reservation, quarantine, validation, immutable commit, content identity, owner claims, integrity evidence, and safe cleanup. An S3 adapter alone would be infrastructure, not a Plugin.

The consuming Plugin continues to own domain objects, domain metadata, authorization decisions, and HTTP routes. Content Vault receives only a mandatory tenant plus an opaque owner reference. It never reads a consumer's private tables and it contains no consumer-specific vocabulary.

`lenso.content-vault@1` exposes reservation plus bounded resumable upload and
verified download Streams. The lower-level engine retains its bounded buffered
path for internal acceptance, but it is not the Plugin consumer Contract.

## Authority

PostgreSQL is authoritative for:

- upload session state (`reserved`, recoverable `staging` lease, `committed`, `rejected`, `expired`);
- tenant-scoped blob and content identities;
- active and released owner claims;
- idempotency receipts;
- append-only integrity observations; and
- exact quarantine keys eligible for cleanup.

The object store is authoritative only for bytes. A staged object is an observation until PostgreSQL records a committed content object. PostgreSQL uses a bounded `staging` lease only to keep expiry cleanup from racing an in-flight write; an abandoned lease is recoverable. Protected bytes use create-only writes and have no deletion capability in V1.

## Public seam

The generated `lenso.content-vault@1` Capability is the authoritative V1
consumer surface: `reserve`, `upload`, `describe`, `download`, `claim`,
`release_claim`, and explicit `sweep`. Owner calls require the exact
Kernel-resolved caller Instance to match the opaque owner grant. `sweep` also
requires an exact configured maintenance caller; a Capability binding alone
does not grant maintenance authority.

All portable string, UUID, digest, timestamp, byte-frame, offset, size, TTL,
and maintenance-count edges are bounded in generated JSON Schemas and checked
again by the native provider. Stream messages carry at most one encoded 8 MiB
chunk; legacy buffered content is segmented without changing byte order or
offset semantics.

The old linked Rust seam could compose a claim mutation with an owner component's
database transaction. A cross-Plugin Capability call cannot share that
transaction. Claim operations now commit in Vault-owned transactions, and
consumers use idempotency plus compensation or a future explicit Workflow for
multi-Plugin outcomes.

There is no generic product HTTP route. An owner Plugin may wrap the Capability
after doing its own business authorization.

Production Rust consumers cannot bypass Kernel authority through the old
engine, storage, migration, pool, or transaction types. Those seams are private
under every Cargo feature combination; acceptance suites compile only inside
the crate's test target. Schema setup, upgrade, and exact legacy adoption remain
deployment-operator actions. Legacy adoption requires an offline maintenance
window with all DDL-capable owner sessions stopped; its table locks cannot
prevent same-owner concurrent object creation. Runtime connect repeats the
exact catalog proof and fails closed on objects added after adoption.

Idempotency identity is the operation, tenant, complete opaque owner reference, and key. Actor and correlation values are first-attempt audit metadata; a legitimate retry may be reconstructed under a different actor or correlation without creating a second reservation.

## Failure semantics

| Failure point | Durable outcome | Retry behavior |
| --- | --- | --- |
| Reservation transaction fails | No reservation | Retry with the same key |
| Upload never arrives | Reserved until expiry | Stage or expire |
| Process stops while staging | Recoverable staging lease, with or without quarantine bytes | Stage can take over after the lease; expiry waits for the lease |
| Digest, size, MIME, or decode mismatch | Rejected; no content reference | Stable rejection |
| Protected create fails | Reservation remains; quarantine remains | Complete again |
| Protected create succeeds but DB commit fails | Possible protected orphan | Complete converges on the same content identity; V1 never deletes it |
| Sweeper quarantine delete fails | Content remains usable; quarantine remains | A later sweep retries the exact key |
| Protected content is missing or corrupt | Claims remain; read fails with integrity code and records an observation | Repair outside V1 |

## Security and isolation invariants

- Tenant is mandatory; no global fallback exists.
- A wrong tenant or owner observes `not_found`, not authorization or idempotency detail.
- Integrity read errors are returned only after the append-only observation is durable; observation persistence failure fails closed as database unavailable.
- Public values never contain quarantine or protected object keys.
- Content-address deduplication is tenant-scoped, so cross-tenant equality is not observable.
- Only the quarantine capability exposes deletion.
- Sweeping never lists a prefix and never guesses reachability. Terminal exact keys stay eligible for idempotent cleanup on every later pass.
- Releasing the last claim does not delete protected bytes in V1.

## Deferred

- Signed direct-upload tickets and a generic upload transport.
- Cross-Plugin claim transfer protocol.
- Retention policies and protected-content deletion.
- Malware scanning and additional media validators.
- Console surfaces and public events.
- A selected durable Scheduler/Jobs/Workflow integration for sweep cadence.

The bounded sweep remains an explicit operation. Content Vault does not recreate
the retired generic cron runtime or claim that a volatile Kernel task is a
durable scheduler. These additions require real consumers and independently
reviewed contracts.
