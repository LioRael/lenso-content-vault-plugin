# Content Vault V1

## Decision

Content Vault is a linked Rust Lenso Module because it owns a cohesive, durable business lifecycle: untrusted upload reservation, quarantine, validation, immutable commit, content identity, owner claims, integrity evidence, and safe cleanup. An S3 adapter alone would be infrastructure, not a Module.

The consuming Module continues to own domain objects, domain metadata, authorization decisions, and HTTP routes. Content Vault receives only a mandatory tenant plus an opaque owner reference. It never reads a consumer's private tables and it contains no consumer-specific vocabulary.

The V1 transport boundary is a complete in-memory byte buffer with a default
64 MiB limit. Streaming and multipart ingestion are intentionally outside this
revision; an owner with a larger contract must not silently route those objects
through V1.

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

The Rust API is the authoritative V1 surface:

1. `reserve_upload` creates an idempotent reservation.
2. `stage_upload` writes bytes to its private quarantine key.
3. `complete_upload` validates and promotes bytes, then returns an opaque `ContentId`.
4. `describe_content` and `read_content` require the exact tenant and active owner claim.
5. `claim_content_in_tx` and `release_claim_in_tx` compose claim changes with an owner Module transaction.
6. `sweep_terminal_quarantine` repeatedly deletes only exact quarantine keys whose terminal state and grace period are proven by PostgreSQL. Attempt and success timestamps are observations, never permanent reachability claims; every attempt rotates to the back of the bounded queue, so failures do not starve other keys and a delayed object-store write is removed by a later pass.

There is no generic product HTTP route. An owner Module may wrap this API after doing its own business authorization.

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
- Cross-Module claim transfer protocol.
- Retention policies and protected-content deletion.
- Malware scanning and additional media validators.
- Console surfaces, runtime scheduling, and public events.

Runtime scheduling remains deferred specifically because Lenso's public facade does not yet expose the behavior-binding types required by an external linked Module. Until that public seam exists, consumers invoke the bounded sweeper from a host-owned maintenance hook; the manifest does not declare behavior that cannot be registered.

These require real consumers and independently reviewed contracts before they enter the manifest.
