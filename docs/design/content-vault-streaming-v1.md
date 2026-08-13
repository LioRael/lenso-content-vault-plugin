# Content Vault Streaming V1

## Decision

Streaming extends the existing Content Vault lifecycle; it is not a second
content identity system. The caller still reserves an expected size, SHA-256,
media type, tenant, and opaque owner. PostgreSQL remains authoritative for the
session, durable part offsets, writer lease, immutable content identity, owner
claim, integrity evidence, and exact cleanup keys.

The first revision supports UTF-8 `text/plain` and a fixed 8 MiB part size. Its
default reservation ceiling is 1 GiB. PNG/JPEG incremental validation, an HTTP
transport, and signed direct-upload tickets are outside this revision.

## Public seam

1. `reserve_streaming_upload` creates an idempotent owner-scoped reservation and
   returns a `StreamingUpload` handle.
2. `StreamingUpload::commit` consumes an `AsyncRead` beginning at
   `next_offset`. Each completed part is written to a unique attempt key before
   PostgreSQL advances the durable offset.
3. `resume_streaming_upload` reconstructs the handle and its next durable byte
   offset after interruption.
4. Validation rereads the immutable parts, incrementally verifies UTF-8, size,
   per-part digests, and the complete SHA-256, then promotes a bounded stream to
   the tenant-scoped protected key.
5. `fetch_verified` verifies each protected part before yielding it. A mismatch
   records durable integrity evidence and fails before the corrupt part crosses
   the API boundary.

The fixed part size is part of the protocol, not a tuning knob. Store streams
are split into chunks no larger than 8 MiB, and the production adapter keeps at
most one multipart chunk plus bounded verification state in flight.

## Recovery and fencing

Every writer claims a time-bounded token. PostgreSQL advances a part only when
the token, part index, byte offset, and session state still match. A stale
writer can finish an object-store request, but cannot advance or commit the
session. Its unique part key remains exact cleanup evidence.

An interrupted caller resumes only from the last durable part boundary. A
temporary database or object-store failure releases or eventually expires the
lease and leaves the session retryable. Only deterministic content failures
(invalid UTF-8, size mismatch, or digest mismatch) transition to `rejected`;
dependency unavailability never does.

## Object-store promotion

The generic object-store multipart API cannot make the initial upload
create-only. The adapter therefore writes to a unique internal attempt key,
completes it, then performs an atomic copy-if-absent to the immutable final key.
Identical existing content converges; different existing bytes fail closed.
S3 composition enables multipart copy-if-absent explicitly.

Attempt keys use the private `.content-vault-attempts` prefix. Exact cleanup is
issued after normal completion, but a process can stop after the remote write
and before cleanup. Production buckets must therefore apply a provider
lifecycle rule to expire that private prefix. The rule is a backstop for
unreachable transport attempts, not a substitute for PostgreSQL-authoritative
quarantine sweeping.

## Cleanup and retention

The bounded sweeper repeatedly selects terminal session keys and recorded part
keys after the configured grace period, deletes only those exact keys, and
rotates both successful and failed attempts fairly. Protected objects remain
immutable and are never deleted by this revision. Streaming completion and
cleanup therefore preserve the V1 retention boundary.

## Consumer readiness

A consumer must use an actual streaming transport and run PostgreSQL plus its
production-compatible object store acceptance before increasing an advertised
upload limit. Replacing a `Vec<u8>` or JSON request body with a larger numeric
limit is not a streaming migration.
