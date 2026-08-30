# Content Vault Plugin card

## Owner and deletion boundary

`lenso-content-vault-plugin` owns upload reservations, quarantine and staging
state, immutable protected bytes, content references, claims, integrity
observations, and the explicit terminal-quarantine sweep operation. Removing
the Plugin removes the `lenso.content-vault@1` provider and its PostgreSQL/S3
implementation. It does not add a Kernel branch, delete protected bytes, or
delete owner-domain records. Existing protected objects remain subject to a
separately reviewed retention protocol.

The owning business Plugin remains the final authority for its domain object.
Content Vault authorizes each owner-scoped call by matching the Kernel-resolved
caller Instance to `grant.owner.plugin_instance`. Only configured maintenance
callers may invoke `sweep`.

The old linked Rust API could share a `ContentVaultTransaction` with an owner
business write. That is deliberately not preserved across the Plugin boundary:
Capability `claim` and `release_claim` use Vault-owned transactions. Consumers
coordinate cross-Plugin outcomes with idempotency and compensation, or a future
explicit Workflow protocol; they must not assume database atomicity.

## Contract and implementation

- Plugin Release: `lenso.content-vault`, root Slot `content-vault`.
- Provided Capability: `lenso.content-vault@1`, descriptor `1.0.0`.
- Required Capability: exactly one `lenso.secrets@1` provider for the
  PostgreSQL URL and S3 credential references.
- Implementation: native Rust Plugin, PostgreSQL metadata plus S3-compatible
  quarantine/protected object areas.
- Configuration: database secret reference; explicit S3 bucket, region,
  optional endpoint/HTTP policy, and credential secret references; quarantine
  grace (at most 31,536,000 seconds, or one year); bounded sweep batch and
  stream channel capacity; and a maintenance
  caller allowlist (maximum 64). HTTP is accepted only for one explicit
  `http://` endpoint; credentials are non-empty resolved Secrets values.
  Production code performs no environment discovery.
- State: one fresh pool, object-store handles, validator, and stream task scope
  per prepared Plugin generation. Deactivation closes the pool and cancels all
  generation-owned streams.

## Observable behavior

The first observable workflow is reserve -> stream upload -> immutable commit
-> describe/download, with owner-scoped claims and integrity-checked download.
Stream frame `kind` values have Contract-enforced field combinations: chunks
carry offset plus bounded bytes only, while committed/descriptor frames carry
content only; the initial download descriptor has offset zero.
`sweep` is explicit; durable cadence belongs to a selected Jobs, Scheduler, or
Workflow Plugin and is not recreated as a hidden cron or volatile Kernel task.

## Operator boundary

Schema setup and upgrade are explicit operator workflows. Runtime activation
only connects to and validates an already-installed `content_vault` schema. The
historical SQL migrations are immutable. Neither startup nor Capability calls
apply schema changes.

Current and legacy verification compare a same-server session-local reference
catalog derived from the immutable SQL. Owner/ACL/default-ACL, columns,
constraints, indexes, types, comments, labels, and extra objects fail closed.
Legacy adoption additionally requires the exact old Host ledger rows, takes
table locks, and is an offline deployment protocol: every writer and
DDL-capable owner session must be stopped before and throughout the call.
Table locks stabilize existing objects but do not prevent a same-owner
concurrent `CREATE`; adoption does not claim atomicity against arbitrary DDL.
The transaction takes the shared database-wide `:lenso-maintenance` advisory
lock before its Content Vault-specific lock, coordinating with other compliant
Plugin operators.
Every later `connect`/`prepare` repeats the complete catalog proof and rejects
objects added after adoption. The temporary reference disappears with its
database connection.

The Rust crate does not publish the former engine/storage/transaction seam.
Only Plugin configuration/identity and the explicit operator are production
APIs; applications consume the generated Capability Client.

Release sequencing is explicit: the versioned
`lenso-capability-content-vault` package must exist in the selected registry
before a packaged Plugin can resolve that non-path dependency. This migration
worktree does not publish either package.
