# Lenso Content Vault Plugin

`lenso-content-vault-plugin` is a removable native Lenso Plugin that accepts
untrusted bytes and turns them into tenant-scoped immutable content references.
It provides the source-first `lenso.content-vault@1` Capability.

The Plugin owns upload reservations, quarantine and staging, immutable bytes,
claims, integrity observations, and a bounded maintenance operation. It does
not own consumer-domain records, routes, or authorization policy. Every
owner-scoped call must carry an opaque owner grant whose Plugin Instance is the
Kernel-resolved caller. Protected object keys never cross the Capability.

See [the Plugin card](docs/plugin-card.md),
[the V1 design](docs/design/content-vault-v1.md), and
[the streaming design](docs/design/content-vault-streaming-v1.md).

## Capability workflow

The observable workflow is:

1. `reserve` creates an owner-scoped upload reservation.
2. `upload` opens or resumes a bounded bidirectional Stream at `next_offset`.
3. Chunk frames are accepted in order. Consumer half-close ends the byte
   source without closing the provider receive direction.
4. Validation, quarantine promotion, and PostgreSQL commit produce one
   immutable content descriptor.
5. `describe`, `download`, `claim`, and `release_claim` remain owner scoped.

Downloads verify each persisted part before yielding it. Upload and download
messages use bounded channels and generation-owned tasks, so consumer
cancellation wakes blocked work. Each Stream closes its provider-send direction
and emits one terminal outcome.

Frame schemas are discriminated by `kind`: chunks require a non-null offset and
bounded Base64 bytes and cannot carry a descriptor; committed/descriptor frames
require a descriptor and cannot carry bytes. A download descriptor starts at
offset zero. These combinations are Contract validation rules, not conventions
left to one native provider.

`sweep` is an explicit Capability operation. Only exact Plugin Instances in
`maintenance_callers` may invoke it. Durable cadence belongs to an explicitly
selected Jobs, Scheduler, or Workflow Plugin; Content Vault does not register a
hidden cron or Kernel background loop.

## Configuration and authority

The Plugin requires exactly one `lenso.secrets@1` provider. App Composition
supplies these immutable configuration fields:

- `database_url_secret`
- `s3_bucket` and `s3_region`
- optional `s3_endpoint` and explicit `s3_allow_http`
- `s3_access_key_id_secret`, `s3_secret_access_key_secret`, and optional
  `s3_session_token_secret`
- `quarantine_grace_seconds` (0 through 31,536,000, at most one year),
  `sweep_batch_limit`, and
  `stream_channel_capacity`
- `maintenance_callers`

`s3_allow_http` is valid only with one explicit `http://` endpoint; it is
rejected when the endpoint is absent or HTTPS. Endpoint userinfo, query, and
fragment components are rejected so authority cannot be smuggled into Debug
configuration. Resolved access-key/secret values, and a configured session
token, must be non-empty. `maintenance_callers` is unique and capped at 64.
The package configuration schema deliberately stays within the current
App-plan JSON Schema subset. `ContentVaultPluginConfig::validate` remains the
fail-closed authority for string bounds, secret-reference syntax, numeric
ceilings, caller uniqueness, and the endpoint/HTTP relationship before the
Plugin starts.

The implementation never discovers production S3 settings or credentials from
process environment variables. PostgreSQL and S3 credential values are
resolved only through the bound Secrets Capability. Quarantine and protected
areas use disjoint prefixes inside the explicitly selected bucket. Quarantine
may delete exact keys; protected storage exposes no delete method.

## Operator boundary

Schema ownership is explicit:

```rust
use content_vault::ContentVaultOperator;

# async fn install(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
ContentVaultOperator::setup(database_url).await?;
ContentVaultOperator::upgrade(database_url).await?;
# Ok(())
# }
```

Plugin `prepare` resolves secrets, connects dependencies, and verifies that the
current historical schema is already installed. It never runs migrations.
`setup` and `upgrade` are operator workflows, and the SQL files under
`crates/content-vault/migrations` remain immutable.

Verification uses the checksum ledger plus a session-local `pg_temp` reference
built from those same immutable migrations on the same PostgreSQL server. It
compares the complete owned catalog, including owners, table/type/column ACLs,
default ACLs, defaults, constraints, indexes, comments, security labels, and
extra objects. The temporary reference disappears with the connection and is
not a durable schema mutation.

Exact legacy Host deployments are adopted explicitly with
`adopt_legacy_v1` or `adopt_legacy_current`. Adoption also verifies the old
`platform.schema_migrations` provenance, preserves rows, and atomically creates
the checksum ledger. Run `upgrade` after either adoption call to apply later
Plugin-owned migrations. Adoption is a mandatory offline maintenance operation:
stop all writers and every DDL-capable session for the database owner before the call.
Its `SHARE`/`ACCESS EXCLUSIVE` locks stabilize the legacy ledger and existing
tables, but PostgreSQL provides no ordinary schema lock that can prevent the
same owner from concurrently creating a new object. Runtime `prepare` never
adopts, and every `connect`/`prepare` performs a fresh full-catalog comparison;
an extra object created after adoption makes startup fail closed. Adoption
first takes the database-wide `:lenso-maintenance` transaction advisory lock,
then the Content Vault-specific lock, so cooperating Plugin operators compose.

The Plugin-first Capability intentionally breaks the old cross-module
transaction seam. `claim` and `release_claim` run in Content Vault-owned
transactions; a consuming Plugin cannot combine them atomically with its own
database write. Consumers must use idempotent commands plus compensation, or a
future explicitly selected Workflow protocol, when a business action spans
both Plugins. The lower-level `ContentVaultTransaction` is private production
code and is never re-exported, including when every Cargo feature is enabled.
Acceptance suites are compiled as crate-internal test modules rather than
publishable integration-test APIs. The crate's production surface is Plugin
configuration/identity plus the deployment operator; engine, storage,
migrations, and transaction types are not consumer APIs.

## Verification

Run repository checks with Cargo (workspace contributors may use their local shared-target
wrapper instead):

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

PostgreSQL acceptance is explicit and refuses destructive setup unless the
database name starts with `content_vault_test`:

```bash
CONTENT_VAULT_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/content_vault_test \
  cargo test \
  --workspace --features postgres-acceptance
```

S3-compatible acceptance may use environment variables as test harness input;
the test constructs the adapter explicitly. Production Plugin authority still
comes only from App configuration and Secrets:

```bash
CONTENT_VAULT_S3_BUCKET=content-vault-test \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ENDPOINT=http://127.0.0.1:9000 \
AWS_ALLOW_HTTP=true \
AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
cargo test \
  -p lenso-content-vault-plugin --features s3-acceptance --lib s3_acceptance::
```

## Status

This repository has not been published from this migration worktree.
