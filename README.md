# Lenso Content Vault Module

`lenso-module-content-vault` is a first-party linked Rust Module for accepting untrusted bytes and turning them into tenant-scoped, immutable content references.

It owns the durable lifecycle from upload reservation through quarantine validation and content-addressed commit. It deliberately does **not** own a consuming application's domain records, metadata, provenance, authorization policy, or domain routes.

## V1 contract

- Mandatory tenant and opaque owner identity on every operation.
- Reservation before staging.
- Recoverable staging leases prevent expiry cleanup from racing an in-flight object write.
- SHA-256, byte-size, media-type, and decode validation in quarantine.
- Immutable protected objects addressed by tenant-scoped content digest.
- Stable `ContentId` values and owner claims; storage keys are never public.
- Idempotent reserve and complete commands.
- Exact-key quarantine cleanup only after PostgreSQL proves a terminal state and a grace period has elapsed.
- No protected-object deletion in V1.
- No generic product HTTP surface. The consuming linked Module performs business authorization and owns its routes.

Supported by the built-in validator: `image/png`, `image/jpeg`, and UTF-8 `text/plain`. Hosts may inject another validator without changing the persistence contract.

V1 accepts complete byte buffers through its Rust API and defaults to a 64 MiB
maximum. It does not yet provide streaming or multipart upload, so consumers
with larger artifact contracts must keep their existing object path until a
streaming Vault revision is available.

See [the V1 design](docs/design/content-vault-v1.md) for the authority boundary and failure semantics.

## Transaction precondition

`ContentVault::begin_transaction` issues an opaque `ContentVaultTransaction` that lets claim changes commit atomically with the owner Module's business write. `claim_content_in_tx` and `release_claim_in_tx` reject transactions issued by another Vault before running SQL, so the same-host database boundary is enforced by the Rust API.

## Host wiring

```rust
use content_vault::module;
use lenso::host::HostBuilder;

let host = HostBuilder::new()
    .linked_module(module::linked_module())
    .build();
```

The owner Module constructs `ContentVault` with the host database pool and two object-store capabilities: quarantine storage may delete exact keys, while protected storage intentionally has no delete method.

```rust
use content_vault::{ContentVault, ContentVaultStores};
use lenso::host::http::AppContext;

fn vault(context: &AppContext) -> Result<ContentVault, content_vault::StoreError> {
    let stores = ContentVaultStores::from_s3_env()?;
    Ok(ContentVault::from_stores(context.db.clone(), &stores))
}
```

Enable the crate's `s3` feature and set `CONTENT_VAULT_S3_BUCKET`. Credentials, region, endpoint, and HTTP policy use the standard `AWS_*` variables understood by `object_store`; this also supports S3-compatible services such as MinIO. The factory never falls back to memory storage. It reserves disjoint `content-vault/quarantine` and `content-vault/protected` prefixes inside the configured bucket.

The current public Lenso facade cannot yet express a runtime-bearing linked Module without importing private `platform-*` crates. Accordingly, this Module truthfully remains `manifest_only`; the Host or owner Module must call `sweep_terminal_quarantine` from an explicit maintenance hook. Automatic schedule declaration is deferred until Lenso exposes its linked runtime authoring types publicly.

## Verification

```bash
cargo fmt --all --check
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
```

PostgreSQL black-box acceptance is explicit and cannot silently skip:

```bash
CONTENT_VAULT_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/content_vault_test \
  cargo test --locked --workspace --features postgres-acceptance
```

S3-compatible capability acceptance is also explicit and cannot silently skip:

```bash
CONTENT_VAULT_S3_BUCKET=content-vault-test \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_DEFAULT_REGION=us-east-1 \
AWS_ENDPOINT=http://127.0.0.1:9000 \
AWS_ALLOW_HTTP=true \
AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
  cargo test --locked --workspace --features s3-acceptance --test s3_acceptance
```

## Status

This repository is a local contribution candidate. It has not been published to crates.io, added to the official catalog, or pushed to a remote repository.
