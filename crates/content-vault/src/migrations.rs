//! Immutable Content Vault schema plan.

use lenso_postgres_kit::{Migration, PlanError, SchemaPlan, sql_migrations};

pub const CONTENT_VAULT_SCHEMA: &str = "content_vault";

pub const CONTENT_VAULT_MIGRATIONS: &[Migration] = sql_migrations![
    (
        1,
        "create-content-vault-schema",
        "migrations/0001_create_content_vault_schema.sql"
    ),
    (2, "add-streaming-io", "migrations/0002_streaming_io.sql"),
];

pub(crate) fn schema_plan() -> Result<SchemaPlan, PlanError> {
    SchemaPlan::new(CONTENT_VAULT_SCHEMA, CONTENT_VAULT_MIGRATIONS)
}
