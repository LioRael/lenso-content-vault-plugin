//! Explicit, fail-closed schema ownership workflows for the Content Vault Plugin.

use crate::migrations::{CONTENT_VAULT_MIGRATIONS, CONTENT_VAULT_SCHEMA, schema_plan};
use lenso_postgres_kit::{
    OwnedPostgres, PostgresKitError, SchemaOperator, SetupOutcome, UpgradeOutcome,
};
use sha2::{Digest as _, Sha256};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, postgres::PgPoolOptions};

const LEGACY_V1_HOST_MIGRATIONS: &[&str] = &["content-vault/0001_create_content_vault_schema"];
const LEGACY_CURRENT_HOST_MIGRATIONS: &[&str] = &[
    "content-vault/0001_create_content_vault_schema",
    "content-vault/0002_streaming_io",
];
const LEGACY_SCHEMA_STATEMENT: &str = "CREATE SCHEMA IF NOT EXISTS content_vault;";
const ADOPTION_ADVISORY_LOCKS: [&str; 2] = [
    "SELECT pg_advisory_xact_lock(\
       hashtextextended(current_database() || ':lenso-maintenance', 0)\
     )",
    "SELECT pg_advisory_xact_lock(\
       hashtextextended(current_database() || ':content_vault', 0)\
     )",
];
const SCHEMA_UNSUPPORTED_OBJECT_COUNT_SQL: &str = r"
    SELECT
      (SELECT count(*) FROM pg_proc WHERE pronamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_collation WHERE collnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_constraint
         WHERE connamespace = namespaces.oid AND conrelid = 0)
    + (SELECT count(*) FROM pg_conversion WHERE connamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_operator WHERE oprnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_opclass WHERE opcnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_opfamily WHERE opfnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_statistic_ext WHERE stxnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_extension WHERE extnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_ts_config WHERE cfgnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_ts_dict WHERE dictnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_ts_parser WHERE prsnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_ts_template WHERE tmplnamespace = namespaces.oid)
    + (SELECT count(*) FROM pg_type AS types
         WHERE types.typnamespace = namespaces.oid
           AND obj_description(types.oid, 'pg_type') IS NOT NULL)
    + (SELECT count(*) FROM pg_seclabel AS labels
         WHERE (labels.classoid = 'pg_namespace'::regclass
                  AND labels.objoid = namespaces.oid)
            OR (labels.classoid = 'pg_class'::regclass
                  AND labels.objoid IN (
                    SELECT relations.oid FROM pg_class AS relations
                    WHERE relations.relnamespace = namespaces.oid
                  ))
            OR (labels.classoid = 'pg_constraint'::regclass
                  AND labels.objoid IN (
                    SELECT constraints.oid FROM pg_constraint AS constraints
                    WHERE constraints.connamespace = namespaces.oid
                  ))
            OR (labels.classoid = 'pg_type'::regclass
                  AND labels.objoid IN (
                    SELECT types.oid FROM pg_type AS types
                    WHERE types.typnamespace = namespaces.oid
                  )))
    FROM pg_namespace AS namespaces
    WHERE namespaces.nspname = $1
";
const UNSAFE_PUBLICATION_EXPOSURE_SQL: &str = r"
    SELECT EXISTS (
      SELECT 1 FROM pg_publication AS publications
      WHERE publications.puballtables
      UNION ALL
      SELECT 1 FROM pg_publication_namespace AS entries
      JOIN pg_namespace AS namespaces ON namespaces.oid = entries.pnnspid
      WHERE namespaces.nspname = $1
      UNION ALL
      SELECT 1 FROM pg_publication_rel AS entries
      JOIN pg_class AS relations ON relations.oid = entries.prrelid
      JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
      WHERE namespaces.nspname = $1
    )
";
const LEGACY_LEDGER_UNSAFE_METADATA_SQL: &str = r"
    SELECT EXISTS (
      SELECT 1 FROM pg_trigger AS triggers
      WHERE triggers.tgrelid = 'platform.schema_migrations'::regclass
        AND NOT triggers.tgisinternal
      UNION ALL
      SELECT 1 FROM pg_policy AS policies
      WHERE policies.polrelid = 'platform.schema_migrations'::regclass
      UNION ALL
      SELECT 1 FROM pg_rewrite AS rules
      WHERE rules.ev_class = 'platform.schema_migrations'::regclass
        AND rules.rulename <> '_RETURN'
      UNION ALL
      SELECT 1 FROM pg_inherits AS inheritance
      WHERE inheritance.inhrelid = 'platform.schema_migrations'::regclass
         OR inheritance.inhparent = 'platform.schema_migrations'::regclass
      UNION ALL
      SELECT 1 FROM pg_statistic_ext AS statistics
      WHERE statistics.stxrelid = 'platform.schema_migrations'::regclass
      UNION ALL
      SELECT 1 FROM pg_description AS descriptions
      WHERE (descriptions.classoid = 'pg_namespace'::regclass
               AND descriptions.objoid = (
                 SELECT oid FROM pg_namespace WHERE nspname = 'platform'
               ))
         OR (descriptions.classoid = 'pg_class'::regclass
               AND descriptions.objoid = 'platform.schema_migrations'::regclass)
         OR (descriptions.classoid = 'pg_constraint'::regclass
               AND descriptions.objoid IN (
                 SELECT constraints.oid FROM pg_constraint AS constraints
                 WHERE constraints.conrelid = 'platform.schema_migrations'::regclass
               ))
      UNION ALL
      SELECT 1 FROM pg_seclabel AS labels
      WHERE (labels.classoid = 'pg_namespace'::regclass
               AND labels.objoid = (
                 SELECT oid FROM pg_namespace WHERE nspname = 'platform'
               ))
         OR (labels.classoid = 'pg_class'::regclass
               AND labels.objoid = 'platform.schema_migrations'::regclass)
         OR (labels.classoid = 'pg_constraint'::regclass
               AND labels.objoid IN (
                 SELECT constraints.oid FROM pg_constraint AS constraints
                 WHERE constraints.conrelid = 'platform.schema_migrations'::regclass
               ))
    )
";

/// One explicit legacy adoption result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyAdoptionOutcome {
    pub version: u64,
}

/// Explicit setup/upgrade surface owned by deployment operators.
///
/// Plugin preparation calls only [`Self::connect`]. It never applies or adopts migrations.
#[derive(Clone, Debug)]
pub struct ContentVaultOperator {
    postgres: OwnedPostgres,
}

impl ContentVaultOperator {
    /// Connects to the exact current managed schema without durable mutation.
    ///
    /// In addition to checking the checksum ledger, this builds a session-local reference
    /// catalog from the immutable migrations in `pg_temp` and compares all owned objects. The
    /// reference disappears with the verification connection.
    pub async fn connect(database_url: &str) -> Result<Self, ContentVaultOperatorError> {
        let postgres = OwnedPostgres::prepare_with_pool_options(
            database_url,
            schema_plan()?,
            PgPoolOptions::new().max_connections(10),
        )
        .await?;
        if let Err(error) = verify_managed_catalog(postgres.pool(), SchemaVersion::V3).await {
            postgres.pool().close().await;
            return Err(error);
        }
        Ok(Self { postgres })
    }

    /// Creates a fresh managed schema and applies the full immutable plan.
    pub async fn setup(database_url: &str) -> Result<SetupOutcome, ContentVaultOperatorError> {
        let outcome = SchemaOperator::connect(database_url, schema_plan()?)
            .await?
            .setup()
            .await?;
        Self::connect(database_url).await?.close().await;
        Ok(outcome)
    }

    /// Applies pending migrations only after the managed current/V1/V2 catalog is exact.
    pub async fn upgrade(database_url: &str) -> Result<UpgradeOutcome, ContentVaultOperatorError> {
        match OwnedPostgres::prepare(database_url, schema_plan()?).await {
            Ok(postgres) => {
                verify_managed_catalog(postgres.pool(), SchemaVersion::V3).await?;
                postgres.pool().close().await;
                return Ok(UpgradeOutcome::AlreadyCurrent { version: 3 });
            }
            Err(PostgresKitError::UpgradeRequired {
                current: 1,
                expected: 3,
                ..
            }) => {
                let pool = connect_unchecked(database_url).await?;
                let verification = verify_managed_catalog(&pool, SchemaVersion::V1).await;
                pool.close().await;
                verification?;
            }
            Err(PostgresKitError::UpgradeRequired {
                current: 2,
                expected: 3,
                ..
            }) => {
                let pool = connect_unchecked(database_url).await?;
                let verification = verify_managed_catalog(&pool, SchemaVersion::V2).await;
                pool.close().await;
                verification?;
            }
            Err(error) => return Err(error.into()),
        }

        let outcome = SchemaOperator::connect(database_url, schema_plan()?)
            .await?
            .upgrade()
            .await?;
        Self::connect(database_url).await?.close().await;
        Ok(outcome)
    }

    /// One-time adoption of an exact legacy V1 schema and matching Host migration provenance.
    ///
    /// This preserves all rows, atomically creates the Plugin-owned checksum ledger, and leaves
    /// the schema at V1. The operator must then call [`Self::upgrade`] explicitly.
    ///
    /// This is an offline deployment operation: every writer and every DDL-capable session owned
    /// by the database role must be stopped for the whole call. `PostgreSQL` table locks stabilize
    /// existing objects, but cannot prevent that same role from concurrently creating a new
    /// schema object.
    pub async fn adopt_legacy_v1(
        database_url: &str,
    ) -> Result<LegacyAdoptionOutcome, ContentVaultOperatorError> {
        adopt_legacy(database_url, SchemaVersion::V1).await
    }

    /// One-time adoption of the exact legacy current (V2) schema and Host provenance.
    ///
    /// This preserves all rows and atomically records both historical migrations in the
    /// Plugin-owned checksum ledger. The operator must then call [`Self::upgrade`] explicitly
    /// to apply Plugin-owned migrations. Adoption has the same mandatory offline DDL boundary as
    /// [`Self::adopt_legacy_v1`].
    pub async fn adopt_legacy_current(
        database_url: &str,
    ) -> Result<LegacyAdoptionOutcome, ContentVaultOperatorError> {
        adopt_legacy(database_url, SchemaVersion::V2).await
    }

    /// Returns the exact managed pool used by one Plugin generation.
    pub(crate) fn pool(&self) -> &PgPool {
        self.postgres.pool()
    }

    /// Closes this generation's pool.
    pub async fn close(self) {
        self.postgres.pool().close().await;
    }
}

/// Failure from an explicit Content Vault schema workflow.
#[derive(Debug, thiserror::Error)]
pub enum ContentVaultOperatorError {
    #[error(transparent)]
    Plan(#[from] lenso_postgres_kit::PlanError),
    #[error(transparent)]
    Postgres(#[from] PostgresKitError),
    #[error("Content Vault schema catalog does not match its immutable migration history")]
    ManagedSchemaMismatch,
    #[error("legacy Content Vault schema does not exist")]
    LegacySchemaMissing,
    #[error("legacy Content Vault schema is already managed; use setup or upgrade")]
    LegacySchemaAlreadyManaged,
    #[error("legacy Content Vault schema does not match the exact V{version} catalog")]
    LegacySchemaMismatch { version: u64 },
    #[error("legacy Host migration provenance does not match Content Vault V{version}")]
    LegacyHostHistoryMismatch { version: u64 },
    #[error("legacy Content Vault schema is owned by `{owner}`, not current role `{current_role}`")]
    LegacyOwnershipMismatch { owner: String, current_role: String },
    #[error("Content Vault schema catalog inspection failed")]
    Database(#[from] sqlx::Error),
}

#[derive(Clone, Copy)]
enum SchemaVersion {
    V1,
    V2,
    V3,
}

impl SchemaVersion {
    const fn number(self) -> u64 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
            Self::V3 => 3,
        }
    }

    const fn legacy_host_migrations(self) -> Option<&'static [&'static str]> {
        match self {
            Self::V1 => Some(LEGACY_V1_HOST_MIGRATIONS),
            Self::V2 => Some(LEGACY_CURRENT_HOST_MIGRATIONS),
            Self::V3 => None,
        }
    }
}

async fn connect_unchecked(database_url: &str) -> Result<PgPool, ContentVaultOperatorError> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .map_err(ContentVaultOperatorError::Database)
}

async fn adopt_legacy(
    database_url: &str,
    version: SchemaVersion,
) -> Result<LegacyAdoptionOutcome, ContentVaultOperatorError> {
    let pool = connect_unchecked(database_url).await?;
    let mut transaction = pool.begin().await?;
    // The shared database-wide maintenance key composes adoption with every Plugin operator;
    // the schema-specific key then serializes Content Vault adoption calls.
    for lock_sql in ADOPTION_ADVISORY_LOCKS {
        sqlx::query(lock_sql).execute(transaction.as_mut()).await?;
    }

    verify_legacy_owner(transaction.as_mut(), version).await?;
    if managed_ledger_exists(transaction.as_mut()).await? {
        return Err(ContentVaultOperatorError::LegacySchemaAlreadyManaged);
    }
    if !legacy_host_history_matches(transaction.as_mut(), version).await? {
        return Err(ContentVaultOperatorError::LegacyHostHistoryMismatch {
            version: version.number(),
        });
    }
    if !catalog_matches_reference(transaction.as_mut(), version, false).await? {
        return Err(ContentVaultOperatorError::LegacySchemaMismatch {
            version: version.number(),
        });
    }
    lock_legacy_catalog(transaction.as_mut(), version).await?;
    // Re-read every proof under the table locks. The advisory lock coordinates operator calls,
    // while the PostgreSQL locks stabilize the legacy ledger and existing data tables. They do
    // not lock the schema against same-owner CREATE; the public operator contract therefore
    // requires an offline maintenance window with every DDL-capable session stopped.
    verify_legacy_owner(transaction.as_mut(), version).await?;
    if managed_ledger_exists(transaction.as_mut()).await? {
        return Err(ContentVaultOperatorError::LegacySchemaAlreadyManaged);
    }
    if !legacy_host_history_matches(transaction.as_mut(), version).await? {
        return Err(ContentVaultOperatorError::LegacyHostHistoryMismatch {
            version: version.number(),
        });
    }
    if !catalog_matches_installed_reference(transaction.as_mut()).await? {
        return Err(ContentVaultOperatorError::LegacySchemaMismatch {
            version: version.number(),
        });
    }
    create_managed_ledger(transaction.as_mut(), version).await?;
    install_reference_ledger(transaction.as_mut()).await?;
    if !catalog_matches_installed_reference(transaction.as_mut()).await? {
        return Err(ContentVaultOperatorError::LegacySchemaMismatch {
            version: version.number(),
        });
    }
    transaction.commit().await?;
    pool.close().await;
    Ok(LegacyAdoptionOutcome {
        version: version.number(),
    })
}

async fn lock_legacy_catalog(
    connection: &mut PgConnection,
    version: SchemaVersion,
) -> Result<(), sqlx::Error> {
    sqlx::raw_sql("LOCK TABLE platform.schema_migrations IN SHARE MODE")
        .execute(&mut *connection)
        .await?;
    let content_tables = match version {
        SchemaVersion::V1 => {
            "LOCK TABLE content_vault.upload_sessions, content_vault.blobs, \
             content_vault.content_objects, content_vault.content_claims, \
             content_vault.command_receipts, content_vault.integrity_observations \
             IN ACCESS EXCLUSIVE MODE"
        }
        SchemaVersion::V2 | SchemaVersion::V3 => {
            "LOCK TABLE content_vault.upload_sessions, content_vault.blobs, \
             content_vault.content_objects, content_vault.content_claims, \
             content_vault.command_receipts, content_vault.integrity_observations, \
             content_vault.upload_parts, content_vault.blob_parts \
             IN ACCESS EXCLUSIVE MODE"
        }
    };
    sqlx::raw_sql(content_tables)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn verify_managed_catalog(
    pool: &PgPool,
    version: SchemaVersion,
) -> Result<(), ContentVaultOperatorError> {
    let mut connection = pool.acquire().await?;
    if !catalog_matches_reference(&mut connection, version, true).await? {
        return Err(ContentVaultOperatorError::ManagedSchemaMismatch);
    }
    Ok(())
}

async fn verify_legacy_owner(
    connection: &mut PgConnection,
    version: SchemaVersion,
) -> Result<(), ContentVaultOperatorError> {
    let authority: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT pg_get_userbyid(namespaces.nspowner)::text, namespaces.nspacl::text \
         FROM pg_namespace AS namespaces \
         WHERE namespaces.nspname = 'content_vault'",
    )
    .fetch_optional(&mut *connection)
    .await?;
    let Some((owner, acl)) = authority else {
        return Err(ContentVaultOperatorError::LegacySchemaMissing);
    };
    let current_role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut *connection)
        .await?;
    if owner != current_role {
        return Err(ContentVaultOperatorError::LegacyOwnershipMismatch {
            owner,
            current_role,
        });
    }
    if acl.is_some() {
        return Err(ContentVaultOperatorError::LegacySchemaMismatch {
            version: version.number(),
        });
    }
    Ok(())
}

async fn managed_ledger_exists(connection: &mut PgConnection) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (\
           SELECT 1 FROM pg_class AS relations \
           JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace \
           WHERE namespaces.nspname = 'content_vault' \
             AND relations.relname = '_lenso_schema_migrations' \
             AND relations.relkind = 'r'\
         )",
    )
    .fetch_one(&mut *connection)
    .await
}

#[allow(clippy::too_many_lines)] // One contiguous, reviewable exact legacy catalog proof.
async fn legacy_host_history_matches(
    connection: &mut PgConnection,
    version: SchemaVersion,
) -> Result<bool, sqlx::Error> {
    if unsafe_default_acls_exist(connection, "platform", false).await?
        || unsafe_publication_exposure_exists(connection, "platform").await?
    {
        return Ok(false);
    }
    let current_role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(&mut *connection)
        .await?;
    let relation = sqlx::query_scalar::<_, sqlx::types::Json<serde_json::Value>>(
        r"
        SELECT jsonb_build_array(
               relations.relkind::text,
               pg_get_userbyid(namespaces.nspowner)::text,
               pg_get_userbyid(relations.relowner)::text,
               namespaces.nspacl::text,
               relations.relacl::text,
               relations.relpersistence::text,
               relations.relrowsecurity,
               relations.relforcerowsecurity,
               relations.relreplident::text,
               COALESCE(access_methods.amname::text, ''),
               COALESCE((
                 SELECT jsonb_agg(option ORDER BY option)
                 FROM unnest(relations.reloptions) AS option
               ), '[]'::jsonb),
               relations.relispartition,
               COALESCE(tablespaces.spcname::text, ''),
               COALESCE(obj_description(namespaces.oid, 'pg_namespace'), ''),
               COALESCE(obj_description(relations.oid, 'pg_class'), ''))
        FROM pg_class AS relations
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        LEFT JOIN pg_am AS access_methods ON access_methods.oid = relations.relam
        LEFT JOIN pg_tablespace AS tablespaces ON tablespaces.oid = relations.reltablespace
        WHERE namespaces.nspname = 'platform'
          AND relations.relname = 'schema_migrations'
        ",
    )
    .fetch_optional(&mut *connection)
    .await?;
    if relation
        != Some(sqlx::types::Json(serde_json::json!([
            "r".to_owned(),
            current_role.clone(),
            current_role.clone(),
            null,
            null,
            "p".to_owned(),
            false,
            false,
            "d".to_owned(),
            "heap".to_owned(),
            [],
            false,
            String::new(),
            String::new(),
            String::new(),
        ])))
    {
        return Ok(false);
    }

    let columns = sqlx::query_scalar::<_, sqlx::types::Json<serde_json::Value>>(
        r"
        SELECT jsonb_build_array(
               attributes.attnum,
               attributes.attname::text,
               format_type(attributes.atttypid, attributes.atttypmod)::text,
               attributes.attnotnull,
               COALESCE(pg_get_expr(defaults.adbin, defaults.adrelid, false), ''),
               attributes.attidentity::text,
               attributes.attgenerated::text,
               attributes.attndims,
               attributes.attislocal,
               attributes.attinhcount,
               attributes.atthasmissing,
               COALESCE(attributes.attmissingval::text, ''),
               attributes.attstorage::text,
               attributes.attcompression::text,
               COALESCE(attributes.attstattarget, -1),
               COALESCE((
                 SELECT jsonb_agg(option ORDER BY option)
                 FROM unnest(attributes.attoptions) AS option
               ), '[]'::jsonb),
               COALESCE((
                 SELECT jsonb_agg(option ORDER BY option)
                 FROM unnest(attributes.attfdwoptions) AS option
               ), '[]'::jsonb),
               COALESCE(collations.collname::text, ''),
               COALESCE(attributes.attacl::text, ''),
               COALESCE(col_description(relations.oid, attributes.attnum), ''))
        FROM pg_attribute AS attributes
        JOIN pg_class AS relations ON relations.oid = attributes.attrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        LEFT JOIN pg_attrdef AS defaults
          ON defaults.adrelid = attributes.attrelid AND defaults.adnum = attributes.attnum
        LEFT JOIN pg_collation AS collations ON collations.oid = attributes.attcollation
        WHERE namespaces.nspname = 'platform'
          AND relations.relname = 'schema_migrations'
          AND attributes.attnum > 0 AND NOT attributes.attisdropped
        ORDER BY attributes.attnum
        ",
    )
    .fetch_all(&mut *connection)
    .await?
    .into_iter()
    .map(|value| value.0)
    .collect::<Vec<_>>();
    if columns
        != vec![
            serde_json::json!([
                1,
                "name",
                "text",
                true,
                "",
                "",
                "",
                0,
                true,
                0,
                false,
                "",
                "x",
                "",
                -1,
                [],
                [],
                "default",
                "",
                ""
            ]),
            serde_json::json!([
                2,
                "applied_at",
                "timestamp with time zone",
                true,
                "now()",
                "",
                "",
                0,
                true,
                0,
                false,
                "",
                "p",
                "",
                -1,
                [],
                [],
                "",
                "",
                ""
            ]),
        ]
    {
        return Ok(false);
    }

    let constraints =
        sqlx::query_as::<_, (String, String, bool, bool, bool, bool, String, String)>(
            r"
        SELECT constraints.conname::text,
               constraints.contype::text,
               constraints.condeferrable,
               constraints.condeferred,
               constraints.convalidated,
               constraints.connoinherit,
               pg_get_constraintdef(constraints.oid, false)::text,
               COALESCE(obj_description(constraints.oid, 'pg_constraint'), '')
        FROM pg_constraint AS constraints
        JOIN pg_class AS relations ON relations.oid = constraints.conrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        WHERE namespaces.nspname = 'platform'
          AND relations.relname = 'schema_migrations'
          AND constraints.contype <> 'n'
        ORDER BY constraints.conname
        ",
        )
        .fetch_all(&mut *connection)
        .await?;
    if constraints
        != vec![(
            "schema_migrations_pkey".to_owned(),
            "p".to_owned(),
            false,
            false,
            true,
            true,
            "PRIMARY KEY (name)".to_owned(),
            String::new(),
        )]
    {
        return Ok(false);
    }

    let indexes = sqlx::query_scalar::<_, sqlx::types::Json<serde_json::Value>>(
        r"
        SELECT jsonb_build_array(
               indexes.relname::text,
               pg_get_userbyid(indexes.relowner)::text,
               COALESCE(indexes.relacl::text, ''),
               access_methods.amname::text,
               catalog.indisunique,
               catalog.indisprimary,
               catalog.indisexclusion,
               catalog.indimmediate,
               catalog.indisvalid,
               catalog.indisready,
               catalog.indislive,
               catalog.indisreplident,
               catalog.indcheckxmin,
               indexes.relpersistence::text,
               COALESCE((
                 SELECT jsonb_agg(option ORDER BY option)
                 FROM unnest(indexes.reloptions) AS option
               ), '[]'::jsonb),
               COALESCE(tablespaces.spcname::text, ''),
               pg_get_indexdef(indexes.oid, 0, false)::text,
               COALESCE(obj_description(indexes.oid, 'pg_class'), ''),
               COALESCE((SELECT jsonb_agg(jsonb_build_array(labels.provider, labels.label)
                                          ORDER BY labels.provider)
                         FROM pg_seclabel AS labels
                         WHERE labels.classoid = 'pg_class'::regclass
                           AND labels.objoid = indexes.oid AND labels.objsubid = 0), '[]'::jsonb))
        FROM pg_index AS catalog
        JOIN pg_class AS indexes ON indexes.oid = catalog.indexrelid
        JOIN pg_class AS relations ON relations.oid = catalog.indrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        LEFT JOIN pg_am AS access_methods ON access_methods.oid = indexes.relam
        LEFT JOIN pg_tablespace AS tablespaces ON tablespaces.oid = indexes.reltablespace
        WHERE namespaces.nspname = 'platform'
          AND relations.relname = 'schema_migrations'
        ORDER BY indexes.relname
        ",
    )
    .fetch_all(&mut *connection)
    .await?
    .into_iter()
    .map(|value| value.0)
    .collect::<Vec<_>>();
    if indexes
        != vec![serde_json::json!([
            "schema_migrations_pkey",
            current_role,
            "",
            "btree",
            true,
            true,
            false,
            true,
            true,
            true,
            true,
            false,
            false,
            "p",
            [],
            "",
            "CREATE UNIQUE INDEX schema_migrations_pkey ON platform.schema_migrations USING btree (name)",
            "",
            []
        ])]
    {
        return Ok(false);
    }

    let types = sqlx::query_scalar::<_, serde_json::Value>(
        r"
        SELECT jsonb_build_array(types.typname, types.typtype::text,
          pg_get_userbyid(types.typowner), COALESCE(types.typacl::text, ''),
          types.typcategory::text, types.typispreferred, types.typnotnull,
          COALESCE(base_types.typname, ''), COALESCE(element_types.typname, ''),
          COALESCE(relations.relname, ''), COALESCE(array_types.typname, ''),
          COALESCE(collations.collname, ''), COALESCE(types.typdefault, ''),
          COALESCE(obj_description(types.oid, 'pg_type'), ''),
          COALESCE((SELECT jsonb_agg(jsonb_build_array(labels.provider, labels.label)
                                     ORDER BY labels.provider)
                    FROM pg_seclabel AS labels
                    WHERE labels.objoid = types.oid AND labels.objsubid = 0), '[]'::jsonb))
        FROM pg_type AS types
        JOIN pg_namespace AS namespaces ON namespaces.oid = types.typnamespace
        LEFT JOIN pg_type AS base_types ON base_types.oid = types.typbasetype
        LEFT JOIN pg_type AS element_types ON element_types.oid = types.typelem
        LEFT JOIN pg_class AS relations ON relations.oid = types.typrelid
        LEFT JOIN pg_type AS array_types ON array_types.oid = types.typarray
        LEFT JOIN pg_collation AS collations ON collations.oid = types.typcollation
        WHERE namespaces.nspname = 'platform'
          AND types.typname IN ('schema_migrations', '_schema_migrations')
        ORDER BY types.typname
        ",
    )
    .fetch_all(&mut *connection)
    .await?;
    if types
        != vec![
            serde_json::json!([
                "_schema_migrations",
                "b",
                current_role,
                "",
                "A",
                false,
                false,
                "",
                "schema_migrations",
                "",
                "",
                "",
                "",
                "",
                []
            ]),
            serde_json::json!([
                "schema_migrations",
                "c",
                current_role,
                "",
                "C",
                false,
                false,
                "",
                "",
                "schema_migrations",
                "_schema_migrations",
                "",
                "",
                "",
                []
            ]),
        ]
    {
        return Ok(false);
    }

    let unsafe_metadata: bool = sqlx::query_scalar(LEGACY_LEDGER_UNSAFE_METADATA_SQL)
        .fetch_one(&mut *connection)
        .await?;
    if unsafe_metadata {
        return Ok(false);
    }

    let names = sqlx::query_scalar::<_, String>(
        "SELECT name::text FROM platform.schema_migrations \
         WHERE name LIKE 'content-vault/%' ORDER BY name",
    )
    .fetch_all(&mut *connection)
    .await?;
    let expected_names = version
        .legacy_host_migrations()
        .expect("legacy adoption only accepts historical Host schema versions")
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    Ok(names == expected_names)
}

async fn create_managed_ledger(
    connection: &mut PgConnection,
    version: SchemaVersion,
) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "CREATE TABLE content_vault._lenso_schema_migrations (\
           version bigint PRIMARY KEY CHECK (version > 0),\
           name text NOT NULL,\
           checksum text NOT NULL,\
           applied_at timestamptz NOT NULL DEFAULT transaction_timestamp()\
         )",
    )
    .execute(&mut *connection)
    .await?;
    for migration in CONTENT_VAULT_MIGRATIONS
        .iter()
        .take(usize::try_from(version.number()).expect("schema version fits usize"))
    {
        sqlx::query(
            "INSERT INTO content_vault._lenso_schema_migrations \
             (version, name, checksum) VALUES ($1, $2, $3)",
        )
        .bind(i64::try_from(migration.version()).expect("migration version fits bigint"))
        .bind(migration.name())
        .bind(migration_checksum(migration))
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

async fn catalog_matches_reference(
    connection: &mut PgConnection,
    version: SchemaVersion,
    managed: bool,
) -> Result<bool, sqlx::Error> {
    if unsafe_default_acls_exist(connection, CONTENT_VAULT_SCHEMA, true).await?
        || unsafe_publication_exposure_exists(connection, CONTENT_VAULT_SCHEMA).await?
        || schema_unsupported_object_count(connection, CONTENT_VAULT_SCHEMA).await? != 0
    {
        return Ok(false);
    }
    let actual = catalog_fingerprint(connection, CONTENT_VAULT_SCHEMA).await?;
    install_reference_schema(connection, version, managed).await?;
    catalog_matches_installed_reference_with_actual(connection, actual).await
}

async fn catalog_matches_installed_reference(
    connection: &mut PgConnection,
) -> Result<bool, sqlx::Error> {
    if unsafe_default_acls_exist(connection, CONTENT_VAULT_SCHEMA, true).await?
        || unsafe_publication_exposure_exists(connection, CONTENT_VAULT_SCHEMA).await?
        || schema_unsupported_object_count(connection, CONTENT_VAULT_SCHEMA).await? != 0
    {
        return Ok(false);
    }
    let actual = catalog_fingerprint(connection, CONTENT_VAULT_SCHEMA).await?;
    catalog_matches_installed_reference_with_actual(connection, actual).await
}

async fn catalog_matches_installed_reference_with_actual(
    connection: &mut PgConnection,
    actual: CatalogFingerprint,
) -> Result<bool, sqlx::Error> {
    let reference_schema: String = sqlx::query_scalar(
        "SELECT namespaces.nspname::text FROM pg_namespace AS namespaces \
         WHERE namespaces.oid = pg_my_temp_schema()",
    )
    .fetch_one(&mut *connection)
    .await?;
    let expected = catalog_fingerprint(connection, &reference_schema).await?;
    Ok(actual.normalized(CONTENT_VAULT_SCHEMA) == expected.normalized(&reference_schema))
}

async fn unsafe_default_acls_exist(
    connection: &mut PgConnection,
    schema: &str,
    include_global: bool,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        r"
        SELECT EXISTS (
          SELECT 1 FROM pg_default_acl AS defaults
          JOIN pg_roles AS roles ON roles.oid = defaults.defaclrole
          WHERE roles.rolname = current_user
            AND (($2 AND defaults.defaclnamespace = 0) OR defaults.defaclnamespace = (
              SELECT namespaces.oid FROM pg_namespace AS namespaces
              WHERE namespaces.nspname = $1
            ))
        )
        ",
    )
    .bind(schema)
    .bind(include_global)
    .fetch_one(&mut *connection)
    .await
}

async fn unsafe_publication_exposure_exists(
    connection: &mut PgConnection,
    schema: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(UNSAFE_PUBLICATION_EXPOSURE_SQL)
        .bind(schema)
        .fetch_one(&mut *connection)
        .await
}

async fn schema_unsupported_object_count(
    connection: &mut PgConnection,
    schema: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(SCHEMA_UNSUPPORTED_OBJECT_COUNT_SQL)
        .bind(schema)
        .fetch_one(&mut *connection)
        .await
}

async fn install_reference_schema(
    connection: &mut PgConnection,
    version: SchemaVersion,
    managed: bool,
) -> Result<(), sqlx::Error> {
    for migration in CONTENT_VAULT_MIGRATIONS
        .iter()
        .take(usize::try_from(version.number()).expect("schema version fits usize"))
    {
        let body = if migration.version() == 1 {
            migration
                .sql()
                .strip_prefix(LEGACY_SCHEMA_STATEMENT)
                .expect("immutable V1 migration starts with the audited schema statement")
        } else {
            migration.sql()
        };
        let reference_sql = body.replace("content_vault.", "pg_temp.");
        // The statement is derived only from immutable, compiled-in SQL and a fixed qualifier.
        sqlx::raw_sql(AssertSqlSafe(reference_sql))
            .execute(&mut *connection)
            .await?;
    }
    if managed {
        install_reference_ledger(connection).await?;
    }
    Ok(())
}

async fn install_reference_ledger(connection: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "CREATE TABLE pg_temp._lenso_schema_migrations (\
           version bigint PRIMARY KEY CHECK (version > 0),\
           name text NOT NULL,\
           checksum text NOT NULL,\
           applied_at timestamptz NOT NULL DEFAULT transaction_timestamp()\
         )",
    )
    .execute(&mut *connection)
    .await?;
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct CatalogFingerprint {
    schema: String,
    relations: Vec<String>,
    columns: Vec<String>,
    constraints: Vec<String>,
    indexes: Vec<String>,
    triggers: Vec<String>,
    policies: Vec<String>,
    routines: Vec<String>,
    types: Vec<String>,
    inheritance: Vec<String>,
    rules: Vec<String>,
    statistics: Vec<String>,
    publications: Vec<String>,
    default_acls: Vec<String>,
}

impl CatalogFingerprint {
    fn normalized(mut self, schema: &str) -> Self {
        normalize_catalog_sql_fields(&mut self.columns, &[3, 5], schema);
        normalize_catalog_sql_fields(&mut self.constraints, &[7], schema);
        normalize_catalog_sql_fields(&mut self.indexes, &[14], schema);
        normalize_catalog_sql_fields(&mut self.triggers, &[3], schema);
        normalize_catalog_sql_fields(&mut self.policies, &[4, 5], schema);
        normalize_catalog_sql_fields(&mut self.routines, &[1, 2, 10], schema);
        normalize_catalog_sql_fields(&mut self.types, &[12], schema);
        normalize_catalog_sql_fields(&mut self.rules, &[3], schema);
        normalize_catalog_sql_fields(&mut self.publications, &[2], schema);
        self
    }
}

fn normalize_catalog_sql_fields(rows: &mut [String], fields: &[usize], schema: &str) {
    for row in rows {
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(row) else {
            continue;
        };
        let Some(values) = value.as_array_mut() else {
            continue;
        };
        for field in fields {
            let Some(value) = values.get_mut(*field) else {
                continue;
            };
            let Some(sql) = value.as_str() else {
                continue;
            };
            let normalized = normalize_sql_qualifier(sql, schema);
            let normalized = if schema == "pg_temp" {
                normalized
            } else {
                normalize_sql_qualifier(&normalized, "pg_temp")
            };
            *value = serde_json::Value::String(normalized);
        }
        if let Ok(normalized) = serde_json::to_string(&value) {
            *row = normalized;
        }
    }
}

/// Removes only an identifier qualifier from SQL returned by `pg_get_*`.
///
/// String and dollar-quoted literals are copied byte-for-byte. Quoted identifiers are decoded
/// only far enough to recognize the selected schema, so literal data such as
/// `'content_vault.reserved'` remains part of the catalog proof.
fn normalize_sql_qualifier(sql: &str, schema: &str) -> String {
    let bytes = sql.as_bytes();
    let schema = schema.as_bytes();
    let mut normalized = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] == b'\'' {
            let end = quoted_literal_end(bytes, cursor);
            normalized.extend_from_slice(&bytes[cursor..end]);
            cursor = end;
            continue;
        }
        if bytes[cursor] == b'$'
            && let Some((delimiter_end, body_end)) = dollar_quoted_end(bytes, cursor)
        {
            normalized.extend_from_slice(&bytes[cursor..body_end]);
            cursor = body_end.max(delimiter_end);
            continue;
        }
        if bytes[cursor] == b'"'
            && let Some((end, identifier)) = quoted_identifier(bytes, cursor)
        {
            if identifier == schema && bytes.get(end) == Some(&b'.') {
                cursor = end + 1;
            } else {
                normalized.extend_from_slice(&bytes[cursor..end]);
                cursor = end;
            }
            continue;
        }
        if bytes[cursor..].starts_with(schema)
            && (cursor == 0 || !is_identifier_byte(bytes[cursor - 1]))
            && bytes.get(cursor + schema.len()) == Some(&b'.')
        {
            cursor += schema.len() + 1;
            continue;
        }
        normalized.push(bytes[cursor]);
        cursor += 1;
    }
    String::from_utf8(normalized).expect("PostgreSQL catalog text is valid UTF-8")
}

fn quoted_literal_end(bytes: &[u8], start: usize) -> usize {
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'\\' && cursor + 1 < bytes.len() {
            cursor += 2;
        } else if bytes[cursor] == b'\'' {
            if bytes.get(cursor + 1) == Some(&b'\'') {
                cursor += 2;
            } else {
                return cursor + 1;
            }
        } else {
            cursor += 1;
        }
    }
    bytes.len()
}

fn dollar_quoted_end(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let tag_end = bytes[start + 1..].iter().position(|byte| *byte == b'$')? + start + 1;
    let tag = &bytes[start + 1..tag_end];
    if !tag.is_empty()
        && (!matches!(tag[0], b'A'..=b'Z' | b'a'..=b'z' | b'_')
            || !tag
                .iter()
                .all(|byte| matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')))
    {
        return None;
    }
    let delimiter = &bytes[start..=tag_end];
    let body_start = tag_end + 1;
    let closing = bytes[body_start..]
        .windows(delimiter.len())
        .position(|candidate| candidate == delimiter)?
        + body_start;
    Some((body_start, closing + delimiter.len()))
}

fn quoted_identifier(bytes: &[u8], start: usize) -> Option<(usize, Vec<u8>)> {
    let mut identifier = Vec::new();
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"' {
            if bytes.get(cursor + 1) == Some(&b'"') {
                identifier.push(b'"');
                cursor += 2;
            } else {
                return Some((cursor + 1, identifier));
            }
        } else {
            identifier.push(bytes[cursor]);
            cursor += 1;
        }
    }
    None
}

const fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$') || !byte.is_ascii()
}

#[allow(clippy::too_many_lines)] // The full owned-catalog projection is intentionally auditable together.
async fn catalog_fingerprint(
    connection: &mut PgConnection,
    schema: &str,
) -> Result<CatalogFingerprint, sqlx::Error> {
    let schema_fingerprint = sqlx::query_scalar::<_, String>(
        r"
        SELECT jsonb_build_array(
          pg_get_userbyid(namespaces.nspowner),
          COALESCE(namespaces.nspacl::text, ''),
          COALESCE(obj_description(namespaces.oid, 'pg_namespace'), ''),
          COALESCE((SELECT jsonb_agg(jsonb_build_array(labels.provider, labels.label)
                                     ORDER BY labels.provider)::text
                    FROM pg_seclabel AS labels
                    WHERE labels.objoid = namespaces.oid AND labels.objsubid = 0), '[]')
        )::text
        FROM pg_namespace AS namespaces WHERE namespaces.nspname = $1
        ",
    )
    .bind(schema)
    .fetch_one(&mut *connection)
    .await?;

    let relations = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(
          relations.relname, relations.relkind::text,
          pg_get_userbyid(relations.relowner), COALESCE(relations.relacl::text, ''),
          CASE WHEN namespaces.oid = pg_my_temp_schema() AND relations.relpersistence = 't'
               THEN 'p' ELSE relations.relpersistence::text END,
          relations.relrowsecurity, relations.relforcerowsecurity,
          relations.relreplident::text, COALESCE(access_methods.amname, ''),
          COALESCE(obj_description(relations.oid, 'pg_class'), ''),
          COALESCE((SELECT jsonb_agg(jsonb_build_array(labels.provider, labels.label)
                                     ORDER BY labels.provider)::text
                    FROM pg_seclabel AS labels
                    WHERE labels.objoid = relations.oid AND labels.objsubid = 0), '[]')
        )::text
        FROM pg_class AS relations
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        LEFT JOIN pg_am AS access_methods ON access_methods.oid = relations.relam
        WHERE namespaces.nspname = $1 AND relations.relkind NOT IN ('i', 'I')
        ORDER BY relations.relname
        ",
    )
    .await?;
    let columns = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(
          relations.relname, attributes.attnum, attributes.attname,
          format_type(attributes.atttypid, attributes.atttypmod), attributes.attnotnull,
          COALESCE(pg_get_expr(defaults.adbin, defaults.adrelid, false), ''),
          attributes.attidentity::text, attributes.attgenerated::text,
          attributes.attndims, attributes.atthasmissing,
          COALESCE(attributes.attmissingval::text, ''), COALESCE(collations.collname, ''),
          COALESCE(attributes.attacl::text, ''),
          COALESCE(col_description(relations.oid, attributes.attnum), ''),
          COALESCE((SELECT jsonb_agg(jsonb_build_array(labels.provider, labels.label)
                                     ORDER BY labels.provider)::text
                    FROM pg_seclabel AS labels
                    WHERE labels.objoid = relations.oid
                      AND labels.objsubid = attributes.attnum), '[]')
        )::text
        FROM pg_attribute AS attributes
        JOIN pg_class AS relations ON relations.oid = attributes.attrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        LEFT JOIN pg_attrdef AS defaults
          ON defaults.adrelid = relations.oid AND defaults.adnum = attributes.attnum
        LEFT JOIN pg_collation AS collations ON collations.oid = attributes.attcollation
        WHERE namespaces.nspname = $1
          AND relations.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND attributes.attnum > 0 AND NOT attributes.attisdropped
        ORDER BY relations.relname, attributes.attnum
        ",
    )
    .await?;
    let constraints = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(
          relations.relname, constraints.conname, constraints.contype::text,
          constraints.condeferrable, constraints.condeferred, constraints.convalidated,
          constraints.connoinherit, pg_get_constraintdef(constraints.oid, false),
          COALESCE(obj_description(constraints.oid, 'pg_constraint'), '')
        )::text
        FROM pg_constraint AS constraints
        JOIN pg_class AS relations ON relations.oid = constraints.conrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        WHERE namespaces.nspname = $1
        ORDER BY relations.relname, constraints.conname
        ",
    )
    .await?;
    let indexes = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(
          relations.relname, indexes.relname, pg_get_userbyid(indexes.relowner),
          COALESCE(indexes.relacl::text, ''), COALESCE(access_methods.amname, ''),
          catalog.indisunique, catalog.indisprimary, catalog.indisexclusion,
          catalog.indimmediate, catalog.indisvalid, catalog.indisready,
          catalog.indislive, catalog.indisreplident, catalog.indcheckxmin,
          pg_get_indexdef(indexes.oid, 0, false),
          COALESCE(obj_description(indexes.oid, 'pg_class'), ''),
          COALESCE((SELECT jsonb_agg(jsonb_build_array(labels.provider, labels.label)
                                     ORDER BY labels.provider)::text
                    FROM pg_seclabel AS labels
                    WHERE labels.objoid = indexes.oid AND labels.objsubid = 0), '[]')
        )::text
        FROM pg_index AS catalog
        JOIN pg_class AS indexes ON indexes.oid = catalog.indexrelid
        JOIN pg_class AS relations ON relations.oid = catalog.indrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        LEFT JOIN pg_am AS access_methods ON access_methods.oid = indexes.relam
        WHERE namespaces.nspname = $1
        ORDER BY relations.relname, indexes.relname
        ",
    )
    .await?;
    let triggers = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(relations.relname, triggers.tgname,
          triggers.tgenabled::text, pg_get_triggerdef(triggers.oid, false),
          COALESCE(obj_description(triggers.oid, 'pg_trigger'), ''))::text
        FROM pg_trigger AS triggers
        JOIN pg_class AS relations ON relations.oid = triggers.tgrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        WHERE namespaces.nspname = $1 AND NOT triggers.tgisinternal
        ORDER BY relations.relname, triggers.tgname
        ",
    )
    .await?;
    let policies = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(relations.relname, policies.polname,
          policies.polcmd::text, policies.polpermissive,
          COALESCE(pg_get_expr(policies.polqual, policies.polrelid, false), ''),
          COALESCE(pg_get_expr(policies.polwithcheck, policies.polrelid, false), ''))::text
        FROM pg_policy AS policies
        JOIN pg_class AS relations ON relations.oid = policies.polrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        WHERE namespaces.nspname = $1 ORDER BY relations.relname, policies.polname
        ",
    )
    .await?;
    let routines = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(routines.proname,
          pg_get_function_identity_arguments(routines.oid),
          pg_get_function_result(routines.oid), languages.lanname,
          routines.provolatile::text, routines.proisstrict, routines.prosecdef,
          routines.proparallel::text, COALESCE(routines.proconfig::text, ''),
          COALESCE(routines.proacl::text, ''), pg_get_functiondef(routines.oid))::text
        FROM pg_proc AS routines
        JOIN pg_namespace AS namespaces ON namespaces.oid = routines.pronamespace
        JOIN pg_language AS languages ON languages.oid = routines.prolang
        WHERE namespaces.nspname = $1 ORDER BY routines.proname, routines.oid
        ",
    )
    .await?;
    let types = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(types.typname, types.typtype::text,
          pg_get_userbyid(types.typowner), COALESCE(types.typacl::text, ''),
          types.typcategory::text, types.typispreferred, types.typnotnull,
          COALESCE(base_types.typname, ''), COALESCE(element_types.typname, ''),
          COALESCE(relations.relname, ''), COALESCE(array_types.typname, ''),
          COALESCE(collations.collname, ''), COALESCE(types.typdefault, ''),
          COALESCE(obj_description(types.oid, 'pg_type'), ''),
          COALESCE((SELECT jsonb_agg(jsonb_build_array(labels.provider, labels.label)
                                     ORDER BY labels.provider)::text
                    FROM pg_seclabel AS labels
                    WHERE labels.objoid = types.oid AND labels.objsubid = 0), '[]'))::text
        FROM pg_type AS types
        JOIN pg_namespace AS namespaces ON namespaces.oid = types.typnamespace
        LEFT JOIN pg_type AS base_types ON base_types.oid = types.typbasetype
        LEFT JOIN pg_type AS element_types ON element_types.oid = types.typelem
        LEFT JOIN pg_class AS relations ON relations.oid = types.typrelid
        LEFT JOIN pg_type AS array_types ON array_types.oid = types.typarray
        LEFT JOIN pg_collation AS collations ON collations.oid = types.typcollation
        WHERE namespaces.nspname = $1
        ORDER BY types.typname
        ",
    )
    .await?;
    let inheritance = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(children.relname, parents.relname, inheritance.inhseqno)::text
        FROM pg_inherits AS inheritance
        JOIN pg_class AS children ON children.oid = inheritance.inhrelid
        JOIN pg_class AS parents ON parents.oid = inheritance.inhparent
        JOIN pg_namespace AS namespaces ON namespaces.oid = children.relnamespace
        WHERE namespaces.nspname = $1 ORDER BY children.relname, inheritance.inhseqno
        ",
    )
    .await?;
    let rules = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(relations.relname, rules.rulename,
          rules.ev_enabled::text, pg_get_ruledef(rules.oid, false))::text
        FROM pg_rewrite AS rules
        JOIN pg_class AS relations ON relations.oid = rules.ev_class
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        WHERE namespaces.nspname = $1 AND rules.rulename <> '_RETURN'
        ORDER BY relations.relname, rules.rulename
        ",
    )
    .await?;
    let statistics = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(statistics.stxname, pg_get_userbyid(statistics.stxowner),
          statistics.stxkeys::text, statistics.stxkind::text,
          COALESCE(obj_description(statistics.oid, 'pg_statistic_ext'), ''))::text
        FROM pg_statistic_ext AS statistics
        JOIN pg_namespace AS namespaces ON namespaces.oid = statistics.stxnamespace
        WHERE namespaces.nspname = $1 ORDER BY statistics.stxname
        ",
    )
    .await?;
    let publications = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(relations.relname, publications.pubname,
          COALESCE(pg_get_expr(entries.prqual, entries.prrelid, false), ''),
          COALESCE(entries.prattrs::text, ''))::text
        FROM pg_publication_rel AS entries
        JOIN pg_publication AS publications ON publications.oid = entries.prpubid
        JOIN pg_class AS relations ON relations.oid = entries.prrelid
        JOIN pg_namespace AS namespaces ON namespaces.oid = relations.relnamespace
        WHERE namespaces.nspname = $1 ORDER BY relations.relname, publications.pubname
        ",
    )
    .await?;
    let default_acls = catalog_rows(
        connection,
        schema,
        r"
        SELECT jsonb_build_array(pg_get_userbyid(defaults.defaclrole),
          defaults.defaclobjtype::text, defaults.defaclacl::text)::text
        FROM pg_default_acl AS defaults
        JOIN pg_namespace AS namespaces ON namespaces.oid = defaults.defaclnamespace
        WHERE namespaces.nspname = $1 ORDER BY defaults.defaclobjtype
        ",
    )
    .await?;

    Ok(CatalogFingerprint {
        schema: schema_fingerprint,
        relations,
        columns,
        constraints,
        indexes,
        triggers,
        policies,
        routines,
        types,
        inheritance,
        rules,
        statistics,
        publications,
        default_acls,
    })
}

async fn catalog_rows(
    connection: &mut PgConnection,
    schema: &str,
    query: &'static str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(query)
        .bind(schema)
        .fetch_all(&mut *connection)
        .await
}

fn migration_checksum(migration: &lenso_postgres_kit::Migration) -> String {
    let mut digest = Sha256::new();
    digest.update(migration.version().to_be_bytes());
    digest.update([0]);
    digest.update(migration.name().as_bytes());
    digest.update([0]);
    digest.update(migration.sql().as_bytes());
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_keeps_historical_migration_order_and_names() {
        assert_eq!(CONTENT_VAULT_MIGRATIONS.len(), 3);
        assert_eq!(CONTENT_VAULT_MIGRATIONS[0].version(), 1);
        assert_eq!(
            CONTENT_VAULT_MIGRATIONS[0].name(),
            "create-content-vault-schema"
        );
        assert_eq!(CONTENT_VAULT_MIGRATIONS[1].version(), 2);
        assert_eq!(CONTENT_VAULT_MIGRATIONS[1].name(), "add-streaming-io");
        assert_eq!(CONTENT_VAULT_MIGRATIONS[2].version(), 3);
        assert_eq!(
            CONTENT_VAULT_MIGRATIONS[2].name(),
            "make-pending-quarantine-cleanup-crash-safe"
        );
    }

    #[test]
    fn reference_sql_is_derived_only_from_immutable_migrations() {
        assert!(
            CONTENT_VAULT_MIGRATIONS[0]
                .sql()
                .starts_with(LEGACY_SCHEMA_STATEMENT)
        );
        assert!(
            CONTENT_VAULT_MIGRATIONS
                .iter()
                .all(|migration| !migration.sql().contains("pg_temp."))
        );
    }

    #[test]
    fn adoption_takes_shared_maintenance_lock_before_vault_lock() {
        assert!(ADOPTION_ADVISORY_LOCKS[0].contains(":lenso-maintenance"));
        assert!(ADOPTION_ADVISORY_LOCKS[1].contains(":content_vault"));
    }

    #[test]
    fn unsupported_namespace_catalog_coverage_is_pinned() {
        for namespace_edge in [
            "pg_proc WHERE pronamespace = namespaces.oid",
            "pg_collation WHERE collnamespace = namespaces.oid",
            "WHERE connamespace = namespaces.oid AND conrelid = 0",
            "pg_conversion WHERE connamespace = namespaces.oid",
            "pg_operator WHERE oprnamespace = namespaces.oid",
            "pg_opclass WHERE opcnamespace = namespaces.oid",
            "pg_opfamily WHERE opfnamespace = namespaces.oid",
            "pg_statistic_ext WHERE stxnamespace = namespaces.oid",
            "pg_extension WHERE extnamespace = namespaces.oid",
            "pg_ts_config WHERE cfgnamespace = namespaces.oid",
            "pg_ts_dict WHERE dictnamespace = namespaces.oid",
            "pg_ts_parser WHERE prsnamespace = namespaces.oid",
            "pg_ts_template WHERE tmplnamespace = namespaces.oid",
        ] {
            assert!(
                SCHEMA_UNSUPPORTED_OBJECT_COUNT_SQL.contains(namespace_edge),
                "missing unsupported namespace edge {namespace_edge}"
            );
        }
        for labeled_catalog in ["pg_namespace", "pg_class", "pg_constraint", "pg_type"] {
            assert!(
                SCHEMA_UNSUPPORTED_OBJECT_COUNT_SQL
                    .contains(&format!("'{labeled_catalog}'::regclass")),
                "missing SECURITY LABEL coverage for {labeled_catalog}"
            );
        }
        assert!(SCHEMA_UNSUPPORTED_OBJECT_COUNT_SQL.contains("obj_description(types.oid"));
    }

    #[test]
    fn publication_and_legacy_ledger_extra_object_proofs_are_pinned() {
        for publication_edge in [
            "publications.puballtables",
            "pg_publication_namespace",
            "pg_publication_rel",
        ] {
            assert!(
                UNSAFE_PUBLICATION_EXPOSURE_SQL.contains(publication_edge),
                "missing publication edge {publication_edge}"
            );
        }
        for ledger_edge in [
            "pg_rewrite",
            "pg_inherits",
            "pg_statistic_ext",
            "pg_constraint",
            "pg_seclabel",
        ] {
            assert!(
                LEGACY_LEDGER_UNSAFE_METADATA_SQL.contains(ledger_edge),
                "missing legacy ledger edge {ledger_edge}"
            );
        }
    }

    #[test]
    fn sql_qualifier_normalization_never_rewrites_literal_data() {
        assert_eq!(
            normalize_sql_qualifier(
                "REFERENCES content_vault.blobs (id) CHECK (state = 'content_vault.reserved')",
                CONTENT_VAULT_SCHEMA,
            ),
            "REFERENCES blobs (id) CHECK (state = 'content_vault.reserved')"
        );
        assert_eq!(
            normalize_sql_qualifier(
                "SELECT \"content_vault\".inspect($body$content_vault.literal$body$)",
                CONTENT_VAULT_SCHEMA,
            ),
            "SELECT inspect($body$content_vault.literal$body$)"
        );
        assert_eq!(
            normalize_sql_qualifier(
                "SELECT content_vault.inspect('it''s content_vault.literal')",
                CONTENT_VAULT_SCHEMA,
            ),
            "SELECT inspect('it''s content_vault.literal')"
        );
    }

    #[test]
    fn field_aware_normalization_preserves_comments() {
        let mut rows = vec![
            serde_json::json!([
                "upload_sessions",
                1,
                "state",
                "text",
                true,
                "content_vault.default_value",
                "",
                "",
                0,
                false,
                "",
                "",
                "",
                "content_vault.comment_value",
                []
            ])
            .to_string(),
        ];
        normalize_catalog_sql_fields(&mut rows, &[3, 5], CONTENT_VAULT_SCHEMA);
        let row: serde_json::Value = serde_json::from_str(&rows[0]).unwrap();
        assert_eq!(row[5], "default_value");
        assert_eq!(row[13], "content_vault.comment_value");
    }
}
