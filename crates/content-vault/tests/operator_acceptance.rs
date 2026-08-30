#![cfg(feature = "postgres-acceptance")]

use crate::test_support::CONTENT_VAULT_MIGRATIONS;
use crate::{ContentVaultOperator, ContentVaultOperatorError, SetupOutcome, UpgradeOutcome};
use lenso_postgres_kit::PostgresKitError;
use sqlx::{PgPool, postgres::PgPoolOptions};

const ACCEPTANCE_LOCK_ID: i64 = 0x434F_4E54_5641_554C;
const V1_HOST_MIGRATION: &str = "content-vault/0001_create_content_vault_schema";
const V2_HOST_MIGRATION: &str = "content-vault/0002_streaming_io";

#[tokio::test]
#[allow(clippy::too_many_lines)] // One serialized matrix owns and restores the destructive DB fixture.
async fn operator_is_exact_read_only_at_runtime_and_adopts_legacy_data_fail_closed() {
    let database_url = std::env::var("CONTENT_VAULT_TEST_DATABASE_URL").expect(
        "postgres-acceptance requires CONTENT_VAULT_TEST_DATABASE_URL; it never silently skips",
    );
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&database_url)
        .await
        .expect("connect to dedicated Content Vault test database");
    assert_test_database(&pool).await;
    let mut guard = pool.acquire().await.expect("acceptance database guard");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(ACCEPTANCE_LOCK_ID)
        .execute(&mut *guard)
        .await
        .expect("serialize Content Vault acceptance");

    reset(&pool).await;
    assert!(matches!(
        ContentVaultOperator::connect(&database_url).await,
        Err(ContentVaultOperatorError::Postgres(
            PostgresKitError::SetupRequired { .. }
        ))
    ));
    let schema_exists: bool =
        sqlx::query_scalar("SELECT to_regnamespace('content_vault') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!schema_exists, "runtime connect must not apply migrations");

    assert_eq!(
        ContentVaultOperator::setup(&database_url).await.unwrap(),
        SetupOutcome::Created {
            version: 3,
            applied: 3
        }
    );
    assert_eq!(
        ContentVaultOperator::setup(&database_url).await.unwrap(),
        SetupOutcome::AlreadyCurrent { version: 3 }
    );
    ContentVaultOperator::connect(&database_url)
        .await
        .expect("fresh setup installs exact current schema")
        .close()
        .await;

    sqlx::raw_sql("CREATE TABLE content_vault.unexpected (id bigint PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::connect(&database_url).await,
        Err(ContentVaultOperatorError::ManagedSchemaMismatch)
    ));
    assert!(matches!(
        ContentVaultOperator::setup(&database_url).await,
        Err(ContentVaultOperatorError::ManagedSchemaMismatch)
    ));
    sqlx::raw_sql("DROP TABLE content_vault.unexpected")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE COLLATION content_vault.unexpected_collation FROM \"C\"")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::connect(&database_url).await,
        Err(ContentVaultOperatorError::ManagedSchemaMismatch)
    ));
    assert!(matches!(
        ContentVaultOperator::setup(&database_url).await,
        Err(ContentVaultOperatorError::ManagedSchemaMismatch)
    ));
    sqlx::raw_sql("DROP COLLATION content_vault.unexpected_collation")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TEXT SEARCH CONFIGURATION content_vault.unexpected_ts \
         (COPY = pg_catalog.simple)",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::connect(&database_url).await,
        Err(ContentVaultOperatorError::ManagedSchemaMismatch)
    ));
    sqlx::raw_sql("DROP TEXT SEARCH CONFIGURATION content_vault.unexpected_ts")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE content_vault._lenso_schema_migrations SET checksum = 'tampered' WHERE version = 3",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::connect(&database_url).await,
        Err(ContentVaultOperatorError::Postgres(
            PostgresKitError::HistoryDiverged { version: 3, .. }
        ))
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    insert_sentinel(&pool, "legacy-v1").await;
    assert_eq!(
        ContentVaultOperator::adopt_legacy_v1(&database_url)
            .await
            .unwrap()
            .version,
        1
    );
    assert!(sentinel_exists(&pool, "legacy-v1").await);
    assert!(legacy_unrelated_row_exists(&pool).await);
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacySchemaAlreadyManaged)
    ));
    assert!(matches!(
        ContentVaultOperator::connect(&database_url).await,
        Err(ContentVaultOperatorError::Postgres(
            PostgresKitError::UpgradeRequired {
                current: 1,
                expected: 3,
                ..
            }
        ))
    ));
    assert_eq!(
        ContentVaultOperator::upgrade(&database_url).await.unwrap(),
        UpgradeOutcome::Applied {
            from: 1,
            to: 3,
            applied: 2
        }
    );
    assert!(sentinel_exists(&pool, "legacy-v1").await);
    ContentVaultOperator::connect(&database_url)
        .await
        .unwrap()
        .close()
        .await;

    reset(&pool).await;
    install_legacy(&pool, 2).await;
    insert_sentinel(&pool, "legacy-v2").await;
    assert_eq!(
        ContentVaultOperator::adopt_legacy_current(&database_url)
            .await
            .unwrap()
            .version,
        2
    );
    assert!(sentinel_exists(&pool, "legacy-v2").await);
    assert!(legacy_unrelated_row_exists(&pool).await);
    assert!(matches!(
        ContentVaultOperator::connect(&database_url).await,
        Err(ContentVaultOperatorError::Postgres(
            PostgresKitError::UpgradeRequired {
                current: 2,
                expected: 3,
                ..
            }
        ))
    ));
    assert_eq!(
        ContentVaultOperator::upgrade(&database_url).await.unwrap(),
        UpgradeOutcome::Applied {
            from: 2,
            to: 3,
            applied: 1
        }
    );
    ContentVaultOperator::connect(&database_url)
        .await
        .unwrap()
        .close()
        .await;

    assert_legacy_catalog_rejected(&pool, &database_url, "partial", |pool| {
        Box::pin(async move {
            sqlx::raw_sql("DROP TABLE content_vault.integrity_observations")
                .execute(pool)
                .await
                .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "tampered", |pool| {
        Box::pin(async move {
            sqlx::raw_sql(
                "ALTER TABLE content_vault.command_receipts \
                 ALTER COLUMN operation TYPE varchar(101)",
            )
            .execute(pool)
            .await
            .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "extra", |pool| {
        Box::pin(async move {
            sqlx::raw_sql("CREATE TABLE content_vault.unexpected (id bigint)")
                .execute(pool)
                .await
                .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "acl", |pool| {
        Box::pin(async move {
            sqlx::raw_sql("GRANT SELECT ON content_vault.blobs TO PUBLIC")
                .execute(pool)
                .await
                .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "type acl", |pool| {
        Box::pin(async move {
            sqlx::raw_sql("GRANT USAGE ON TYPE content_vault.blobs TO PUBLIC")
                .execute(pool)
                .await
                .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "comment", |pool| {
        Box::pin(async move {
            sqlx::raw_sql("COMMENT ON TABLE content_vault.blobs IS 'unexpected metadata'")
                .execute(pool)
                .await
                .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "default acl", |pool| {
        Box::pin(async move {
            sqlx::raw_sql(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA content_vault GRANT SELECT ON TABLES TO PUBLIC",
            )
            .execute(pool)
            .await
            .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "schema publication", |pool| {
        Box::pin(async move {
            sqlx::raw_sql(
                "CREATE PUBLICATION content_vault_test_publication \
                 FOR TABLES IN SCHEMA content_vault",
            )
            .execute(pool)
            .await
            .unwrap();
        })
    })
    .await;
    sqlx::raw_sql("DROP PUBLICATION content_vault_test_publication")
        .execute(&pool)
        .await
        .unwrap();
    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("CREATE PUBLICATION content_vault_test_publication FOR ALL TABLES")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));
    sqlx::raw_sql("DROP PUBLICATION content_vault_test_publication")
        .execute(&pool)
        .await
        .unwrap();
    assert_legacy_catalog_rejected(&pool, &database_url, "CHECK literal qualifier", |pool| {
        Box::pin(async move {
            sqlx::raw_sql(
                "ALTER TABLE content_vault.upload_sessions \
                   DROP CONSTRAINT upload_sessions_state_check; \
                 ALTER TABLE content_vault.upload_sessions \
                   ADD CONSTRAINT upload_sessions_state_check \
                   CHECK (state IN (\
                     'content_vault.reserved', 'staging', 'committed', 'rejected', 'expired'\
                   ))",
            )
            .execute(pool)
            .await
            .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "function literal", |pool| {
        Box::pin(async move {
            sqlx::raw_sql(
                "CREATE FUNCTION content_vault.literal_probe() RETURNS text \
                 LANGUAGE sql IMMUTABLE \
                 AS $body$ SELECT 'content_vault.literal'::text $body$",
            )
            .execute(pool)
            .await
            .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "custom collation", |pool| {
        Box::pin(async move {
            sqlx::raw_sql("CREATE COLLATION content_vault.unexpected_collation FROM \"C\"")
                .execute(pool)
                .await
                .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "text search configuration", |pool| {
        Box::pin(async move {
            sqlx::raw_sql(
                "CREATE TEXT SEARCH CONFIGURATION content_vault.unexpected_ts \
                 (COPY = pg_catalog.simple)",
            )
            .execute(pool)
            .await
            .unwrap();
        })
    })
    .await;
    assert_legacy_catalog_rejected(&pool, &database_url, "domain constraint", |pool| {
        Box::pin(async move {
            sqlx::raw_sql(
                "CREATE DOMAIN content_vault.unexpected_domain AS integer CHECK (VALUE > 0)",
            )
            .execute(pool)
            .await
            .unwrap();
        })
    })
    .await;

    reset(&pool).await;
    install_legacy(&pool, 2).await;
    sqlx::raw_sql(
        "ALTER TABLE content_vault.upload_sessions \
         ALTER COLUMN ingestion_mode SET DEFAULT 'content_vault.buffered'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_current(&database_url).await,
        Err(ContentVaultOperatorError::LegacySchemaMismatch { version: 2 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO PUBLIC")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacySchemaMismatch { version: 1 })
    ));
    sqlx::raw_sql("ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM PUBLIC")
        .execute(&pool)
        .await
        .unwrap();

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::query("DELETE FROM platform.schema_migrations WHERE name = $1")
        .bind(V1_HOST_MIGRATION)
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("GRANT SELECT ON platform.schema_migrations TO PUBLIC")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("ALTER TABLE platform.schema_migrations SET (fillfactor = 90)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("ALTER TABLE platform.schema_migrations ALTER COLUMN name SET STATISTICS 5")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("ALTER INDEX platform.schema_migrations_pkey SET (fillfactor = 80)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql(
        "COMMENT ON CONSTRAINT schema_migrations_pkey \
         ON platform.schema_migrations IS 'unexpected metadata'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql(
        "CREATE PUBLICATION content_vault_test_publication \
         FOR TABLE platform.schema_migrations",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql(
        "CREATE PUBLICATION content_vault_test_publication \
         FOR TABLES IN SCHEMA platform",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql(
        "CREATE RULE unexpected_ledger_rule AS \
         ON INSERT TO platform.schema_migrations DO ALSO NOTHING",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql(
        "CREATE STATISTICS platform.unexpected_ledger_stats \
         ON name, applied_at FROM platform.schema_migrations",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("GRANT USAGE ON TYPE platform.schema_migrations TO PUBLIC")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
    install_legacy(&pool, 1).await;
    sqlx::raw_sql("ALTER DEFAULT PRIVILEGES IN SCHEMA platform GRANT USAGE ON TYPES TO PUBLIC")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        ContentVaultOperator::adopt_legacy_v1(&database_url).await,
        Err(ContentVaultOperatorError::LegacyHostHistoryMismatch { version: 1 })
    ));

    reset(&pool).await;
}

async fn assert_test_database(pool: &PgPool) {
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .expect("read test database name");
    assert!(
        database_name == "content_vault_test" || database_name.starts_with("content_vault_test_"),
        "refusing destructive test setup against non-test database {database_name}"
    );
}

async fn reset(pool: &PgPool) {
    sqlx::raw_sql(
        "DROP PUBLICATION IF EXISTS content_vault_test_publication; \
         DROP SCHEMA IF EXISTS content_vault CASCADE; \
         DROP SCHEMA IF EXISTS platform CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset dedicated acceptance schemas");
}

async fn install_legacy(pool: &PgPool, version: usize) {
    sqlx::raw_sql(
        "CREATE SCHEMA platform; \
         CREATE TABLE platform.schema_migrations (\
           name text PRIMARY KEY,\
           applied_at timestamptz NOT NULL DEFAULT now()\
         )",
    )
    .execute(pool)
    .await
    .expect("install exact legacy Host ledger");
    sqlx::query("INSERT INTO platform.schema_migrations (name) VALUES ($1)")
        .bind("unrelated/0001")
        .execute(pool)
        .await
        .unwrap();
    for name in [V1_HOST_MIGRATION, V2_HOST_MIGRATION]
        .into_iter()
        .take(version)
    {
        sqlx::query("INSERT INTO platform.schema_migrations (name) VALUES ($1)")
            .bind(name)
            .execute(pool)
            .await
            .unwrap();
    }
    for migration in CONTENT_VAULT_MIGRATIONS.iter().take(version) {
        sqlx::raw_sql(migration.sql())
            .execute(pool)
            .await
            .expect("install immutable legacy migration");
    }
}

async fn insert_sentinel(pool: &PgPool, key: &str) {
    sqlx::query(
        "INSERT INTO content_vault.command_receipts \
         (scope, idempotency_key, tenant_id, operation, request_digest, response, created_at) \
         VALUES ('acceptance', $1, 'tenant', 'adoption', repeat('a', 64), '{}'::jsonb, now())",
    )
    .bind(key)
    .execute(pool)
    .await
    .unwrap();
}

async fn sentinel_exists(pool: &PgPool, key: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM content_vault.command_receipts \
         WHERE scope = 'acceptance' AND idempotency_key = $1)",
    )
    .bind(key)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn legacy_unrelated_row_exists(pool: &PgPool) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM platform.schema_migrations WHERE name = 'unrelated/0001')",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn assert_legacy_catalog_rejected<F>(pool: &PgPool, database_url: &str, case: &str, mutate: F)
where
    F: for<'a> FnOnce(&'a PgPool) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
{
    reset(pool).await;
    install_legacy(pool, 1).await;
    mutate(pool).await;
    assert!(
        matches!(
            ContentVaultOperator::adopt_legacy_v1(database_url).await,
            Err(ContentVaultOperatorError::LegacySchemaMismatch { version: 1 })
        ),
        "{case} legacy catalog must be rejected"
    );
}
