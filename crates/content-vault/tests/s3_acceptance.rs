#![cfg(feature = "s3-acceptance")]

use content_vault::migrations::CONTENT_VAULT_MIGRATIONS;
use content_vault::module::{SWEEP_FUNCTION_NAME, linked_module};
use content_vault::{
    CONTENT_VAULT_S3_BUCKET_ENV, CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES, CompleteUploadRequest,
    ContentVault, ContentVaultStores, ImmutablePut, OwnerGrant, OwnerRef, ReserveUploadRequest,
    StageOutcome,
};
use lenso::host::runtime::{
    ActorContext, AppContext, CorrelationId, ExecutionContext, ExecutionId, TraceContext,
};
use platform_core::{
    AppConfig, AuthConfig, DatabaseConfig, HttpConfig, LoggingEventPublisher, ModuleConfig,
    ModuleSourcesConfig, RedisConfig, ServiceConfig, TelemetryConfig,
};
use sha2::{Digest as _, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

const ACCEPTANCE_LOCK_ID: i64 = 0x434F_4E54_5641_554C;

fn s3_stores() -> ContentVaultStores {
    std::env::var(CONTENT_VAULT_S3_BUCKET_ENV)
        .expect("s3-acceptance requires CONTENT_VAULT_S3_BUCKET; it never silently skips");
    ContentVaultStores::from_s3_env().expect("construct configured S3-compatible stores")
}

#[tokio::test]
async fn s3_capabilities_are_isolated_and_create_only() {
    let stores = s3_stores();
    let quarantine = stores.quarantine();
    let protected = stores.protected();
    let token = uuid::Uuid::now_v7();
    let key = format!("acceptance/{token}");
    let bytes = b"content vault S3 acceptance".to_vec();

    assert_eq!(
        quarantine
            .put_immutable(&key, bytes.clone())
            .await
            .expect("create quarantined object"),
        ImmutablePut::Created
    );
    assert_eq!(
        quarantine
            .put_immutable(&key, bytes.clone())
            .await
            .expect("retry quarantined object with identical bytes"),
        ImmutablePut::AlreadyPresent
    );
    assert!(
        quarantine
            .put_immutable(&key, b"different bytes".to_vec())
            .await
            .is_err(),
        "S3 create-only quarantine key must reject different bytes"
    );
    assert_eq!(
        quarantine.read(&key).await.expect("read quarantine"),
        Some(bytes.clone())
    );
    assert_eq!(
        protected
            .put_immutable(&key, bytes.clone())
            .await
            .expect("create protected object"),
        ImmutablePut::Created
    );
    assert_eq!(
        protected.read(&key).await.expect("read protected"),
        Some(bytes.clone())
    );

    quarantine
        .delete_exact(&key)
        .await
        .expect("delete exact quarantine key");
    assert_eq!(
        quarantine
            .read(&key)
            .await
            .expect("read deleted quarantine"),
        None
    );
    assert_eq!(
        protected.read(&key).await.expect("read protected again"),
        Some(bytes)
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn content_vault_commits_and_reads_through_postgres_and_s3() {
    let database_url = std::env::var("CONTENT_VAULT_TEST_DATABASE_URL")
        .expect("s3-acceptance requires CONTENT_VAULT_TEST_DATABASE_URL; it never silently skips");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("connect to dedicated Content Vault test database");
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .expect("read test database name");
    assert!(
        database_name == "content_vault_test" || database_name.starts_with("content_vault_test_"),
        "refusing destructive test setup against non-test database {database_name}"
    );
    let mut database_guard = pool
        .acquire()
        .await
        .expect("acquire acceptance database guard");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(ACCEPTANCE_LOCK_ID)
        .execute(&mut *database_guard)
        .await
        .expect("serialize destructive Content Vault acceptance");
    sqlx::raw_sql("DROP SCHEMA IF EXISTS content_vault CASCADE")
        .execute(&pool)
        .await
        .expect("reset Content Vault schema");
    for migration in CONTENT_VAULT_MIGRATIONS {
        sqlx::raw_sql(migration.sql)
            .execute(&pool)
            .await
            .expect("apply Content Vault migration");
    }

    let runtime_context = AppContext::new(
        AppConfig {
            service: ServiceConfig::default(),
            database: DatabaseConfig {
                url: database_url,
                max_connections: 5,
            },
            redis: RedisConfig::default(),
            http: HttpConfig::default(),
            telemetry: TelemetryConfig::default(),
            auth: AuthConfig::default(),
            module_sources: ModuleSourcesConfig {
                linked_profile: "core".to_owned(),
            },
            modules: BTreeMap::from([(
                "content-vault".to_owned(),
                ModuleConfig {
                    enabled: Some(true),
                    values: BTreeMap::from([
                        ("quarantine_grace_seconds".to_owned(), serde_json::json!(0)),
                        ("sweep_batch_limit".to_owned(), serde_json::json!(100)),
                    ]),
                },
            )]),
        },
        pool.clone(),
        Arc::new(LoggingEventPublisher),
    );
    let runtime_module = linked_module()
        .try_load_module(&runtime_context)
        .expect("load S3-backed Content Vault Runtime Module");
    let runtime_registry = lenso_bootstrap::try_function_registry(&[runtime_module])
        .expect("admit Content Vault Runtime binding");
    let sweep_handler = runtime_registry
        .get(SWEEP_FUNCTION_NAME)
        .expect("registered Content Vault sweep function")
        .handler
        .clone();

    let stores = s3_stores();
    let vault = ContentVault::from_stores(pool, &stores);
    let owner = OwnerGrant::new(
        "tenant-s3-acceptance",
        OwnerRef::new("fixture", "document", Uuid::now_v7().to_string(), None)
            .expect("valid owner"),
        "s3-acceptance",
        Uuid::now_v7().to_string(),
    )
    .expect("valid owner grant");
    let bytes = format!("content vault end-to-end {}", Uuid::now_v7()).into_bytes();
    let digest = hex::encode(Sha256::digest(&bytes));
    let session = vault
        .reserve_upload(
            &owner,
            &ReserveUploadRequest::new(
                "s3-e2e-reserve",
                digest,
                bytes.len() as u64,
                "text/plain",
                600,
            ),
        )
        .await
        .expect("reserve S3-backed upload");
    assert_eq!(
        vault
            .stage_upload(&owner, session.session_id(), bytes.clone())
            .await
            .expect("stage S3-backed upload"),
        StageOutcome::Created
    );
    let descriptor = vault
        .complete_upload(
            &owner,
            &CompleteUploadRequest::new(session.session_id(), "s3-e2e-complete"),
        )
        .await
        .expect("complete S3-backed upload");
    assert_eq!(
        vault
            .read_content(&owner, descriptor.content_id())
            .await
            .expect("read protected S3 object")
            .bytes(),
        bytes
    );
    let sweep_result = sweep_handler
        .call(
            ExecutionContext {
                execution_id: ExecutionId(Uuid::now_v7().to_string()),
                function_name: SWEEP_FUNCTION_NAME.to_owned(),
                attempt: 1,
                queue: "content-vault-maintenance".to_owned(),
                correlation_id: CorrelationId::new(Uuid::now_v7().to_string()),
                causation_id: None,
                actor: ActorContext::Service {
                    service_id: "content-vault-acceptance".to_owned(),
                    scopes: Vec::new(),
                },
                tenant_id: None,
                trace: TraceContext::default(),
                deadline: None,
            },
            serde_json::json!({
                "quarantine_grace_seconds": 86400,
                "sweep_batch_limit": 1,
            }),
        )
        .await
        .expect("run S3-backed Content Vault sweep handler");
    assert!(sweep_result["cleaned_objects"].as_u64().unwrap_or_default() >= 1);

    let streaming_bytes = vec![b's'; 2 * CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES as usize + 17];
    let streaming_digest = hex::encode(Sha256::digest(&streaming_bytes));
    let descriptor = vault
        .reserve_streaming_upload(
            &owner,
            &ReserveUploadRequest::new(
                "s3-streaming-reserve",
                streaming_digest,
                streaming_bytes.len() as u64,
                "text/plain",
                600,
            ),
        )
        .await
        .expect("reserve S3-backed streaming upload")
        .commit(std::io::Cursor::new(streaming_bytes.clone()))
        .await
        .expect("complete S3-backed streaming upload");
    let mut verified = vault
        .fetch_verified(&owner, descriptor.content_id())
        .await
        .expect("open verified S3 stream");
    let mut received = Vec::with_capacity(streaming_bytes.len());
    while let Some(chunk) = verified
        .next_chunk()
        .await
        .expect("verify the next S3-backed part")
    {
        assert!(chunk.len() <= CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES as usize);
        received.extend_from_slice(&chunk);
    }
    assert_eq!(received, streaming_bytes);
    assert!(
        vault
            .sweep_terminal_quarantine(chrono::Duration::zero(), 100)
            .await
            .expect("sweep streaming S3 quarantine")
            .cleaned_objects
            >= 3
    );

    let mut verified = vault
        .fetch_verified(&owner, descriptor.content_id())
        .await
        .expect("protected S3 stream survives quarantine cleanup");
    assert!(verified.next_chunk().await.unwrap().is_some());
}
