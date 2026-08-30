#![cfg(feature = "postgres-acceptance")]

use crate::test_support::{
    CONTENT_VAULT_MIGRATIONS, CompleteUploadRequest, ContentDescriptor, ContentVault,
    ContentVaultConfig, ContentVaultErrorCode, ContentVaultStreamingConfig, ImmutablePut,
    OwnerGrant, OwnerRef, ProtectedStore, QuarantineStore, ReserveUploadRequest, StageOutcome,
    StoreByteStream, StoreError, StoreErrorKind, StoreRead,
};
use crate::{public::UploadSessionId, storage::STREAMING_ATTEMPT_PREFIX};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::{StreamExt as _, stream};
use lenso::{Ctx, PluginError, ProviderStream, RuntimeFailure};
use lenso_capability_content_vault as capability;
use lenso_kernel::{CancellationToken, NativeStreamSession};
use sha2::{Digest as _, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeMap;
use std::io;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWriteExt as _, ReadBuf};

// Share the V1 acceptance lock because both suites rebuild the same schema.
const ACCEPTANCE_LOCK_ID: i64 = 0x434F_4E54_5641_554C;
const STREAM_PART_SIZE: usize = 8 * 1024 * 1024;
const LARGE_UPLOAD_SIZE: u64 = 64 * 1024 * 1024 + 1;

#[derive(Debug, Default)]
struct MemoryStreamingArea {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    largest_put_chunk: Mutex<usize>,
    fail_next_stream_read: Mutex<bool>,
    put_gate: Mutex<Option<PutGate>>,
    post_put_gate: Mutex<Option<PutGate>>,
    fail_after_put_once: AtomicBool,
    delete_calls: AtomicUsize,
}

#[derive(Debug)]
struct PutGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl MemoryStreamingArea {
    fn read(&self, key: &str) -> Option<Vec<u8>> {
        self.objects.lock().unwrap().get(key).cloned()
    }

    fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        self.observe_put_chunk(bytes.len());
        self.insert_immutable(key, bytes)
    }

    fn insert_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        let mut objects = self.objects.lock().unwrap();
        match objects.get(key) {
            Some(existing) if existing == &bytes => Ok(ImmutablePut::AlreadyPresent),
            Some(_) => Err(StoreError::new(
                StoreErrorKind::ImmutableConflict,
                "test immutable conflict",
            )),
            None => {
                objects.insert(key.to_owned(), bytes);
                Ok(ImmutablePut::Created)
            }
        }
    }

    async fn put_stream_immutable(
        &self,
        key: &str,
        expected_size_bytes: u64,
        mut source: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        let capacity = usize::try_from(expected_size_bytes).map_err(|_| {
            StoreError::new(StoreErrorKind::InvalidLength, "test stream is too large")
        })?;
        let mut received = Vec::with_capacity(capacity);
        while let Some(chunk) = source.next().await {
            let chunk = chunk?;
            self.observe_put_chunk(chunk.len());
            received.extend_from_slice(&chunk);
        }
        if received.len() != capacity {
            return Err(StoreError::new(
                StoreErrorKind::InvalidLength,
                "test stream length does not match its declaration",
            ));
        }
        self.insert_immutable(key, received)
    }

    fn read_stream(&self, key: &str) -> Option<StoreRead> {
        let bytes = self.read(key)?;
        let size_bytes = bytes.len() as u64;
        let chunks = bytes
            .chunks(STREAM_PART_SIZE)
            .map(Bytes::copy_from_slice)
            .map(Ok)
            .collect::<Vec<Result<Bytes, StoreError>>>();
        Some(StoreRead::new(size_bytes, stream::iter(chunks).boxed()))
    }

    fn fail_next_stream_read(&self) {
        *self.fail_next_stream_read.lock().unwrap() = true;
    }

    fn take_stream_read_failure(&self) -> bool {
        std::mem::take(&mut *self.fail_next_stream_read.lock().unwrap())
    }

    fn gate_next_put(&self) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.put_gate.lock().unwrap() = Some(PutGate {
            entered: entered.clone(),
            release: release.clone(),
        });
        (entered, release)
    }

    fn take_put_gate(&self) -> Option<PutGate> {
        self.put_gate.lock().unwrap().take()
    }

    fn gate_after_next_put(&self) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *self.post_put_gate.lock().unwrap() = Some(PutGate {
            entered: entered.clone(),
            release: release.clone(),
        });
        (entered, release)
    }

    fn take_post_put_gate(&self) -> Option<PutGate> {
        self.post_put_gate.lock().unwrap().take()
    }

    fn fail_after_next_put(&self) {
        self.fail_after_put_once.store(true, Ordering::Relaxed);
    }

    fn take_post_put_failure(&self) -> bool {
        self.fail_after_put_once.swap(false, Ordering::Relaxed)
    }

    fn contains_value(&self, value: &[u8]) -> bool {
        self.objects
            .lock()
            .unwrap()
            .values()
            .any(|candidate| candidate == value)
    }

    fn delete_exact(&self, key: &str) {
        self.delete_calls.fetch_add(1, Ordering::Relaxed);
        self.objects.lock().unwrap().remove(key);
    }

    fn delete_calls(&self) -> usize {
        self.delete_calls.load(Ordering::Relaxed)
    }

    fn largest_put_chunk(&self) -> usize {
        *self.largest_put_chunk.lock().unwrap()
    }

    fn observe_put_chunk(&self, length: usize) {
        let mut largest = self.largest_put_chunk.lock().unwrap();
        *largest = (*largest).max(length);
    }

    fn corrupt_after_first_part(&self) {
        let mut objects = self.objects.lock().unwrap();
        let mut remaining_offset = STREAM_PART_SIZE;
        for bytes in objects.values_mut() {
            if remaining_offset < bytes.len() {
                bytes[remaining_offset] ^= 0xff;
                return;
            }
            remaining_offset = remaining_offset.saturating_sub(bytes.len());
        }
        panic!("committed test content has no second part");
    }
}

#[derive(Debug, Clone, Default)]
struct MemoryQuarantine(Arc<MemoryStreamingArea>);

#[async_trait]
impl QuarantineStore for MemoryQuarantine {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.0.read(key))
    }

    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        if let Some(gate) = self.0.take_put_gate() {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let result = self.0.put_immutable(key, bytes);
        if result.is_ok()
            && let Some(gate) = self.0.take_post_put_gate()
        {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        if result.is_ok() && self.0.take_post_put_failure() {
            Err(StoreError::new(
                StoreErrorKind::Unavailable,
                "injected uncertain quarantine put failure",
            ))
        } else {
            result
        }
    }

    async fn delete_exact(&self, key: &str) -> Result<(), StoreError> {
        self.0.delete_exact(key);
        Ok(())
    }

    async fn read_stream(&self, key: &str) -> Result<Option<StoreRead>, StoreError> {
        if self.0.take_stream_read_failure() {
            return Err(StoreError::new(
                StoreErrorKind::Unavailable,
                "injected transient quarantine read failure",
            ));
        }
        Ok(self.0.read_stream(key))
    }

    async fn put_stream_immutable(
        &self,
        key: &str,
        expected_size_bytes: u64,
        bytes: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        self.0
            .put_stream_immutable(key, expected_size_bytes, bytes)
            .await
    }
}

#[derive(Debug, Clone, Default)]
struct MemoryProtected(Arc<MemoryStreamingArea>);

#[async_trait]
impl ProtectedStore for MemoryProtected {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.0.read(key))
    }

    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        self.0.put_immutable(key, bytes)
    }

    async fn read_stream(&self, key: &str) -> Result<Option<StoreRead>, StoreError> {
        Ok(self.0.read_stream(key))
    }

    async fn put_stream_immutable(
        &self,
        key: &str,
        expected_size_bytes: u64,
        bytes: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        self.0
            .put_stream_immutable(key, expected_size_bytes, bytes)
            .await
    }
}

struct Fixture {
    pool: sqlx::PgPool,
    vault: ContentVault,
    quarantine: MemoryQuarantine,
    protected: MemoryProtected,
    _database_guard: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_migration_count(CONTENT_VAULT_MIGRATIONS.len()).await
    }

    async fn before_cleanup_migration() -> Self {
        assert_eq!(CONTENT_VAULT_MIGRATIONS.len(), 3);
        Self::with_migration_count(CONTENT_VAULT_MIGRATIONS.len() - 1).await
    }

    async fn with_migration_count(migration_count: usize) -> Self {
        let database_url = std::env::var("CONTENT_VAULT_TEST_DATABASE_URL").expect(
            "postgres-acceptance requires CONTENT_VAULT_TEST_DATABASE_URL; it never silently skips",
        );
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("connect to dedicated Content Vault test database");
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            database_name == "content_vault_test"
                || database_name.starts_with("content_vault_test_"),
            "refusing destructive test setup against non-test database {database_name}"
        );

        let mut database_guard = pool.acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(ACCEPTANCE_LOCK_ID)
            .execute(&mut *database_guard)
            .await
            .unwrap();

        sqlx::raw_sql("DROP SCHEMA IF EXISTS content_vault CASCADE")
            .execute(&pool)
            .await
            .unwrap();
        for migration in &CONTENT_VAULT_MIGRATIONS[..migration_count] {
            sqlx::raw_sql(migration.sql()).execute(&pool).await.unwrap();
        }

        let quarantine = MemoryQuarantine::default();
        let protected = MemoryProtected::default();
        let vault = ContentVault::new(
            pool.clone(),
            Arc::new(quarantine.clone()),
            Arc::new(protected.clone()),
        )
        .with_config(ContentVaultConfig {
            minimum_reservation_ttl_seconds: 1,
            ..ContentVaultConfig::default()
        })
        .unwrap();
        Self {
            pool,
            vault,
            quarantine,
            protected,
            _database_guard: database_guard,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_upload_over_v1_limit_stays_lazy_and_bounds_store_chunks() {
    let fixture = Fixture::new().await;
    let grant = owner("tenant-stream", "large-source");
    let request = streaming_request("large", b'a', LARGE_UPLOAD_SIZE);

    let reserved = fixture
        .vault
        .reserve_streaming_upload(&grant, &request)
        .await
        .unwrap();
    assert_eq!(reserved.next_offset(), 0);
    assert_eq!(reserved.expected_size_bytes(), LARGE_UPLOAD_SIZE);
    assert!(reserved.expires_at() > chrono::Utc::now());
    let descriptor = reserved
        .commit(PatternReader::new(b'a', LARGE_UPLOAD_SIZE))
        .await
        .unwrap();

    assert_eq!(descriptor.size_bytes(), LARGE_UPLOAD_SIZE);
    assert_eq!(
        descriptor.sha256(),
        repeated_sha256(b'a', LARGE_UPLOAD_SIZE)
    );
    assert!(fixture.quarantine.0.largest_put_chunk() <= STREAM_PART_SIZE);
    assert!(fixture.protected.0.largest_put_chunk() <= STREAM_PART_SIZE);
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_stream_persists_only_durable_parts_and_resumes_at_the_next_offset() {
    let fixture = Fixture::new().await;
    let grant = owner("tenant-stream", "resumable-source");
    let total_size = 3 * STREAM_PART_SIZE as u64 + 29;
    let request = streaming_request("resume", b'b', total_size);
    let reserved = fixture
        .vault
        .reserve_streaming_upload(&grant, &request)
        .await
        .unwrap();
    let session_id = reserved.session_id();

    let error = reserved
        .commit(PatternReader::failing_after(
            b'b',
            total_size,
            STREAM_PART_SIZE as u64,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), ContentVaultErrorCode::UploadInterrupted);

    let resumed = fixture
        .vault
        .resume_streaming_upload(&grant, session_id)
        .await
        .unwrap();
    assert_eq!(resumed.session_id(), session_id);
    assert_eq!(resumed.next_offset(), STREAM_PART_SIZE as u64);
    let remaining = total_size - resumed.next_offset();
    let descriptor = resumed
        .commit(PatternReader::new(b'b', remaining))
        .await
        .unwrap();
    assert_eq!(descriptor.size_bytes(), total_size);
    assert_eq!(descriptor.sha256(), repeated_sha256(b'b', total_size));
}

#[tokio::test(flavor = "multi_thread")]
async fn another_owner_cannot_discover_or_resume_a_streaming_session() {
    let fixture = Fixture::new().await;
    let source_owner = owner("tenant-stream", "private-source");
    let intruder = owner("tenant-stream", "other-source");
    let request = streaming_request("private", b'p', STREAM_PART_SIZE as u64 + 1);
    let session_id = fixture
        .vault
        .reserve_streaming_upload(&source_owner, &request)
        .await
        .unwrap()
        .session_id();

    let error = fixture
        .vault
        .resume_streaming_upload(&intruder, session_id)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ContentVaultErrorCode::NotFound);
}

#[tokio::test(flavor = "multi_thread")]
async fn transient_validation_storage_failure_is_resumable_and_never_rejects_the_session() {
    let fixture = Fixture::new().await;
    let grant = owner("tenant-stream", "transient-validation");
    let total_size = STREAM_PART_SIZE as u64 + 1;
    let upload = fixture
        .vault
        .reserve_streaming_upload(
            &grant,
            &streaming_request("transient-validation", b't', total_size),
        )
        .await
        .unwrap();
    let session_id = upload.session_id();
    fixture.quarantine.0.fail_next_stream_read();

    let error = upload
        .commit(PatternReader::new(b't', total_size))
        .await
        .unwrap_err();
    assert_eq!(error.code(), ContentVaultErrorCode::StorageUnavailable);

    let resumed = fixture
        .vault
        .resume_streaming_upload(&grant, session_id)
        .await
        .unwrap();
    assert_eq!(resumed.next_offset(), total_size);
    let descriptor = resumed.commit(PatternReader::new(b't', 0)).await.unwrap();
    assert_eq!(descriptor.size_bytes(), total_size);
}

#[tokio::test(flavor = "multi_thread")]
async fn deterministic_streaming_validation_failure_is_a_stable_rejection() {
    let fixture = Fixture::new().await;
    let grant = owner("tenant-stream", "invalid-utf8");
    let bytes = vec![0xff];
    let upload = fixture
        .vault
        .reserve_streaming_upload(
            &grant,
            &ReserveUploadRequest::new(
                "invalid-utf8-reserve",
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                600,
            ),
        )
        .await
        .unwrap();
    let session_id = upload.session_id();

    let error = upload
        .commit(std::io::Cursor::new(bytes))
        .await
        .unwrap_err();
    assert_eq!(error.code(), ContentVaultErrorCode::UploadRejected);
    let replay = fixture
        .vault
        .resume_streaming_upload(&grant, session_id)
        .await
        .unwrap_err();
    assert_eq!(replay.code(), ContentVaultErrorCode::UploadRejected);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_or_cancelled_upload_protocol_cannot_commit_after_all_bytes_arrive() {
    let fixture = Fixture::new().await;

    for (resource_id, cancel) in [("invalid-extra-frame", false), ("consumer-cancel", true)] {
        let grant = owner("tenant-stream", resource_id);
        let bytes = format!("all reserved bytes for {resource_id}").into_bytes();
        let upload = fixture
            .vault
            .reserve_streaming_upload(
                &grant,
                &ReserveUploadRequest::new(
                    format!("{resource_id}-reserve"),
                    sha256(&bytes),
                    bytes.len() as u64,
                    "text/plain",
                    600,
                ),
            )
            .await
            .unwrap();
        let session_id = upload.session_id();
        let context = Ctx::new(1, None, CancellationToken::new());
        let (stream, mut provider) =
            ProviderStream::<capability::ContentVaultUpload>::channel(&context, 1);

        let consumer = async {
            stream
                .send(Box::new(capability::UploadFrame {
                    kind: capability::UploadFrameKind::Chunk,
                    offset: Some(0),
                    bytes_base64: Some(Some(STANDARD.encode(&bytes))),
                    content: None,
                }))
                .await
                .expect("the complete reserved payload is admitted");
            if cancel {
                stream.cancel();
            } else {
                stream
                    .send(Box::new(capability::UploadFrame {
                        kind: capability::UploadFrameKind::Committed,
                        offset: Some(i64::try_from(bytes.len()).unwrap()),
                        bytes_base64: None,
                        content: None,
                    }))
                    .await
                    .expect("the invalid extra frame reaches the provider");
            }
        };
        let provider_task = async { crate::module::drive_upload(&mut provider, upload).await };
        let ((), result) = futures::join!(consumer, provider_task);
        if cancel {
            assert!(matches!(
                result,
                Err(PluginError::Runtime(RuntimeFailure::AdmissionClosed))
            ));
        } else {
            assert!(matches!(
                result,
                Err(PluginError::Domain(capability::UploadError::InvalidInput))
            ));
        }

        let state: String = sqlx::query_scalar(
            "SELECT state FROM content_vault.upload_sessions WHERE session_id = $1",
        )
        .bind(session_id.as_uuid())
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(state, "reserved");
        let committed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM content_vault.content_objects WHERE tenant_id = $1",
        )
        .bind(grant.tenant_id())
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        let claims: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM content_vault.content_claims WHERE tenant_id = $1",
        )
        .bind(grant.tenant_id())
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(committed, 0, "a failed protocol must not commit content");
        assert_eq!(claims, 0, "a failed protocol must not create a claim");
        assert!(
            fixture.protected.0.objects.lock().unwrap().is_empty(),
            "a failed protocol must not promote protected bytes"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_active_streaming_writer_renews_its_database_lease() {
    let fixture = Fixture::new().await;
    let vault = fixture
        .vault
        .clone()
        .with_config(ContentVaultConfig {
            minimum_reservation_ttl_seconds: 1,
            staging_lease_seconds: 1,
            ..ContentVaultConfig::default()
        })
        .unwrap();
    let grant = owner("tenant-stream", "lease-heartbeat");
    let bytes = b"heartbeat keeps a slow streaming writer fenced".to_vec();
    let upload = vault
        .reserve_streaming_upload(
            &grant,
            &ReserveUploadRequest::new(
                "lease-heartbeat-reserve",
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                600,
            ),
        )
        .await
        .unwrap();
    let session_id = upload.session_id();
    let (mut writer, reader) = tokio::io::duplex(64);
    let commit = tokio::spawn(async move { upload.commit(reader).await });

    let first_heartbeat = loop {
        let observed = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT staging_started_at FROM content_vault.upload_sessions \
             WHERE session_id = $1 AND state = 'staging'",
        )
        .bind(session_id.as_uuid())
        .fetch_optional(&fixture.pool)
        .await
        .unwrap();
        if let Some(observed) = observed {
            break observed;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };

    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let renewed = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT staging_started_at FROM content_vault.upload_sessions WHERE session_id = $1",
    )
    .bind(session_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert!(renewed > first_heartbeat);

    writer.write_all(&bytes).await.unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(
        commit.await.unwrap().unwrap().size_bytes(),
        bytes.len() as u64
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_successful_late_streaming_part_put_is_fenced_and_cleaned() {
    late_streaming_part_put_is_fenced_and_cleaned(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_migration_keeps_legacy_abandoned_writers_unresolved_and_retryable() {
    let fixture = Fixture::before_cleanup_migration().await;
    let grant = owner("tenant-stream", "legacy-abandoned-migration");
    let bytes = b"legacy late writer bytes".to_vec();
    let session_id = fixture
        .vault
        .reserve_streaming_upload(
            &grant,
            &ReserveUploadRequest::new(
                "legacy-abandoned-migration-reserve",
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                600,
            ),
        )
        .await
        .unwrap()
        .session_id();
    let part_id = uuid::Uuid::now_v7();
    let legacy_key = format!("tenants/legacy/streaming-parts/{part_id}");
    let legacy_cleanup_at = Utc::now() - Duration::seconds(2);
    sqlx::query(
        "INSERT INTO content_vault.upload_parts (\
             part_id, session_id, part_index, byte_offset, quarantine_key, attempt_token, \
             state, quarantine_cleanup_attempted_at, quarantine_cleanup_succeeded_at, \
             quarantine_cleanup_attempts, created_at, updated_at\
         ) VALUES ($1, $2, 0, 0, $3, $4, 'abandoned', $5, $5, 1, $5, $5)",
    )
    .bind(part_id)
    .bind(session_id.as_uuid())
    .bind(&legacy_key)
    .bind(uuid::Uuid::now_v7())
    .bind(legacy_cleanup_at)
    .execute(&fixture.pool)
    .await
    .unwrap();

    sqlx::raw_sql(CONTENT_VAULT_MIGRATIONS[2].sql())
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(pending_part_cleanup_is_unresolved(&fixture, session_id).await);
    assert!(!legacy_key.starts_with(STREAMING_ATTEMPT_PREFIX));

    fixture
        .quarantine
        .0
        .put_immutable(&legacy_key, bytes.clone())
        .unwrap();
    let first_retry = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now(), Duration::zero(), 100)
        .await
        .unwrap();
    assert_eq!(first_retry.cleaned_objects, 1);
    assert!(!fixture.quarantine.0.contains_value(&bytes));
    assert!(pending_part_cleanup_is_unresolved(&fixture, session_id).await);

    fixture
        .quarantine
        .0
        .put_immutable(&legacy_key, bytes.clone())
        .unwrap();
    let second_retry = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(1), Duration::zero(), 100)
        .await
        .unwrap();
    assert_eq!(second_retry.cleaned_objects, 1);
    assert!(!fixture.quarantine.0.contains_value(&bytes));
    let cleanup_attempts: i64 = sqlx::query_scalar(
        "SELECT quarantine_cleanup_attempts FROM content_vault.upload_parts WHERE part_id = $1",
    )
    .bind(part_id)
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(cleanup_attempts, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_uncertain_late_streaming_part_put_is_fenced_and_cleaned() {
    late_streaming_part_put_is_fenced_and_cleaned(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_writer_after_part_persistence_remains_sweep_eligible() {
    let fixture = Fixture::new().await;
    let grant = owner("tenant-stream", "aborted-after-persist");
    let bytes = b"persisted just before the writer crashes".to_vec();
    let upload = fixture
        .vault
        .reserve_streaming_upload(
            &grant,
            &ReserveUploadRequest::new(
                "aborted-after-persist-reserve",
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                1,
            ),
        )
        .await
        .unwrap();
    let session_id = upload.session_id();
    let (before_put, release_put) = fixture.quarantine.0.gate_next_put();
    let (after_put, _finish_put) = fixture.quarantine.0.gate_after_next_put();
    let late_bytes = bytes.clone();
    let writer = tokio::spawn(async move { upload.commit(std::io::Cursor::new(late_bytes)).await });
    before_put.notified().await;
    let part_key: String = sqlx::query_scalar(
        "SELECT quarantine_key FROM content_vault.upload_parts WHERE session_id = $1",
    )
    .bind(session_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert!(part_key.starts_with(STREAMING_ATTEMPT_PREFIX));

    sqlx::query(
        "UPDATE content_vault.upload_sessions \
         SET staging_started_at = $1, updated_at = $1 \
         WHERE session_id = $2",
    )
    .bind(Utc::now() - Duration::seconds(1_000))
    .bind(session_id.as_uuid())
    .execute(&fixture.pool)
    .await
    .unwrap();
    let terminal_time = Utc::now() + Duration::seconds(2);
    fixture
        .vault
        .sweep_terminal_quarantine_at(terminal_time, Duration::zero(), 100)
        .await
        .unwrap();
    assert!(pending_part_cleanup_is_unresolved(&fixture, session_id).await);

    release_put.notify_one();
    after_put.notified().await;
    assert!(fixture.quarantine.0.contains_value(&bytes));
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());

    let second_sweep = fixture
        .vault
        .sweep_terminal_quarantine_at(terminal_time + Duration::seconds(1), Duration::zero(), 100)
        .await
        .unwrap();
    assert_eq!(second_sweep.cleaned_objects, 1);
    assert!(!fixture.quarantine.0.contains_value(&bytes));
    assert!(pending_part_cleanup_is_unresolved(&fixture, session_id).await);
}

async fn late_streaming_part_put_is_fenced_and_cleaned(fail_after_put: bool) {
    let fixture = Fixture::new().await;
    let (resource_id, idempotency_key, bytes) = if fail_after_put {
        (
            "uncertain-late-part",
            "uncertain-late-part-reserve",
            b"uncertain late streaming part".to_vec(),
        )
    } else {
        (
            "successful-late-part",
            "successful-late-part-reserve",
            b"successful late streaming part".to_vec(),
        )
    };
    let grant = owner("tenant-stream", resource_id);
    let upload = fixture
        .vault
        .reserve_streaming_upload(
            &grant,
            &ReserveUploadRequest::new(
                idempotency_key,
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                1,
            ),
        )
        .await
        .unwrap();
    let session_id = upload.session_id();
    if fail_after_put {
        fixture.quarantine.0.fail_after_next_put();
    }
    let (entered, release) = fixture.quarantine.0.gate_next_put();
    let late_bytes = bytes.clone();
    let commit = tokio::spawn(async move { upload.commit(std::io::Cursor::new(late_bytes)).await });
    entered.notified().await;

    sqlx::query(
        "UPDATE content_vault.upload_sessions \
         SET staging_started_at = $1, updated_at = $1 \
         WHERE session_id = $2",
    )
    .bind(Utc::now() - Duration::seconds(1_000))
    .bind(session_id.as_uuid())
    .execute(&fixture.pool)
    .await
    .unwrap();

    let delete_calls_before = fixture.quarantine.0.delete_calls();
    let terminal_time = Utc::now() + Duration::seconds(2);
    let first_sweep = fixture
        .vault
        .sweep_terminal_quarantine_at(terminal_time, Duration::zero(), 100)
        .await
        .unwrap();
    assert_eq!(first_sweep.expired_sessions, 1);
    assert!(pending_part_cleanup_is_unresolved(&fixture, session_id).await);
    assert_eq!(fixture.quarantine.0.delete_calls(), delete_calls_before + 2);

    release.notify_one();
    let error = commit
        .await
        .unwrap()
        .expect_err("a fenced late streaming writer must not reconcile its part");
    assert_eq!(
        error.code(),
        if fail_after_put {
            ContentVaultErrorCode::StorageUnavailable
        } else {
            ContentVaultErrorCode::Conflict
        }
    );
    assert!(!fixture.quarantine.0.contains_value(&bytes));

    let (cleanup_succeeded_at, cleanup_attempts): (Option<DateTime<Utc>>, i64) = sqlx::query_as(
        "SELECT quarantine_cleanup_succeeded_at, quarantine_cleanup_attempts \
         FROM content_vault.upload_parts WHERE session_id = $1",
    )
    .bind(session_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert!(cleanup_succeeded_at.is_some());
    assert_eq!(cleanup_attempts, 2);
    assert_eq!(fixture.quarantine.0.delete_calls(), delete_calls_before + 3);

    let second_sweep = fixture
        .vault
        .sweep_terminal_quarantine_at(terminal_time + Duration::seconds(1), Duration::zero(), 100)
        .await
        .unwrap();
    assert_eq!(second_sweep.cleaned_objects, 0);
    assert_eq!(second_sweep.failed_objects, 0);
    assert_eq!(fixture.quarantine.0.delete_calls(), delete_calls_before + 3);
}

async fn pending_part_cleanup_is_unresolved(
    fixture: &Fixture,
    session_id: UploadSessionId,
) -> bool {
    sqlx::query_scalar(
        "SELECT writer_resolved_at IS NULL AND quarantine_cleanup_succeeded_at IS NULL \
         FROM content_vault.upload_parts WHERE session_id = $1",
    )
    .bind(session_id.as_uuid())
    .fetch_one(&fixture.pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn verified_read_never_yields_a_corrupt_part_and_fails_closed_if_observation_fails() {
    let fixture = Fixture::new().await;
    let grant = owner("tenant-stream", "integrity-source");
    let total_size = 2 * STREAM_PART_SIZE as u64 + 17;
    let descriptor = stream_upload(&fixture, &grant, "integrity", b'c', total_size).await;
    fixture.protected.0.corrupt_after_first_part();

    let mut read = fixture
        .vault
        .fetch_verified(&grant, descriptor.content_id())
        .await
        .unwrap();
    let first = read.next_chunk().await.unwrap().unwrap();
    assert_eq!(first.len(), STREAM_PART_SIZE);
    assert!(first.iter().all(|byte| *byte == b'c'));
    let error = read.next_chunk().await.unwrap_err();
    assert_eq!(error.code(), ContentVaultErrorCode::IntegrityMismatch);

    sqlx::query("DROP TABLE content_vault.integrity_observations")
        .execute(&fixture.pool)
        .await
        .unwrap();
    let mut read = fixture
        .vault
        .fetch_verified(&grant, descriptor.content_id())
        .await
        .unwrap();
    let first = read.next_chunk().await.unwrap().unwrap();
    assert!(first.iter().all(|byte| *byte == b'c'));
    let error = read.next_chunk().await.unwrap_err();
    assert_eq!(error.code(), ContentVaultErrorCode::DatabaseUnavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_v1_upload_and_verified_read_remain_unchanged() {
    let fixture = Fixture::new().await;
    let invalid_config = fixture
        .vault
        .clone()
        .with_streaming_config(ContentVaultStreamingConfig {
            maximum_upload_size_bytes: 1_024 * 1_024 * 1_024,
            chunk_size_bytes: 7 * 1_024 * 1_024,
        })
        .unwrap_err();
    assert_eq!(invalid_config.code(), ContentVaultErrorCode::InvalidInput);

    let grant = owner("tenant-stream", "v1-source");
    let bytes = b"the buffered API remains compatible".to_vec();
    let session = fixture
        .vault
        .reserve_upload(
            &grant,
            &ReserveUploadRequest::new(
                "v1-reserve",
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                600,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .vault
            .stage_upload(&grant, session.session_id(), bytes.clone())
            .await
            .unwrap(),
        StageOutcome::Created
    );
    let descriptor = fixture
        .vault
        .complete_upload(
            &grant,
            &CompleteUploadRequest::new(session.session_id(), "v1-complete"),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .vault
            .read_content(&grant, descriptor.content_id())
            .await
            .unwrap()
            .bytes(),
        bytes
    );
}

async fn stream_upload(
    fixture: &Fixture,
    grant: &OwnerGrant,
    key: &str,
    byte: u8,
    size: u64,
) -> ContentDescriptor {
    fixture
        .vault
        .reserve_streaming_upload(grant, &streaming_request(key, byte, size))
        .await
        .unwrap()
        .commit(PatternReader::new(byte, size))
        .await
        .unwrap()
}

fn streaming_request(key: &str, byte: u8, size: u64) -> ReserveUploadRequest {
    ReserveUploadRequest::new(
        format!("{key}-reserve"),
        repeated_sha256(byte, size),
        size,
        "text/plain",
        600,
    )
}

fn owner(tenant_id: &str, resource_id: &str) -> OwnerGrant {
    OwnerGrant::new(
        tenant_id,
        OwnerRef::new("streaming-acceptance", "source", resource_id, None).unwrap(),
        "streaming-test-actor",
        format!("streaming-{resource_id}"),
    )
    .unwrap()
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn repeated_sha256(byte: u8, size: u64) -> String {
    let block = vec![byte; 64 * 1024];
    let mut remaining = size;
    let mut digest = Sha256::new();
    while remaining > 0 {
        let length = usize::try_from(remaining.min(block.len() as u64)).unwrap();
        digest.update(&block[..length]);
        remaining -= length as u64;
    }
    hex::encode(digest.finalize())
}

#[derive(Debug)]
struct PatternReader {
    byte: u8,
    remaining: u64,
    emitted: u64,
    fail_after: Option<u64>,
}

impl PatternReader {
    const fn new(byte: u8, size: u64) -> Self {
        Self {
            byte,
            remaining: size,
            emitted: 0,
            fail_after: None,
        }
    }

    const fn failing_after(byte: u8, size: u64, fail_after: u64) -> Self {
        Self {
            byte,
            remaining: size,
            emitted: 0,
            fail_after: Some(fail_after),
        }
    }
}

impl AsyncRead for PatternReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if self.fail_after == Some(self.emitted) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "injected source interruption",
            )));
        }
        if self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let before_failure = self
            .fail_after
            .map_or(self.remaining, |offset| offset.saturating_sub(self.emitted));
        let length = usize::try_from(
            self.remaining
                .min(before_failure)
                .min(buffer.remaining() as u64),
        )
        .unwrap();
        if length == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "injected source interruption",
            )));
        }
        buffer.initialize_unfilled()[..length].fill(self.byte);
        buffer.advance(length);
        self.remaining -= length as u64;
        self.emitted += length as u64;
        Poll::Ready(Ok(()))
    }
}
