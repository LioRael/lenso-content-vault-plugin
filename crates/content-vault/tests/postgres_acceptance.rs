#![cfg(feature = "postgres-acceptance")]

use crate::module::capability_descriptor;
use crate::test_support::{
    CONTENT_VAULT_MIGRATIONS, CompleteUploadRequest, ContentClaimRole, ContentDescriptor,
    ContentId, ContentVault, ContentVaultConfig, ContentVaultErrorCode, ImmutablePut, OwnerGrant,
    OwnerRef, ProtectedStore, QuarantineStore, ReserveUploadRequest, StageOutcome, StoreError,
    StoreErrorKind, UploadSession,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use sha2::{Digest as _, Sha256};
use sqlx::postgres::PgPoolOptions;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const ACCEPTANCE_LOCK_ID: i64 = 0x434F_4E54_5641_554C;

#[derive(Debug, Default)]
struct MemoryQuarantine {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    fail_delete: Mutex<bool>,
    fail_delete_once_keys: Mutex<HashSet<String>>,
    put_gate: Mutex<Option<PutGate>>,
}

#[derive(Debug)]
struct PutGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl MemoryQuarantine {
    fn set_fail_delete(&self, value: bool) {
        *self.fail_delete.lock().unwrap() = value;
    }

    fn fail_delete_once_for_value(&self, value: &[u8]) {
        let key = self
            .objects
            .lock()
            .unwrap()
            .iter()
            .find_map(|(key, candidate)| (candidate.as_slice() == value).then(|| key.clone()))
            .expect("test quarantine value exists");
        self.fail_delete_once_keys.lock().unwrap().insert(key);
    }

    fn contains_value(&self, value: &[u8]) -> bool {
        self.objects
            .lock()
            .unwrap()
            .values()
            .any(|candidate| candidate == value)
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

    fn force_put(&self, key: &str, bytes: Vec<u8>) {
        self.objects.lock().unwrap().insert(key.to_owned(), bytes);
    }
}

#[async_trait]
impl QuarantineStore for MemoryQuarantine {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        let gate = self.put_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
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

    async fn delete_exact(&self, key: &str) -> Result<(), StoreError> {
        if *self.fail_delete.lock().unwrap()
            || self.fail_delete_once_keys.lock().unwrap().remove(key)
        {
            return Err(StoreError::new(
                StoreErrorKind::Unavailable,
                "injected quarantine delete failure",
            ));
        }
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct MemoryProtected {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryProtected {
    fn len(&self) -> usize {
        self.objects.lock().unwrap().len()
    }

    fn contains_value(&self, value: &[u8]) -> bool {
        self.objects
            .lock()
            .unwrap()
            .values()
            .any(|candidate| candidate == value)
    }

    fn remove_value(&self, value: &[u8]) {
        self.objects
            .lock()
            .unwrap()
            .retain(|_, candidate| candidate != value);
    }

    fn corrupt_value(&self, value: &[u8]) {
        let mut objects = self.objects.lock().unwrap();
        let candidate = objects
            .values_mut()
            .find(|candidate| candidate.as_slice() == value)
            .expect("committed test object exists");
        candidate.fill(b'x');
    }
}

#[async_trait]
impl ProtectedStore for MemoryProtected {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
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
}

struct Fixture {
    pool: sqlx::PgPool,
    vault: ContentVault,
    quarantine: Arc<MemoryQuarantine>,
    protected: Arc<MemoryProtected>,
    _database_guard: sqlx::pool::PoolConnection<sqlx::Postgres>,
}

impl Fixture {
    async fn new() -> Self {
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
        for migration in CONTENT_VAULT_MIGRATIONS {
            sqlx::raw_sql(migration.sql()).execute(&pool).await.unwrap();
        }

        let quarantine = Arc::new(MemoryQuarantine::default());
        let protected = Arc::new(MemoryProtected::default());
        let vault = ContentVault::new(pool.clone(), quarantine.clone(), protected.clone())
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

    async fn upload(
        &self,
        grant: &OwnerGrant,
        bytes: &[u8],
        key_prefix: &str,
    ) -> (UploadSession, ContentDescriptor) {
        let session = self
            .vault
            .reserve_upload(
                grant,
                &ReserveUploadRequest::new(
                    format!("{key_prefix}-reserve"),
                    sha256(bytes),
                    bytes.len() as u64,
                    "text/plain",
                    600,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            self.vault
                .stage_upload(grant, session.session_id(), bytes.to_vec())
                .await
                .unwrap(),
            StageOutcome::Created
        );
        let descriptor = self
            .vault
            .complete_upload(
                grant,
                &CompleteUploadRequest::new(session.session_id(), format!("{key_prefix}-complete")),
            )
            .await
            .unwrap();
        (session, descriptor)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn content_vault_v1_black_box_acceptance() {
    let fixture = Fixture::new().await;
    outbound_projection_rejection_acceptance(&fixture).await;
    cleanup_failure_does_not_starve_later_keys(&fixture).await;
    let source = owner("tenant-a", "profile", "avatar", "source");
    let bytes = b"hello from the content vault".to_vec();
    let reserve = ReserveUploadRequest::new(
        "happy-reserve",
        sha256(&bytes),
        bytes.len() as u64,
        "text/plain",
        600,
    );

    let first_session = fixture
        .vault
        .reserve_upload(&source, &reserve)
        .await
        .unwrap();
    let retry_source = owner("tenant-a", "profile", "avatar", "source");
    let repeated_session = fixture
        .vault
        .reserve_upload(&retry_source, &reserve)
        .await
        .unwrap();
    assert_eq!(first_session, repeated_session);
    let retry_source_different_actor = owner_as(
        "tenant-a",
        "profile",
        "avatar",
        "source",
        "replacement-actor",
    );
    let repeated_session_from_another_actor = fixture
        .vault
        .reserve_upload(&retry_source_different_actor, &reserve)
        .await
        .unwrap();
    assert_eq!(first_session, repeated_session_from_another_actor);
    let another_owner_same_key = owner("tenant-a", "profile", "avatar", "another-owner");
    let independent_session = fixture
        .vault
        .reserve_upload(&another_owner_same_key, &reserve)
        .await
        .unwrap();
    assert_ne!(first_session.session_id(), independent_session.session_id());
    let conflict = fixture
        .vault
        .reserve_upload(
            &source,
            &ReserveUploadRequest::new("happy-reserve", sha256(b"different"), 9, "text/plain", 600),
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), ContentVaultErrorCode::Conflict);

    assert_eq!(
        fixture
            .vault
            .stage_upload(&source, first_session.session_id(), bytes.clone())
            .await
            .unwrap(),
        StageOutcome::Created
    );
    assert_eq!(
        fixture
            .vault
            .stage_upload(&source, first_session.session_id(), bytes.clone())
            .await
            .unwrap(),
        StageOutcome::AlreadyPresent
    );
    let completion = CompleteUploadRequest::new(first_session.session_id(), "happy-complete");
    let descriptor = fixture
        .vault
        .complete_upload(&source, &completion)
        .await
        .unwrap();
    assert!(fixture.quarantine.contains_value(&bytes));
    assert_eq!(
        fixture
            .vault
            .complete_upload(&retry_source, &completion)
            .await
            .unwrap(),
        descriptor
    );
    assert_eq!(
        fixture
            .vault
            .complete_upload(&retry_source_different_actor, &completion)
            .await
            .unwrap(),
        descriptor
    );
    fixture
        .vault
        .stage_upload(
            &another_owner_same_key,
            independent_session.session_id(),
            bytes.clone(),
        )
        .await
        .unwrap();
    fixture
        .vault
        .complete_upload(
            &another_owner_same_key,
            &CompleteUploadRequest::new(independent_session.session_id(), "happy-complete"),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .vault
            .read_content(&source, descriptor.content_id())
            .await
            .unwrap()
            .bytes(),
        bytes
    );

    let wrong_tenant = owner("tenant-b", "profile", "avatar", "source");
    let wrong_owner = owner("tenant-a", "documents", "file", "other");
    let wrong_owner_in_same_module = owner("tenant-a", "profile", "avatar", "other");
    assert_eq!(
        fixture
            .vault
            .complete_upload(&wrong_owner_in_same_module, &completion)
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::NotFound
    );
    assert_eq!(
        fixture
            .vault
            .complete_upload(&another_owner_same_key, &completion)
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::NotFound
    );
    assert_eq!(
        fixture
            .vault
            .describe_content(&wrong_tenant, descriptor.content_id())
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::NotFound
    );
    assert_eq!(
        fixture
            .vault
            .describe_content(&wrong_owner, descriptor.content_id())
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::NotFound
    );

    claim_transaction_acceptance(&fixture, &source, descriptor.content_id()).await;
    validation_rejection_acceptance(&fixture).await;
    protected_orphan_retry_acceptance(&fixture).await;
    quarantine_delete_retry_acceptance(&fixture).await;
    quarantine_grace_acceptance(&fixture).await;
    integrity_acceptance(&fixture).await;
    integrity_observation_failure_acceptance(&fixture).await;
    tenant_scoping_acceptance(&fixture, &bytes, descriptor.content_id()).await;
    expiration_and_no_protected_delete_acceptance(&fixture).await;
    bounded_expiration_acceptance(&fixture).await;
    staging_lease_acceptance(&fixture).await;
    stale_staging_eventual_cleanup_acceptance(&fixture).await;
}

async fn outbound_projection_rejection_acceptance(fixture: &Fixture) {
    use chrono::TimeZone as _;

    let grant = owner("tenant-a", "projection", "row", "invalid-outbound");
    let blob_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO content_vault.blobs \
         (blob_id, tenant_id, sha256, size_bytes, media_type, protected_key, created_at) \
         VALUES ($1, $2, $3, 1, 'text/plain', $4, now())",
    )
    .bind(blob_id)
    .bind(grant.tenant_id())
    .bind("d".repeat(64))
    .bind("acceptance/projection/blob")
    .execute(&fixture.pool)
    .await
    .unwrap();

    for (case, content_id, created_at) in [
        (
            "nil UUID",
            Uuid::nil(),
            Utc.with_ymd_and_hms(2026, 8, 30, 0, 0, 0).unwrap(),
        ),
        (
            "extended-year timestamp",
            Uuid::now_v7(),
            Utc.with_ymd_and_hms(10_000, 1, 1, 0, 0, 0).unwrap(),
        ),
    ] {
        sqlx::query(
            "INSERT INTO content_vault.content_objects \
             (content_id, tenant_id, blob_id, committed_by_owner_module, \
              committed_by_owner_resource_type, committed_by_owner_resource_id, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(content_id)
        .bind(grant.tenant_id())
        .bind(blob_id)
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(created_at)
        .execute(&fixture.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO content_vault.content_claims \
             (claim_id, tenant_id, content_id, owner_module, owner_resource_type, \
              owner_resource_id, owner_revision_id, role, state, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, '', 'source', 'active', now())",
        )
        .bind(Uuid::now_v7())
        .bind(grant.tenant_id())
        .bind(content_id)
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .execute(&fixture.pool)
        .await
        .unwrap();
        let descriptor = fixture
            .vault
            .describe_content(&grant, ContentId::from_uuid(content_id))
            .await
            .unwrap();
        assert!(
            capability_descriptor(&descriptor).is_err(),
            "{case} must fail before a Capability wire value is emitted"
        );
    }
}

async fn cleanup_failure_does_not_starve_later_keys(fixture: &Fixture) {
    let first_owner = owner("tenant-a", "documents", "file", "cleanup-first");
    let second_owner = owner("tenant-a", "documents", "file", "cleanup-second");
    let first_bytes = b"cleanup first bytes";
    let second_bytes = b"cleanup second bytes";
    fixture
        .upload(&first_owner, first_bytes, "cleanup-first")
        .await;
    fixture
        .upload(&second_owner, second_bytes, "cleanup-second")
        .await;
    fixture.quarantine.fail_delete_once_for_value(first_bytes);

    let first = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(1), Duration::zero(), 1)
        .await
        .unwrap();
    assert_eq!(first.failed_objects, 1);
    assert!(fixture.quarantine.contains_value(first_bytes));
    assert!(fixture.quarantine.contains_value(second_bytes));

    let second = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(2), Duration::zero(), 1)
        .await
        .unwrap();
    assert_eq!(second.cleaned_objects, 1);
    assert!(fixture.quarantine.contains_value(first_bytes));
    assert!(!fixture.quarantine.contains_value(second_bytes));

    let third = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(3), Duration::zero(), 1)
        .await
        .unwrap();
    assert_eq!(third.cleaned_objects, 1);
    assert!(!fixture.quarantine.contains_value(first_bytes));
}

async fn staging_lease_acceptance(fixture: &Fixture) {
    let grant = owner("tenant-a", "documents", "file", "staging-lease");
    let bytes = b"in-flight staging bytes".to_vec();
    let session = fixture
        .vault
        .reserve_upload(
            &grant,
            &ReserveUploadRequest::new(
                "staging-lease-reserve",
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                1,
            ),
        )
        .await
        .unwrap();
    let (entered, release) = fixture.quarantine.gate_next_put();
    let vault = fixture.vault.clone();
    let staging_grant = grant.clone();
    let staging_bytes = bytes.clone();
    let staging = tokio::spawn(async move {
        vault
            .stage_upload(&staging_grant, session.session_id(), staging_bytes)
            .await
    });
    entered.notified().await;

    let while_leased = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(2), Duration::zero(), 100)
        .await
        .unwrap();
    assert_eq!(while_leased.expired_sessions, 0);
    release.notify_one();
    staging.await.unwrap().unwrap();

    let after_staging = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(2), Duration::zero(), 100)
        .await
        .unwrap();
    assert!(after_staging.expired_sessions >= 1);
    assert!(!fixture.quarantine.contains_value(&bytes));
}

async fn stale_staging_eventual_cleanup_acceptance(fixture: &Fixture) {
    let grant = owner("tenant-a", "documents", "file", "zombie-stage");
    let bytes = b"zombie staging bytes".to_vec();
    let session = fixture
        .vault
        .reserve_upload(
            &grant,
            &ReserveUploadRequest::new(
                "zombie-stage-reserve",
                sha256(&bytes),
                bytes.len() as u64,
                "text/plain",
                1,
            ),
        )
        .await
        .unwrap();
    let quarantine_key: String = sqlx::query_scalar(
        "SELECT quarantine_key FROM content_vault.upload_sessions WHERE session_id = $1",
    )
    .bind(session.session_id().as_uuid())
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE content_vault.upload_sessions \
         SET state = 'staging', staging_started_at = $1, updated_at = $1 \
         WHERE session_id = $2",
    )
    .bind(Utc::now() - Duration::seconds(1_000))
    .bind(session.session_id().as_uuid())
    .execute(&fixture.pool)
    .await
    .unwrap();

    let terminal_time = Utc::now() + Duration::seconds(2);
    let first_sweep = fixture
        .vault
        .sweep_terminal_quarantine_at(terminal_time, Duration::zero(), 1_000)
        .await
        .unwrap();
    assert!(first_sweep.expired_sessions >= 1);
    fixture.quarantine.force_put(&quarantine_key, bytes.clone());
    assert!(fixture.quarantine.contains_value(&bytes));

    let second_sweep = fixture
        .vault
        .sweep_terminal_quarantine_at(
            terminal_time + Duration::seconds(1),
            Duration::zero(),
            1_000,
        )
        .await
        .unwrap();
    assert!(second_sweep.cleaned_objects >= 1);
    assert!(!fixture.quarantine.contains_value(&bytes));
}

async fn claim_transaction_acceptance(
    fixture: &Fixture,
    source: &OwnerGrant,
    content_id: ContentId,
) {
    let target = owner("tenant-a", "profile", "avatar", "target");
    let role = ContentClaimRole::new("thumbnail").unwrap();

    let foreign_vault = ContentVault::new(
        fixture.pool.clone(),
        fixture.quarantine.clone(),
        fixture.protected.clone(),
    );
    let mut foreign_transaction = foreign_vault.begin_transaction().await.unwrap();
    assert_eq!(
        fixture
            .vault
            .claim_content_in_tx(&mut foreign_transaction, source, &target, content_id, &role,)
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::InvalidInput
    );
    foreign_transaction.rollback().await.unwrap();

    let mut transaction = fixture.vault.begin_transaction().await.unwrap();
    fixture
        .vault
        .claim_content_in_tx(&mut transaction, source, &target, content_id, &role)
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(
        fixture
            .vault
            .describe_content(&target, content_id)
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::NotFound
    );

    let mut transaction = fixture.vault.begin_transaction().await.unwrap();
    fixture
        .vault
        .claim_content_in_tx(&mut transaction, source, &target, content_id, &role)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    fixture
        .vault
        .describe_content(&target, content_id)
        .await
        .unwrap();

    let protected_count = fixture.protected.len();
    let mut transaction = fixture.vault.begin_transaction().await.unwrap();
    fixture
        .vault
        .release_claim_in_tx(&mut transaction, &target, content_id, &role)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(fixture.protected.len(), protected_count);
}

async fn validation_rejection_acceptance(fixture: &Fixture) {
    rejected_upload(
        fixture,
        "digest",
        sha256(b"expected bytes"),
        b"different bytes".len() as u64,
        "text/plain",
        b"different bytes",
    )
    .await;
    rejected_upload(
        fixture,
        "size",
        sha256(b"wrong size"),
        1,
        "text/plain",
        b"wrong size",
    )
    .await;
    rejected_upload(
        fixture,
        "utf8",
        sha256(&[0xff, 0xfe]),
        2,
        "text/plain",
        &[0xff, 0xfe],
    )
    .await;
    rejected_upload(
        fixture,
        "mime-signature",
        sha256(b"plain text declared as a jpeg"),
        b"plain text declared as a jpeg".len() as u64,
        "image/jpeg",
        b"plain text declared as a jpeg",
    )
    .await;

    // A valid PNG signature with a truncated body exercises decoder rejection separately from
    // media signature detection.
    let truncated_png = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR";
    rejected_upload(
        fixture,
        "decode",
        sha256(truncated_png),
        truncated_png.len() as u64,
        "image/png",
        truncated_png,
    )
    .await;
}

async fn rejected_upload(
    fixture: &Fixture,
    case: &str,
    expected_sha256: String,
    expected_size: u64,
    media_type: &str,
    staged: &[u8],
) {
    let grant = owner("tenant-a", "documents", "file", &format!("rejected-{case}"));
    let session = fixture
        .vault
        .reserve_upload(
            &grant,
            &ReserveUploadRequest::new(
                format!("reject-{case}-reserve"),
                expected_sha256,
                expected_size,
                media_type,
                600,
            ),
        )
        .await
        .unwrap();
    fixture
        .vault
        .stage_upload(&grant, session.session_id(), staged.to_vec())
        .await
        .unwrap();
    let error = fixture
        .vault
        .complete_upload(
            &grant,
            &CompleteUploadRequest::new(session.session_id(), format!("reject-{case}-complete")),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), ContentVaultErrorCode::UploadRejected);
    assert!(fixture.quarantine.contains_value(staged));

    let report = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(1), Duration::zero(), 100)
        .await
        .unwrap();
    assert!(report.cleaned_objects >= 1);
    assert!(!fixture.quarantine.contains_value(staged));
}

async fn protected_orphan_retry_acceptance(fixture: &Fixture) {
    let grant = owner("tenant-a", "documents", "file", "retry");
    let bytes = b"commit retry bytes";
    let session = fixture
        .vault
        .reserve_upload(
            &grant,
            &ReserveUploadRequest::new(
                "retry-reserve",
                sha256(bytes),
                bytes.len() as u64,
                "text/plain",
                600,
            ),
        )
        .await
        .unwrap();
    fixture
        .vault
        .stage_upload(&grant, session.session_id(), bytes.to_vec())
        .await
        .unwrap();

    sqlx::raw_sql(
        "CREATE FUNCTION content_vault.reject_content_insert() RETURNS trigger AS $$ \
         BEGIN RAISE EXCEPTION 'injected content commit failure'; END; $$ LANGUAGE plpgsql; \
         CREATE TRIGGER reject_content_insert BEFORE INSERT ON content_vault.content_objects \
         FOR EACH ROW EXECUTE FUNCTION content_vault.reject_content_insert();",
    )
    .execute(&fixture.pool)
    .await
    .unwrap();
    let request = CompleteUploadRequest::new(session.session_id(), "retry-complete");
    assert_eq!(
        fixture
            .vault
            .complete_upload(&grant, &request)
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::DatabaseUnavailable
    );
    assert!(fixture.protected.contains_value(bytes));
    sqlx::raw_sql(
        "DROP TRIGGER reject_content_insert ON content_vault.content_objects; \
         DROP FUNCTION content_vault.reject_content_insert();",
    )
    .execute(&fixture.pool)
    .await
    .unwrap();

    let descriptor = fixture
        .vault
        .complete_upload(&grant, &request)
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

async fn quarantine_delete_retry_acceptance(fixture: &Fixture) {
    let grant = owner("tenant-a", "documents", "file", "cleanup");
    let bytes = b"cleanup retry bytes";
    let (_, descriptor) = fixture.upload(&grant, bytes, "cleanup").await;
    assert!(fixture.quarantine.contains_value(bytes));
    assert_eq!(
        fixture
            .vault
            .read_content(&grant, descriptor.content_id())
            .await
            .unwrap()
            .bytes(),
        bytes
    );

    fixture.quarantine.set_fail_delete(true);
    let first_report = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(1), Duration::zero(), 100)
        .await
        .unwrap();
    assert!(first_report.failed_objects >= 1);
    assert!(fixture.quarantine.contains_value(bytes));

    fixture.quarantine.set_fail_delete(false);
    let second_report = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(2), Duration::zero(), 100)
        .await
        .unwrap();
    assert!(second_report.cleaned_objects >= 1);
    assert!(!fixture.quarantine.contains_value(bytes));
}

async fn quarantine_grace_acceptance(fixture: &Fixture) {
    let grant = owner("tenant-a", "documents", "file", "cleanup-grace");
    let bytes = b"quarantine grace bytes";
    fixture.upload(&grant, bytes, "cleanup-grace").await;
    assert!(fixture.quarantine.contains_value(bytes));

    fixture
        .vault
        .sweep_terminal_quarantine_at(
            Utc::now() + Duration::seconds(30),
            Duration::seconds(60),
            100,
        )
        .await
        .unwrap();
    assert!(fixture.quarantine.contains_value(bytes));

    fixture
        .vault
        .sweep_terminal_quarantine_at(
            Utc::now() + Duration::seconds(61),
            Duration::seconds(60),
            100,
        )
        .await
        .unwrap();
    assert!(!fixture.quarantine.contains_value(bytes));
}

async fn integrity_acceptance(fixture: &Fixture) {
    let missing_owner = owner("tenant-a", "documents", "file", "missing");
    let missing_bytes = b"missing protected bytes";
    let (_, missing) = fixture
        .upload(&missing_owner, missing_bytes, "missing")
        .await;
    fixture.protected.remove_value(missing_bytes);
    assert_eq!(
        fixture
            .vault
            .read_content(&missing_owner, missing.content_id())
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::IntegrityMissing
    );

    let corrupt_owner = owner("tenant-a", "documents", "file", "corrupt");
    let corrupt_bytes = b"corrupt protected bytes";
    let (_, corrupt) = fixture
        .upload(&corrupt_owner, corrupt_bytes, "corrupt")
        .await;
    fixture.protected.corrupt_value(corrupt_bytes);
    assert_eq!(
        fixture
            .vault
            .read_content(&corrupt_owner, corrupt.content_id())
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::IntegrityMismatch
    );

    let observation_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM content_vault.integrity_observations")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert!(observation_count >= 2);
}

async fn integrity_observation_failure_acceptance(fixture: &Fixture) {
    let grant = owner(
        "tenant-a",
        "documents",
        "file",
        "integrity-observation-failure",
    );
    let bytes = b"missing bytes whose observation cannot be recorded";
    let (_, descriptor) = fixture
        .upload(&grant, bytes, "integrity-observation-failure")
        .await;
    fixture.protected.remove_value(bytes);

    sqlx::raw_sql(
        "CREATE FUNCTION content_vault.reject_integrity_observation() RETURNS trigger AS $$ \
         BEGIN RAISE EXCEPTION 'injected integrity observation failure'; END; $$ LANGUAGE plpgsql; \
         CREATE TRIGGER reject_integrity_observation BEFORE INSERT \
         ON content_vault.integrity_observations FOR EACH ROW \
         EXECUTE FUNCTION content_vault.reject_integrity_observation();",
    )
    .execute(&fixture.pool)
    .await
    .unwrap();

    assert_eq!(
        fixture
            .vault
            .read_content(&grant, descriptor.content_id())
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::DatabaseUnavailable
    );

    sqlx::raw_sql(
        "DROP TRIGGER reject_integrity_observation ON content_vault.integrity_observations; \
         DROP FUNCTION content_vault.reject_integrity_observation();",
    )
    .execute(&fixture.pool)
    .await
    .unwrap();

    assert_eq!(
        fixture
            .vault
            .read_content(&grant, descriptor.content_id())
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::IntegrityMissing
    );
}

async fn tenant_scoping_acceptance(
    fixture: &Fixture,
    original_bytes: &[u8],
    original_content_id: ContentId,
) {
    let tenant_b = owner("tenant-b", "profile", "avatar", "source");
    let protected_before = fixture.protected.len();
    let (_, tenant_b_descriptor) = fixture.upload(&tenant_b, original_bytes, "tenant-b").await;
    assert_ne!(tenant_b_descriptor.content_id(), original_content_id);
    assert_eq!(fixture.protected.len(), protected_before + 1);

    let blob_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM content_vault.blobs WHERE sha256 = $1")
            .bind(sha256(original_bytes))
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(blob_count, 2);
}

async fn expiration_and_no_protected_delete_acceptance(fixture: &Fixture) {
    let expiring_owner = owner("tenant-a", "documents", "file", "expiring");
    let expiring_bytes = b"expiring quarantine bytes";
    let session = fixture
        .vault
        .reserve_upload(
            &expiring_owner,
            &ReserveUploadRequest::new(
                "expiry-reserve",
                sha256(expiring_bytes),
                expiring_bytes.len() as u64,
                "text/plain",
                1,
            ),
        )
        .await
        .unwrap();
    fixture
        .vault
        .stage_upload(
            &expiring_owner,
            session.session_id(),
            expiring_bytes.to_vec(),
        )
        .await
        .unwrap();

    let protected_before = fixture.protected.len();
    let report = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(2), Duration::zero(), 100)
        .await
        .unwrap();
    assert!(report.expired_sessions >= 1);
    assert!(!fixture.quarantine.contains_value(expiring_bytes));
    assert_eq!(fixture.protected.len(), protected_before);

    let release_owner = owner("tenant-a", "documents", "file", "last-claim");
    let release_bytes = b"last claim bytes";
    let (_, descriptor) = fixture
        .upload(&release_owner, release_bytes, "last-claim")
        .await;
    let protected_before_release = fixture.protected.len();
    let mut transaction = fixture.vault.begin_transaction().await.unwrap();
    fixture
        .vault
        .release_claim_in_tx(
            &mut transaction,
            &release_owner,
            descriptor.content_id(),
            &ContentClaimRole::new("source").unwrap(),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        fixture
            .vault
            .describe_content(&release_owner, descriptor.content_id())
            .await
            .unwrap_err()
            .code(),
        ContentVaultErrorCode::NotFound
    );
    assert_eq!(fixture.protected.len(), protected_before_release);
    assert!(fixture.protected.contains_value(release_bytes));
}

async fn bounded_expiration_acceptance(fixture: &Fixture) {
    for index in 0..3 {
        let grant = owner(
            "tenant-a",
            "documents",
            "file",
            &format!("bounded-expiry-{index}"),
        );
        fixture
            .vault
            .reserve_upload(
                &grant,
                &ReserveUploadRequest::new(
                    format!("bounded-expiry-reserve-{index}"),
                    sha256(b"bounded expiry bytes"),
                    20,
                    "text/plain",
                    1,
                ),
            )
            .await
            .unwrap();
    }

    let first = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(2), Duration::zero(), 2)
        .await
        .unwrap();
    assert_eq!(first.expired_sessions, 2);

    let second = fixture
        .vault
        .sweep_terminal_quarantine_at(Utc::now() + Duration::seconds(3), Duration::zero(), 2)
        .await
        .unwrap();
    assert_eq!(second.expired_sessions, 1);
}

fn owner(tenant: &str, module: &str, resource_type: &str, resource_id: &str) -> OwnerGrant {
    owner_as(tenant, module, resource_type, resource_id, "test-actor")
}

fn owner_as(
    tenant: &str,
    module: &str,
    resource_type: &str,
    resource_id: &str,
    actor: &str,
) -> OwnerGrant {
    OwnerGrant::new(
        tenant,
        OwnerRef::new(module, resource_type, resource_id, None).unwrap(),
        actor,
        Uuid::now_v7().to_string(),
    )
    .unwrap()
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
