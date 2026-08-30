use crate::errors::{ContentVaultError, ContentVaultErrorCode};
use crate::public::{
    CompleteUploadRequest, ContentClaimRole, ContentDescriptor, ContentId, ContentRead, OwnerGrant,
    ReserveUploadRequest, StageOutcome, SweepReport, UploadSession, UploadSessionId,
    validate_idempotency_key,
};
use crate::storage::{
    ContentVaultStores, ImmutablePut, ProtectedStore, QuarantineStore, StoreError, StoreErrorKind,
};
use crate::validation::{BasicContentValidator, ContentValidator};
use chrono::{DateTime, Duration, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentVaultConfig {
    pub maximum_upload_size_bytes: u64,
    pub minimum_reservation_ttl_seconds: u32,
    pub maximum_reservation_ttl_seconds: u32,
    pub staging_lease_seconds: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentVaultStreamingConfig {
    pub maximum_upload_size_bytes: u64,
    pub chunk_size_bytes: u32,
}

pub const CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES: u32 = 8 * 1_024 * 1_024;

impl Default for ContentVaultStreamingConfig {
    fn default() -> Self {
        Self {
            maximum_upload_size_bytes: 1_024 * 1_024 * 1_024,
            chunk_size_bytes: CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES,
        }
    }
}

impl Default for ContentVaultConfig {
    fn default() -> Self {
        Self {
            maximum_upload_size_bytes: 64 * 1024 * 1024,
            minimum_reservation_ttl_seconds: 60,
            maximum_reservation_ttl_seconds: 24 * 60 * 60,
            staging_lease_seconds: 300,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContentVault {
    pub(crate) pool: PgPool,
    pub(crate) quarantine: Arc<dyn QuarantineStore>,
    pub(crate) protected: Arc<dyn ProtectedStore>,
    pub(crate) validator: Arc<dyn ContentValidator>,
    pub(crate) config: ContentVaultConfig,
    pub(crate) streaming_config: ContentVaultStreamingConfig,
    transaction_authority: Arc<()>,
}

#[derive(Debug)]
pub struct ContentVaultTransaction<'a> {
    transaction: Transaction<'a, Postgres>,
    authority: Arc<()>,
}

impl<'a> ContentVaultTransaction<'a> {
    pub fn sql(&mut self) -> &mut Transaction<'a, Postgres> {
        &mut self.transaction
    }

    pub async fn commit(self) -> Result<(), ContentVaultError> {
        self.transaction
            .commit()
            .await
            .map_err(|_| ContentVaultError::database())
    }

    pub async fn rollback(self) -> Result<(), ContentVaultError> {
        self.transaction
            .rollback()
            .await
            .map_err(|_| ContentVaultError::database())
    }
}

impl ContentVault {
    pub fn new(
        pool: PgPool,
        quarantine: Arc<dyn QuarantineStore>,
        protected: Arc<dyn ProtectedStore>,
    ) -> Self {
        Self {
            pool,
            quarantine,
            protected,
            validator: Arc::new(BasicContentValidator),
            config: ContentVaultConfig::default(),
            streaming_config: ContentVaultStreamingConfig::default(),
            transaction_authority: Arc::new(()),
        }
    }

    pub fn from_stores(pool: PgPool, stores: &ContentVaultStores) -> Self {
        Self::new(pool, stores.quarantine(), stores.protected())
    }

    pub async fn begin_transaction(
        &self,
    ) -> Result<ContentVaultTransaction<'_>, ContentVaultError> {
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| ContentVaultError::database())?;
        Ok(ContentVaultTransaction {
            transaction,
            authority: self.transaction_authority.clone(),
        })
    }

    #[must_use]
    pub fn with_validator(mut self, validator: Arc<dyn ContentValidator>) -> Self {
        self.validator = validator;
        self
    }

    pub fn with_config(mut self, config: ContentVaultConfig) -> Result<Self, ContentVaultError> {
        if config.maximum_upload_size_bytes == 0
            || config.minimum_reservation_ttl_seconds == 0
            || config.minimum_reservation_ttl_seconds > config.maximum_reservation_ttl_seconds
            || config.staging_lease_seconds == 0
        {
            return Err(ContentVaultError::invalid(
                "content vault configuration contains invalid zero or inverted bounds",
            ));
        }
        self.config = config;
        Ok(self)
    }

    pub fn with_streaming_config(
        mut self,
        config: ContentVaultStreamingConfig,
    ) -> Result<Self, ContentVaultError> {
        if config.maximum_upload_size_bytes == 0
            || config.chunk_size_bytes != CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES
            || u64::from(config.chunk_size_bytes) > config.maximum_upload_size_bytes
        {
            return Err(ContentVaultError::invalid(
                "streaming configuration must use the fixed 8 MiB chunk size within the upload bound",
            ));
        }
        self.streaming_config = config;
        Ok(self)
    }

    pub async fn reserve_upload(
        &self,
        grant: &OwnerGrant,
        request: &ReserveUploadRequest,
    ) -> Result<UploadSession, ContentVaultError> {
        self.validate_reservation(request)?;

        let now = Utc::now();
        let expires_at = now + Duration::seconds(i64::from(request.ttl_seconds()));
        let session_id = UploadSessionId::from_uuid(Uuid::now_v7());
        let candidate_content_id = ContentId::from_uuid(Uuid::now_v7());
        let quarantine_key = quarantine_key(grant.tenant_id(), session_id);
        let result = UploadSession::reserved(
            session_id,
            request.expected_sha256().to_owned(),
            request.expected_size_bytes(),
            request.media_type().to_owned(),
            expires_at,
        );
        let scope = command_scope("reserve", grant)?;
        let digest = reservation_request_digest(grant, request)?;
        let response = serde_json::to_value(&result).map_err(|_| ContentVaultError::database())?;

        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| ContentVaultError::database())?;
        let inserted = insert_receipt(
            &mut transaction,
            &scope,
            request.idempotency_key(),
            grant.tenant_id(),
            "reserve",
            &digest,
            &response,
            now,
        )
        .await?;
        if !inserted {
            let existing = receipt_in_tx::<UploadSession>(
                &mut transaction,
                &scope,
                request.idempotency_key(),
                &digest,
            )
            .await;
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return existing;
        }

        sqlx::query(
            "INSERT INTO content_vault.upload_sessions (\
                session_id, tenant_id, owner_module, owner_resource_type, owner_resource_id, \
                owner_revision_id, actor_id, correlation_id, expected_sha256, \
                expected_size_bytes, expected_media_type, quarantine_key, candidate_content_id, \
                staging_token, \
                state, expires_at, created_at, updated_at\
             ) VALUES (\
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
                $14, 'reserved', $15, $16, $16\
             )",
        )
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .bind(grant.actor_id())
        .bind(grant.correlation_id())
        .bind(request.expected_sha256())
        .bind(as_i64(request.expected_size_bytes())?)
        .bind(request.media_type())
        .bind(&quarantine_key)
        .bind(candidate_content_id.as_uuid())
        .bind(Uuid::now_v7())
        .bind(expires_at)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ContentVaultError::database())?;

        transaction
            .commit()
            .await
            .map_err(|_| ContentVaultError::database())?;
        Ok(result)
    }

    pub async fn stage_upload(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        bytes: Vec<u8>,
    ) -> Result<StageOutcome, ContentVaultError> {
        if bytes.is_empty() || bytes.len() as u64 > self.config.maximum_upload_size_bytes {
            return Err(ContentVaultError::invalid(
                "staged bytes exceed the configured upload bounds",
            ));
        }

        let now = Utc::now();
        let session = self.fetch_session(grant, session_id).await?;
        let attempt_token = Uuid::now_v7();
        if session.state == "expired" || session.expires_at <= now {
            self.expire_session(grant, session_id, now).await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::UploadExpired,
                "upload reservation has expired",
            ));
        }
        let stale_staging = session.state == "staging"
            && session.staging_started_at.is_some_and(|started_at| {
                started_at + Duration::seconds(i64::from(self.config.staging_lease_seconds)) <= now
            });
        if session.state != "reserved" && !stale_staging {
            return Err(ContentVaultError::conflict(
                "only a reserved upload session can accept staged bytes",
            ));
        }

        let claimed = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'staging', staging_started_at = $1, staging_token = $10, updated_at = $1 \
             WHERE session_id = $2 AND tenant_id = $3 AND owner_module = $4 \
               AND owner_resource_type = $5 AND owner_resource_id = $6 \
               AND owner_revision_id = $7 \
               AND (state = 'reserved' OR (\
                    state = 'staging' AND staging_started_at <= $9\
               )) \
               AND expires_at > $1 \
               AND staging_token = $8 \
             RETURNING session_id",
        )
        .bind(now)
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .bind(session.staging_token)
        .bind(now - Duration::seconds(i64::from(self.config.staging_lease_seconds)))
        .bind(attempt_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if claimed.is_none() {
            return Err(ContentVaultError::conflict(
                "upload session is no longer available for staging",
            ));
        }

        let put_result = self
            .quarantine
            .put_immutable(&session.quarantine_key, bytes)
            .await
            .map(|outcome| match outcome {
                ImmutablePut::Created => StageOutcome::Created,
                ImmutablePut::AlreadyPresent => StageOutcome::AlreadyPresent,
            })
            .map_err(|error| map_store_error(&error));
        let reset = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'reserved', staging_started_at = NULL, updated_at = $1 \
             WHERE session_id = $2 AND state = 'staging' AND staging_token = $3",
        )
        .bind(Utc::now())
        .bind(session_id.as_uuid())
        .bind(attempt_token)
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if reset.rows_affected() != 1 {
            return Err(ContentVaultError::conflict(
                "staging lease was superseded before reconciliation",
            ));
        }
        put_result
    }

    pub async fn complete_upload(
        &self,
        grant: &OwnerGrant,
        request: &CompleteUploadRequest,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        validate_idempotency_key(request.idempotency_key())?;
        let session = self.fetch_session(grant, request.session_id()).await?;
        let scope = command_scope("complete", grant)?;
        let digest = completion_request_digest(grant, request)?;
        if let Some(existing) = self
            .receipt::<ContentDescriptor>(&scope, request.idempotency_key(), &digest)
            .await?
        {
            return Ok(existing);
        }

        match session.state.as_str() {
            "committed" => {
                let descriptor = self
                    .descriptor_for_session_owner(grant, request.session_id())
                    .await?;
                return self
                    .store_completed_receipt(
                        grant,
                        request.idempotency_key(),
                        &scope,
                        &digest,
                        descriptor,
                    )
                    .await;
            }
            "rejected" => {
                return Err(ContentVaultError::new(
                    ContentVaultErrorCode::UploadRejected,
                    session
                        .terminal_reason
                        .unwrap_or_else(|| "upload validation was rejected".to_owned()),
                ));
            }
            "expired" => {
                return Err(ContentVaultError::new(
                    ContentVaultErrorCode::UploadExpired,
                    "upload reservation has expired",
                ));
            }
            "reserved" => {}
            _ => return Err(ContentVaultError::database()),
        }
        if session.expires_at <= Utc::now() {
            self.expire_session(grant, request.session_id(), Utc::now())
                .await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::UploadExpired,
                "upload reservation has expired",
            ));
        }

        let bytes = self
            .quarantine
            .read(&session.quarantine_key)
            .await
            .map_err(|error| map_store_error(&error))?
            .ok_or_else(|| {
                ContentVaultError::new(
                    ContentVaultErrorCode::UploadMissing,
                    "staged upload bytes are missing",
                )
            })?;

        let actual_size = bytes.len() as u64;
        if actual_size != session.expected_size_bytes()? {
            return self
                .reject_upload(
                    grant,
                    request.session_id(),
                    "staged byte size does not match the reservation",
                )
                .await;
        }
        let actual_sha256 = sha256_hex(&bytes);
        if actual_sha256 != session.expected_sha256 {
            return self
                .reject_upload(
                    grant,
                    request.session_id(),
                    "staged SHA-256 does not match the reservation",
                )
                .await;
        }
        if let Err(error) = self.validator.validate(&session.media_type, &bytes) {
            return self
                .reject_upload(grant, request.session_id(), error.message())
                .await;
        }

        let protected_key = protected_key(grant.tenant_id(), &actual_sha256);
        self.protected
            .put_immutable(&protected_key, bytes)
            .await
            .map_err(|error| map_store_error(&error))?;

        self.commit_validated(grant, request, &scope, &digest, &session, &protected_key)
            .await
    }

    pub async fn describe_content(
        &self,
        grant: &OwnerGrant,
        content_id: ContentId,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        self.descriptor_for_active_owner(grant, content_id)
            .await
            .and_then(|row| row.descriptor())
    }

    pub async fn read_content(
        &self,
        grant: &OwnerGrant,
        content_id: ContentId,
    ) -> Result<ContentRead, ContentVaultError> {
        let row = self.descriptor_for_active_owner(grant, content_id).await?;
        if row.size_bytes()? > self.config.maximum_upload_size_bytes {
            return Err(ContentVaultError::invalid(
                "content exceeds the buffered read limit; use fetch_verified",
            ));
        }
        let bytes = self
            .protected
            .read(&row.protected_key)
            .await
            .map_err(|error| map_store_error(&error))?;
        let Some(bytes) = bytes else {
            self.observe_integrity(&row, "missing", None, None).await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::IntegrityMissing,
                "protected content is missing",
            ));
        };

        let actual_size = bytes.len() as u64;
        let actual_sha256 = sha256_hex(&bytes);
        if actual_size != row.size_bytes()? {
            self.observe_integrity(
                &row,
                "size_mismatch",
                Some(&actual_sha256),
                Some(actual_size),
            )
            .await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::IntegrityMismatch,
                "protected content size does not match its committed descriptor",
            ));
        }
        if actual_sha256 != row.sha256 {
            self.observe_integrity(
                &row,
                "digest_mismatch",
                Some(&actual_sha256),
                Some(actual_size),
            )
            .await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::IntegrityMismatch,
                "protected content digest does not match its committed descriptor",
            ));
        }

        Ok(ContentRead::new(row.descriptor()?, bytes))
    }

    pub async fn claim_content_in_tx(
        &self,
        transaction: &mut ContentVaultTransaction<'_>,
        source: &OwnerGrant,
        target: &OwnerGrant,
        content_id: ContentId,
        role: &ContentClaimRole,
    ) -> Result<(), ContentVaultError> {
        if !Arc::ptr_eq(&self.transaction_authority, &transaction.authority) {
            return Err(ContentVaultError::invalid(
                "transaction was not issued by this content vault",
            ));
        }
        if source.tenant_id() != target.tenant_id()
            || source.owner().module() != target.owner().module()
        {
            return Err(ContentVaultError::not_found());
        }

        let source_exists = sqlx::query_scalar::<_, Uuid>(
            "SELECT claim_id FROM content_vault.content_claims \
             WHERE tenant_id = $1 AND content_id = $2 AND owner_module = $3 \
               AND owner_resource_type = $4 AND owner_resource_id = $5 \
               AND owner_revision_id = $6 AND state = 'active' \
             LIMIT 1 FOR SHARE",
        )
        .bind(source.tenant_id())
        .bind(content_id.as_uuid())
        .bind(source.owner().module())
        .bind(source.owner().resource_type())
        .bind(source.owner().resource_id())
        .bind(source.owner().revision_for_store())
        .fetch_optional(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?
        .is_some();
        if !source_exists {
            return Err(ContentVaultError::not_found());
        }

        sqlx::query(
            "INSERT INTO content_vault.content_claims (\
                claim_id, tenant_id, content_id, owner_module, owner_resource_type, \
                owner_resource_id, owner_revision_id, role, state, created_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9) \
             ON CONFLICT (\
                tenant_id, content_id, owner_module, owner_resource_type, owner_resource_id, \
                owner_revision_id, role\
             ) WHERE state = 'active' DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(target.tenant_id())
        .bind(content_id.as_uuid())
        .bind(target.owner().module())
        .bind(target.owner().resource_type())
        .bind(target.owner().resource_id())
        .bind(target.owner().revision_for_store())
        .bind(role.as_str())
        .bind(Utc::now())
        .execute(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?;
        Ok(())
    }

    pub async fn release_claim_in_tx(
        &self,
        transaction: &mut ContentVaultTransaction<'_>,
        grant: &OwnerGrant,
        content_id: ContentId,
        role: &ContentClaimRole,
    ) -> Result<(), ContentVaultError> {
        if !Arc::ptr_eq(&self.transaction_authority, &transaction.authority) {
            return Err(ContentVaultError::invalid(
                "transaction was not issued by this content vault",
            ));
        }
        let updated = sqlx::query(
            "UPDATE content_vault.content_claims \
             SET state = 'released', released_at = $1 \
             WHERE tenant_id = $2 AND content_id = $3 AND owner_module = $4 \
               AND owner_resource_type = $5 AND owner_resource_id = $6 \
               AND owner_revision_id = $7 AND role = $8 AND state = 'active'",
        )
        .bind(Utc::now())
        .bind(grant.tenant_id())
        .bind(content_id.as_uuid())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .bind(role.as_str())
        .execute(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?;
        if updated.rows_affected() == 0 {
            return Err(ContentVaultError::not_found());
        }
        Ok(())
    }

    pub async fn sweep_terminal_quarantine(
        &self,
        grace: Duration,
        limit: u32,
    ) -> Result<SweepReport, ContentVaultError> {
        self.sweep_terminal_quarantine_inner(Utc::now(), grace, limit)
            .await
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "postgres-acceptance"))]
    pub async fn sweep_terminal_quarantine_at(
        &self,
        now: DateTime<Utc>,
        grace: Duration,
        limit: u32,
    ) -> Result<SweepReport, ContentVaultError> {
        self.sweep_terminal_quarantine_inner(now, grace, limit)
            .await
    }

    async fn sweep_terminal_quarantine_inner(
        &self,
        now: DateTime<Utc>,
        grace: Duration,
        limit: u32,
    ) -> Result<SweepReport, ContentVaultError> {
        if grace < Duration::zero() || !(1..=1_000).contains(&limit) {
            return Err(ContentVaultError::invalid(
                "sweep grace must be non-negative and limit must be between 1 and 1000",
            ));
        }

        let staging_cutoff = checked_timestamp_sub(
            now,
            Duration::seconds(i64::from(self.config.staging_lease_seconds)),
        )?;
        let expired = sqlx::query(
            "WITH expirable AS (\
                SELECT session_id \
                FROM content_vault.upload_sessions \
                WHERE state IN ('reserved', 'staging') AND expires_at <= $1 \
                  AND (state = 'reserved' OR staging_started_at <= $2) \
                ORDER BY expires_at, session_id \
                LIMIT $3 \
                FOR UPDATE SKIP LOCKED\
             ) \
             UPDATE content_vault.upload_sessions AS sessions \
             SET state = 'expired', staging_started_at = NULL, \
                 terminal_reason = 'upload reservation expired', \
                 terminal_at = $1, updated_at = $1 \
             FROM expirable \
             WHERE sessions.session_id = expirable.session_id",
        )
        .bind(now)
        .bind(staging_cutoff)
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?
        .rows_affected();

        let cutoff = checked_timestamp_sub(now, grace)?;
        let candidates = sqlx::query_as::<_, SweepRow>(
            "WITH cleanup_candidates AS (\
                SELECT 'session'::text AS object_kind, sessions.session_id AS object_id, \
                       sessions.quarantine_key, \
                       COALESCE(sessions.quarantine_cleanup_attempted_at, sessions.terminal_at) \
                           AS cleanup_order \
                FROM content_vault.upload_sessions AS sessions \
                WHERE sessions.state IN ('committed', 'rejected', 'expired') \
                  AND sessions.terminal_at <= $1 \
                UNION ALL \
                SELECT 'part'::text AS object_kind, parts.part_id AS object_id, \
                       parts.quarantine_key, \
                       COALESCE(\
                           parts.quarantine_cleanup_attempted_at, \
                           CASE WHEN parts.state = 'abandoned' THEN parts.created_at \
                                ELSE sessions.terminal_at END\
                       ) AS cleanup_order \
                FROM content_vault.upload_parts AS parts \
                JOIN content_vault.upload_sessions AS sessions \
                  ON sessions.session_id = parts.session_id \
                WHERE (parts.state = 'abandoned' AND parts.created_at <= $1) \
                   OR (sessions.state IN ('committed', 'rejected', 'expired') \
                       AND sessions.terminal_at <= $1)\
             ) \
             SELECT object_kind, object_id, quarantine_key \
             FROM cleanup_candidates \
             ORDER BY cleanup_order, object_id \
             LIMIT $2",
        )
        .bind(cutoff)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;

        let mut report = SweepReport {
            expired_sessions: expired,
            ..SweepReport::default()
        };
        for candidate in candidates {
            let (deleted, updated) = self
                .clean_quarantine_candidate(&candidate, now, cutoff)
                .await?;
            if deleted {
                report.cleaned_objects += updated;
            } else {
                report.failed_objects += updated;
            }
        }
        Ok(report)
    }

    async fn clean_quarantine_candidate(
        &self,
        candidate: &SweepRow,
        now: DateTime<Utc>,
        cutoff: DateTime<Utc>,
    ) -> Result<(bool, u64), ContentVaultError> {
        let deleted = self
            .quarantine
            .delete_exact(&candidate.quarantine_key)
            .await
            .is_ok();
        let updated = if candidate.object_kind == "session" {
            sqlx::query(
                "UPDATE content_vault.upload_sessions \
                 SET quarantine_cleanup_attempted_at = $1, \
                     quarantine_cleanup_succeeded_at = CASE WHEN $5 THEN $1 \
                                                            ELSE quarantine_cleanup_succeeded_at END, \
                     quarantine_cleanup_attempts = quarantine_cleanup_attempts + 1, \
                     updated_at = $1 \
                 WHERE session_id = $2 AND quarantine_key = $3 \
                   AND state IN ('committed', 'rejected', 'expired') \
                   AND terminal_at <= $4",
            )
            .bind(now)
            .bind(candidate.object_id)
            .bind(&candidate.quarantine_key)
            .bind(cutoff)
            .bind(deleted)
            .execute(&self.pool)
            .await
            .map_err(|_| ContentVaultError::database())?
        } else if candidate.object_kind == "part" {
            sqlx::query(
                "UPDATE content_vault.upload_parts AS parts \
                 SET quarantine_cleanup_attempted_at = $1, \
                     quarantine_cleanup_succeeded_at = CASE WHEN $5 THEN $1 \
                                                            ELSE parts.quarantine_cleanup_succeeded_at END, \
                     quarantine_cleanup_attempts = parts.quarantine_cleanup_attempts + 1, \
                     updated_at = $1 \
                 FROM content_vault.upload_sessions AS sessions \
                 WHERE parts.part_id = $2 AND parts.quarantine_key = $3 \
                   AND sessions.session_id = parts.session_id \
                   AND ((parts.state = 'abandoned' AND parts.created_at <= $4) \
                        OR (sessions.state IN ('committed', 'rejected', 'expired') \
                            AND sessions.terminal_at <= $4))",
            )
            .bind(now)
            .bind(candidate.object_id)
            .bind(&candidate.quarantine_key)
            .bind(cutoff)
            .bind(deleted)
            .execute(&self.pool)
            .await
            .map_err(|_| ContentVaultError::database())?
        } else {
            return Err(ContentVaultError::database());
        };
        Ok((deleted, updated.rows_affected()))
    }

    fn validate_reservation(
        &self,
        request: &ReserveUploadRequest,
    ) -> Result<(), ContentVaultError> {
        validate_idempotency_key(request.idempotency_key())?;
        if request.expected_size_bytes() == 0
            || request.expected_size_bytes() > self.config.maximum_upload_size_bytes
        {
            return Err(ContentVaultError::invalid(
                "expected byte size exceeds the configured upload bounds",
            ));
        }
        if request.expected_sha256().len() != 64
            || hex::decode(request.expected_sha256()).is_err()
            || request
                .expected_sha256()
                .bytes()
                .any(|byte| byte.is_ascii_uppercase())
        {
            return Err(ContentVaultError::invalid(
                "expected SHA-256 must be 64 lowercase hexadecimal characters",
            ));
        }
        if !self.validator.supports(request.media_type()) {
            return Err(ContentVaultError::invalid(
                "media type is not supported by the configured validator",
            ));
        }
        if request.ttl_seconds() < self.config.minimum_reservation_ttl_seconds
            || request.ttl_seconds() > self.config.maximum_reservation_ttl_seconds
        {
            return Err(ContentVaultError::invalid(
                "reservation TTL exceeds the configured bounds",
            ));
        }
        Ok(())
    }

    async fn fetch_session(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
    ) -> Result<UploadRow, ContentVaultError> {
        sqlx::query_as::<_, UploadRow>(
            "SELECT session_id, candidate_content_id, staging_token, staging_started_at, expected_sha256, expected_size_bytes, \
                    expected_media_type AS media_type, quarantine_key, state, terminal_reason, \
                    expires_at \
             FROM content_vault.upload_sessions \
             WHERE session_id = $1 AND tenant_id = $2 AND owner_module = $3 \
               AND owner_resource_type = $4 AND owner_resource_id = $5 \
               AND owner_revision_id = $6",
        )
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?
        .ok_or_else(ContentVaultError::not_found)
    }

    async fn expire_session(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        now: DateTime<Utc>,
    ) -> Result<(), ContentVaultError> {
        sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'expired', staging_started_at = NULL, \
                 terminal_reason = 'upload reservation expired', \
                 terminal_at = $1, updated_at = $1 \
             WHERE session_id = $2 AND tenant_id = $3 AND owner_module = $4 \
               AND owner_resource_type = $5 AND owner_resource_id = $6 \
               AND owner_revision_id = $7 AND state IN ('reserved', 'staging') \
               AND expires_at <= $1 \
               AND (state = 'reserved' OR staging_started_at <= $8)",
        )
        .bind(now)
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .bind(now - Duration::seconds(i64::from(self.config.staging_lease_seconds)))
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        Ok(())
    }

    async fn reject_upload(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        reason: &str,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        let reason = reason.chars().take(500).collect::<String>();
        let now = Utc::now();
        let updated = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'rejected', staging_started_at = NULL, \
                 terminal_reason = $1, terminal_at = $2, updated_at = $2 \
             WHERE session_id = $3 AND tenant_id = $4 AND owner_module = $5 \
               AND owner_resource_type = $6 AND owner_resource_id = $7 \
               AND owner_revision_id = $8 AND state = 'reserved'",
        )
        .bind(&reason)
        .bind(now)
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if updated.rows_affected() != 1 {
            return Err(ContentVaultError::conflict(
                "upload session changed before rejection was recorded",
            ));
        }
        Err(ContentVaultError::new(
            ContentVaultErrorCode::UploadRejected,
            reason,
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_validated(
        &self,
        grant: &OwnerGrant,
        request: &CompleteUploadRequest,
        scope: &str,
        request_digest_value: &str,
        observed_session: &UploadRow,
        protected_key_value: &str,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        let now = Utc::now();
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| ContentVaultError::database())?;
        let session = sqlx::query_as::<_, UploadRow>(
            "SELECT session_id, candidate_content_id, staging_token, staging_started_at, expected_sha256, expected_size_bytes, \
                    expected_media_type AS media_type, quarantine_key, state, terminal_reason, \
                    expires_at \
             FROM content_vault.upload_sessions \
             WHERE session_id = $1 AND tenant_id = $2 AND owner_module = $3 \
               AND owner_resource_type = $4 AND owner_resource_id = $5 \
               AND owner_revision_id = $6 \
             FOR UPDATE",
        )
        .bind(request.session_id().as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| ContentVaultError::database())?
        .ok_or_else(ContentVaultError::not_found)?;

        if session.state == "committed" {
            let descriptor =
                descriptor_for_session_owner_tx(&mut transaction, grant, request.session_id())
                    .await?;
            return finish_existing_completion(
                transaction,
                grant,
                request.idempotency_key(),
                scope,
                request_digest_value,
                descriptor,
            )
            .await;
        }
        if session.state == "rejected" {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::UploadRejected,
                session
                    .terminal_reason
                    .unwrap_or_else(|| "upload validation was rejected".to_owned()),
            ));
        }
        if session.state == "staging" {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "upload bytes are still being staged",
            ));
        }
        if session.state == "expired" || session.expires_at <= now {
            if session.state == "reserved" {
                sqlx::query(
                    "UPDATE content_vault.upload_sessions \
                     SET state = 'expired', staging_started_at = NULL, \
                         terminal_reason = 'upload reservation expired', \
                         terminal_at = $1, updated_at = $1 \
                     WHERE session_id = $2",
                )
                .bind(now)
                .bind(session.session_id)
                .execute(&mut *transaction)
                .await
                .map_err(|_| ContentVaultError::database())?;
                transaction
                    .commit()
                    .await
                    .map_err(|_| ContentVaultError::database())?;
            } else {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| ContentVaultError::database())?;
            }
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::UploadExpired,
                "upload reservation has expired",
            ));
        }
        if session.expected_sha256 != observed_session.expected_sha256
            || session.expected_size_bytes != observed_session.expected_size_bytes
            || session.media_type != observed_session.media_type
            || session.quarantine_key != observed_session.quarantine_key
        {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "upload reservation changed during completion",
            ));
        }

        let descriptor = ContentDescriptor::new(
            ContentId::from_uuid(session.candidate_content_id),
            session.expected_sha256.clone(),
            session.expected_size_bytes()?,
            session.media_type.clone(),
            now,
        );
        let response =
            serde_json::to_value(&descriptor).map_err(|_| ContentVaultError::database())?;
        let inserted = insert_receipt(
            &mut transaction,
            scope,
            request.idempotency_key(),
            grant.tenant_id(),
            "complete",
            request_digest_value,
            &response,
            now,
        )
        .await?;
        if !inserted {
            let existing = receipt_in_tx::<ContentDescriptor>(
                &mut transaction,
                scope,
                request.idempotency_key(),
                request_digest_value,
            )
            .await;
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return existing;
        }

        let blob = sqlx::query_as::<_, BlobRow>(
            "INSERT INTO content_vault.blobs (\
                blob_id, tenant_id, sha256, size_bytes, media_type, protected_key, created_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, sha256) DO UPDATE \
             SET sha256 = content_vault.blobs.sha256 \
             RETURNING blob_id, size_bytes, media_type, protected_key",
        )
        .bind(Uuid::now_v7())
        .bind(grant.tenant_id())
        .bind(&session.expected_sha256)
        .bind(session.expected_size_bytes)
        .bind(&session.media_type)
        .bind(protected_key_value)
        .bind(now)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if blob.size_bytes != session.expected_size_bytes
            || blob.media_type != session.media_type
            || blob.protected_key != protected_key_value
        {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "tenant digest is already bound to incompatible blob metadata",
            ));
        }

        sqlx::query(
            "INSERT INTO content_vault.content_objects (\
                content_id, tenant_id, blob_id, committed_by_owner_module, \
                committed_by_owner_resource_type, committed_by_owner_resource_id, created_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(session.candidate_content_id)
        .bind(grant.tenant_id())
        .bind(blob.blob_id)
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ContentVaultError::database())?;

        sqlx::query(
            "INSERT INTO content_vault.content_claims (\
                claim_id, tenant_id, content_id, owner_module, owner_resource_type, \
                owner_resource_id, owner_revision_id, role, state, created_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active', $9)",
        )
        .bind(Uuid::now_v7())
        .bind(grant.tenant_id())
        .bind(session.candidate_content_id)
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .bind(ContentClaimRole::initial().as_str())
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ContentVaultError::database())?;

        let committed = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'committed', staging_started_at = NULL, \
                 committed_content_id = $1, terminal_at = $2, updated_at = $2 \
             WHERE session_id = $3 AND state = 'reserved'",
        )
        .bind(session.candidate_content_id)
        .bind(now)
        .bind(session.session_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if committed.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "upload session changed before commit was recorded",
            ));
        }

        transaction
            .commit()
            .await
            .map_err(|_| ContentVaultError::database())?;
        Ok(descriptor)
    }

    async fn receipt<T: DeserializeOwned>(
        &self,
        scope: &str,
        idempotency_key: &str,
        expected_digest: &str,
    ) -> Result<Option<T>, ContentVaultError> {
        let row = sqlx::query_as::<_, ReceiptRow>(
            "SELECT request_digest, response \
             FROM content_vault.command_receipts \
             WHERE scope = $1 AND idempotency_key = $2",
        )
        .bind(scope)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        row.map(|row| decode_receipt(row, expected_digest))
            .transpose()
    }

    async fn store_completed_receipt(
        &self,
        grant: &OwnerGrant,
        idempotency_key: &str,
        scope: &str,
        request_digest_value: &str,
        descriptor: ContentDescriptor,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| ContentVaultError::database())?;
        finish_existing_completion(
            transaction,
            grant,
            idempotency_key,
            scope,
            request_digest_value,
            descriptor,
        )
        .await
    }

    async fn descriptor_for_session_owner(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        sqlx::query_as::<_, DescriptorRow>(
            "SELECT contents.content_id, blobs.sha256, blobs.size_bytes, blobs.media_type, \
                    blobs.protected_key, contents.created_at \
             FROM content_vault.upload_sessions AS sessions \
             JOIN content_vault.content_objects AS contents \
               ON contents.tenant_id = sessions.tenant_id \
              AND contents.content_id = sessions.committed_content_id \
             JOIN content_vault.blobs AS blobs \
               ON blobs.tenant_id = contents.tenant_id AND blobs.blob_id = contents.blob_id \
             WHERE sessions.session_id = $1 AND sessions.tenant_id = $2 \
               AND sessions.owner_module = $3 AND sessions.owner_resource_type = $4 \
               AND sessions.owner_resource_id = $5 AND sessions.owner_revision_id = $6 \
               AND sessions.state = 'committed'",
        )
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?
        .ok_or_else(ContentVaultError::not_found)?
        .descriptor()
    }

    async fn descriptor_for_active_owner(
        &self,
        grant: &OwnerGrant,
        content_id: ContentId,
    ) -> Result<DescriptorRow, ContentVaultError> {
        sqlx::query_as::<_, DescriptorRow>(
            "SELECT contents.content_id, blobs.sha256, blobs.size_bytes, blobs.media_type, \
                    blobs.protected_key, contents.created_at \
             FROM content_vault.content_objects AS contents \
             JOIN content_vault.blobs AS blobs \
               ON blobs.tenant_id = contents.tenant_id AND blobs.blob_id = contents.blob_id \
             JOIN content_vault.content_claims AS claims \
               ON claims.tenant_id = contents.tenant_id AND claims.content_id = contents.content_id \
             WHERE contents.content_id = $1 AND contents.tenant_id = $2 \
               AND claims.owner_module = $3 AND claims.owner_resource_type = $4 \
               AND claims.owner_resource_id = $5 AND claims.owner_revision_id = $6 \
               AND claims.state = 'active' \
             LIMIT 1",
        )
        .bind(content_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?
        .ok_or_else(ContentVaultError::not_found)
    }

    async fn observe_integrity(
        &self,
        row: &DescriptorRow,
        kind: &str,
        actual_sha256: Option<&str>,
        actual_size_bytes: Option<u64>,
    ) -> Result<(), ContentVaultError> {
        let actual_size_bytes = actual_size_bytes
            .map(|size| i64::try_from(size).map_err(|_| ContentVaultError::database()))
            .transpose()?;
        let inserted = sqlx::query(
            "INSERT INTO content_vault.integrity_observations (\
                observation_id, tenant_id, content_id, kind, expected_sha256, actual_sha256, \
                expected_size_bytes, actual_size_bytes, observed_at\
             ) \
             SELECT $1, contents.tenant_id, contents.content_id, $2, $3, $4, $5, $6, $7 \
             FROM content_vault.content_objects AS contents \
             WHERE contents.content_id = $8",
        )
        .bind(Uuid::now_v7())
        .bind(kind)
        .bind(&row.sha256)
        .bind(actual_sha256)
        .bind(row.size_bytes)
        .bind(actual_size_bytes)
        .bind(Utc::now())
        .bind(row.content_id)
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if inserted.rows_affected() != 1 {
            return Err(ContentVaultError::database());
        }
        Ok(())
    }
}

fn checked_timestamp_sub(
    timestamp: DateTime<Utc>,
    duration: Duration,
) -> Result<DateTime<Utc>, ContentVaultError> {
    timestamp.checked_sub_signed(duration).ok_or_else(|| {
        ContentVaultError::invalid("sweep grace exceeds the supported timestamp range")
    })
}

async fn finish_existing_completion(
    mut transaction: Transaction<'_, Postgres>,
    grant: &OwnerGrant,
    idempotency_key: &str,
    scope: &str,
    expected_digest: &str,
    descriptor: ContentDescriptor,
) -> Result<ContentDescriptor, ContentVaultError> {
    let response = serde_json::to_value(&descriptor).map_err(|_| ContentVaultError::database())?;
    let inserted = insert_receipt(
        &mut transaction,
        scope,
        idempotency_key,
        grant.tenant_id(),
        "complete",
        expected_digest,
        &response,
        Utc::now(),
    )
    .await?;
    if !inserted {
        let existing = receipt_in_tx::<ContentDescriptor>(
            &mut transaction,
            scope,
            idempotency_key,
            expected_digest,
        )
        .await;
        transaction
            .rollback()
            .await
            .map_err(|_| ContentVaultError::database())?;
        return existing;
    }
    transaction
        .commit()
        .await
        .map_err(|_| ContentVaultError::database())?;
    Ok(descriptor)
}

async fn descriptor_for_session_owner_tx(
    transaction: &mut Transaction<'_, Postgres>,
    grant: &OwnerGrant,
    session_id: UploadSessionId,
) -> Result<ContentDescriptor, ContentVaultError> {
    sqlx::query_as::<_, DescriptorRow>(
        "SELECT contents.content_id, blobs.sha256, blobs.size_bytes, blobs.media_type, \
                blobs.protected_key, contents.created_at \
         FROM content_vault.upload_sessions AS sessions \
         JOIN content_vault.content_objects AS contents \
           ON contents.tenant_id = sessions.tenant_id \
          AND contents.content_id = sessions.committed_content_id \
         JOIN content_vault.blobs AS blobs \
           ON blobs.tenant_id = contents.tenant_id AND blobs.blob_id = contents.blob_id \
         WHERE sessions.session_id = $1 AND sessions.tenant_id = $2 \
           AND sessions.owner_module = $3 AND sessions.owner_resource_type = $4 \
           AND sessions.owner_resource_id = $5 AND sessions.owner_revision_id = $6 \
           AND sessions.state = 'committed'",
    )
    .bind(session_id.as_uuid())
    .bind(grant.tenant_id())
    .bind(grant.owner().module())
    .bind(grant.owner().resource_type())
    .bind(grant.owner().resource_id())
    .bind(grant.owner().revision_for_store())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| ContentVaultError::database())?
    .ok_or_else(ContentVaultError::not_found)?
    .descriptor()
}

#[allow(clippy::too_many_arguments)]
async fn insert_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    scope: &str,
    idempotency_key: &str,
    tenant_id: &str,
    operation: &str,
    request_digest_value: &str,
    response: &Value,
    now: DateTime<Utc>,
) -> Result<bool, ContentVaultError> {
    let result = sqlx::query(
        "INSERT INTO content_vault.command_receipts (\
            scope, idempotency_key, tenant_id, operation, request_digest, response, created_at\
         ) VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (scope, idempotency_key) DO NOTHING",
    )
    .bind(scope)
    .bind(idempotency_key)
    .bind(tenant_id)
    .bind(operation)
    .bind(request_digest_value)
    .bind(response)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|_| ContentVaultError::database())?;
    Ok(result.rows_affected() == 1)
}

async fn receipt_in_tx<T: DeserializeOwned>(
    transaction: &mut Transaction<'_, Postgres>,
    scope: &str,
    idempotency_key: &str,
    expected_digest: &str,
) -> Result<T, ContentVaultError> {
    let row = sqlx::query_as::<_, ReceiptRow>(
        "SELECT request_digest, response \
         FROM content_vault.command_receipts \
         WHERE scope = $1 AND idempotency_key = $2",
    )
    .bind(scope)
    .bind(idempotency_key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| ContentVaultError::database())?;
    decode_receipt(row, expected_digest)
}

fn decode_receipt<T: DeserializeOwned>(
    row: ReceiptRow,
    expected_digest: &str,
) -> Result<T, ContentVaultError> {
    if row.request_digest != expected_digest {
        return Err(ContentVaultError::conflict(
            "idempotency key was already used with a different request",
        ));
    }
    serde_json::from_value(row.response).map_err(|_| ContentVaultError::database())
}

fn command_scope(operation: &str, grant: &OwnerGrant) -> Result<String, ContentVaultError> {
    let identity_digest = request_digest(&(grant.tenant_id(), grant.owner()))?;
    Ok(format!("content-vault:{operation}:{identity_digest}"))
}

fn reservation_request_digest(
    grant: &OwnerGrant,
    request: &ReserveUploadRequest,
) -> Result<String, ContentVaultError> {
    request_digest(&(grant.tenant_id(), grant.owner(), request))
}

fn completion_request_digest(
    grant: &OwnerGrant,
    request: &CompleteUploadRequest,
) -> Result<String, ContentVaultError> {
    request_digest(&(grant.tenant_id(), grant.owner(), request))
}

fn request_digest(value: &impl Serialize) -> Result<String, ContentVaultError> {
    serde_json::to_vec(value)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|_| ContentVaultError::invalid("request cannot be canonicalized"))
}

fn quarantine_key(tenant_id: &str, session_id: UploadSessionId) -> String {
    format!(
        "tenants/{}/sessions/{session_id}",
        tenant_storage_token(tenant_id)
    )
}

fn protected_key(tenant_id: &str, sha256: &str) -> String {
    format!(
        "tenants/{}/sha256/{}/{sha256}",
        tenant_storage_token(tenant_id),
        &sha256[..2]
    )
}

pub(crate) fn tenant_storage_token(tenant_id: &str) -> String {
    sha256_hex(tenant_id.as_bytes())[..32].to_owned()
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(crate) fn as_i64(value: u64) -> Result<i64, ContentVaultError> {
    i64::try_from(value).map_err(|_| ContentVaultError::invalid("byte size is too large"))
}

pub(crate) fn map_store_error(error: &StoreError) -> ContentVaultError {
    match error.kind() {
        StoreErrorKind::ImmutableConflict => {
            ContentVaultError::conflict("immutable storage key already contains different bytes")
        }
        StoreErrorKind::InvalidLength => {
            ContentVaultError::invalid("streamed object length does not match its declaration")
        }
        StoreErrorKind::Unavailable | StoreErrorKind::InvalidKey => ContentVaultError::storage(),
    }
}

#[derive(Debug, FromRow)]
struct UploadRow {
    session_id: Uuid,
    candidate_content_id: Uuid,
    staging_token: Uuid,
    staging_started_at: Option<DateTime<Utc>>,
    expected_sha256: String,
    expected_size_bytes: i64,
    media_type: String,
    quarantine_key: String,
    state: String,
    terminal_reason: Option<String>,
    expires_at: DateTime<Utc>,
}

impl UploadRow {
    fn expected_size_bytes(&self) -> Result<u64, ContentVaultError> {
        u64::try_from(self.expected_size_bytes).map_err(|_| ContentVaultError::database())
    }
}

#[derive(Debug, FromRow)]
struct BlobRow {
    blob_id: Uuid,
    size_bytes: i64,
    media_type: String,
    protected_key: String,
}

#[derive(Debug, FromRow)]
struct DescriptorRow {
    content_id: Uuid,
    sha256: String,
    size_bytes: i64,
    media_type: String,
    protected_key: String,
    created_at: DateTime<Utc>,
}

impl DescriptorRow {
    fn size_bytes(&self) -> Result<u64, ContentVaultError> {
        u64::try_from(self.size_bytes).map_err(|_| ContentVaultError::database())
    }

    fn descriptor(&self) -> Result<ContentDescriptor, ContentVaultError> {
        Ok(ContentDescriptor::new(
            ContentId::from_uuid(self.content_id),
            self.sha256.clone(),
            self.size_bytes()?,
            self.media_type.clone(),
            self.created_at,
        ))
    }
}

#[derive(Debug, FromRow)]
struct ReceiptRow {
    request_digest: String,
    response: Value,
}

#[derive(Debug, FromRow)]
struct SweepRow {
    object_kind: String,
    object_id: Uuid,
    quarantine_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_sweep_timestamp_subtraction_is_bounded_without_panicking() {
        let now = DateTime::parse_from_rfc3339("2026-08-30T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let one_year = Duration::seconds(31_536_000);
        let cutoff = checked_timestamp_sub(now, one_year).unwrap();
        assert_eq!(now - cutoff, one_year);

        let error = checked_timestamp_sub(DateTime::<Utc>::MIN_UTC, Duration::seconds(1))
            .expect_err("timestamp underflow must fail closed instead of panicking");
        assert_eq!(error.code(), ContentVaultErrorCode::InvalidInput);
        assert_eq!(
            error.message(),
            "sweep grace exceeds the supported timestamp range"
        );
    }
}
