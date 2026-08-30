use crate::engine::{ContentVault, as_i64, map_store_error, sha256_hex, tenant_storage_token};
use crate::errors::{ContentVaultError, ContentVaultErrorCode};
use crate::public::{
    ContentClaimRole, ContentDescriptor, ContentId, OwnerGrant, ReserveUploadRequest,
    UploadSession, UploadSessionId, validate_idempotency_key,
};
use crate::storage::{STREAMING_ATTEMPT_PREFIX, StoreByteStream};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Duration, Utc};
use futures::{StreamExt, TryStreamExt};
use lenso::host::transaction::{DbPool, LinkedTransaction};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::FromRow;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct StreamingUpload {
    vault: ContentVault,
    grant: OwnerGrant,
    session_id: UploadSessionId,
    next_offset: u64,
    expected_size_bytes: u64,
    expires_at: DateTime<Utc>,
}

impl StreamingUpload {
    pub const fn session_id(&self) -> UploadSessionId {
        self.session_id
    }

    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub const fn expected_size_bytes(&self) -> u64 {
        self.expected_size_bytes
    }

    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    pub async fn commit<R>(self, source: R) -> Result<ContentDescriptor, ContentVaultError>
    where
        R: AsyncRead + Unpin + Send,
    {
        self.vault
            .commit_streaming_upload(&self.grant, self.session_id, self.next_offset, source)
            .await
    }
}

pub struct VerifiedContent {
    descriptor: ContentDescriptor,
    vault: ContentVault,
    row: VerifiedDescriptorRow,
    parts: Vec<BlobPartRow>,
    stream: Option<StoreByteStream>,
    buffered: BytesMut,
    next_part: usize,
    delivered_bytes: u64,
    legacy_chunk: Option<Bytes>,
    failed: bool,
}

impl std::fmt::Debug for VerifiedContent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedContent")
            .field("descriptor", &self.descriptor)
            .field("part_count", &self.parts.len())
            .field("next_part", &self.next_part)
            .field("delivered_bytes", &self.delivered_bytes)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl VerifiedContent {
    pub const fn descriptor(&self) -> &ContentDescriptor {
        &self.descriptor
    }

    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, ContentVaultError> {
        if self.failed {
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::IntegrityMismatch,
                "verified content cursor has already failed",
            ));
        }
        if let Some(chunk) = self.legacy_chunk.take() {
            self.delivered_bytes = chunk.len() as u64;
            return Ok(Some(chunk));
        }
        if self.next_part == self.parts.len() {
            return Ok(None);
        }

        let part = &self.parts[self.next_part];
        let expected_size = part.size_bytes()?;
        let expected_size_usize =
            usize::try_from(expected_size).map_err(|_| ContentVaultError::database())?;
        let is_last = self.next_part + 1 == self.parts.len();
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(ContentVaultError::database)?;

        while self.buffered.len() < expected_size_usize {
            match stream.next().await {
                Some(Ok(bytes)) => self.buffered.extend_from_slice(&bytes),
                Some(Err(error)) => {
                    self.failed = true;
                    return Err(map_store_error(&error));
                }
                None => {
                    self.failed = true;
                    self.vault
                        .observe_streaming_integrity(
                            &self.row,
                            "size_mismatch",
                            None,
                            Some(self.delivered_bytes + self.buffered.len() as u64),
                        )
                        .await?;
                    return Err(ContentVaultError::new(
                        ContentVaultErrorCode::IntegrityMismatch,
                        "protected content ended before its committed descriptor",
                    ));
                }
            }
        }

        if is_last {
            match stream.next().await {
                Some(Ok(bytes)) if !bytes.is_empty() => {
                    self.failed = true;
                    self.vault
                        .observe_streaming_integrity(
                            &self.row,
                            "size_mismatch",
                            None,
                            Some(
                                self.delivered_bytes
                                    + self.buffered.len() as u64
                                    + bytes.len() as u64,
                            ),
                        )
                        .await?;
                    return Err(ContentVaultError::new(
                        ContentVaultErrorCode::IntegrityMismatch,
                        "protected content exceeds its committed descriptor",
                    ));
                }
                Some(Ok(_)) | None => {}
                Some(Err(error)) => {
                    self.failed = true;
                    return Err(map_store_error(&error));
                }
            }
        }

        let chunk = self.buffered.split_to(expected_size_usize).freeze();
        let actual_sha256 = sha256_hex(&chunk);
        if actual_sha256 != part.sha256 {
            self.failed = true;
            self.vault
                .observe_streaming_integrity(
                    &self.row,
                    "digest_mismatch",
                    None,
                    Some(self.delivered_bytes + expected_size),
                )
                .await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::IntegrityMismatch,
                "protected content part does not match its committed digest",
            ));
        }

        self.next_part += 1;
        self.delivered_bytes += expected_size;
        Ok(Some(chunk))
    }
}

impl ContentVault {
    pub async fn reserve_streaming_upload(
        &self,
        grant: &OwnerGrant,
        request: &ReserveUploadRequest,
    ) -> Result<StreamingUpload, ContentVaultError> {
        self.validate_streaming_reservation(request)?;

        let now = Utc::now();
        let expires_at = now + Duration::seconds(i64::from(request.ttl_seconds()));
        let session_id = UploadSessionId::from_uuid(Uuid::now_v7());
        let candidate_content_id = ContentId::from_uuid(Uuid::now_v7());
        let quarantine_key = streaming_session_key(grant.tenant_id(), session_id);
        let receipt = UploadSession::reserved(
            session_id,
            request.expected_sha256().to_owned(),
            request.expected_size_bytes(),
            request.media_type().to_owned(),
            expires_at,
        );
        let scope = streaming_command_scope("reserve", grant)?;
        let digest = streaming_request_digest(&(grant.tenant_id(), grant.owner(), request))?;
        let response = serde_json::to_value(&receipt).map_err(|_| ContentVaultError::database())?;

        let mut transaction = LinkedTransaction::begin(&self.pool)
            .await
            .map_err(|_| ContentVaultError::database())?;
        let inserted = insert_streaming_receipt(
            transaction.sql(),
            &scope,
            request.idempotency_key(),
            grant.tenant_id(),
            &digest,
            &response,
            now,
        )
        .await?;
        let receipt = if inserted {
            sqlx::query(
                "INSERT INTO content_vault.upload_sessions (\
                    session_id, tenant_id, owner_module, owner_resource_type, owner_resource_id, \
                    owner_revision_id, actor_id, correlation_id, expected_sha256, \
                    expected_size_bytes, expected_media_type, quarantine_key, candidate_content_id, \
                    staging_token, state, expires_at, created_at, updated_at, ingestion_mode, \
                    stream_chunk_size_bytes\
                 ) VALUES (\
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
                    $14, 'reserved', $15, $16, $16, 'streaming', $17\
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
            .bind(i64::from(self.streaming_config.chunk_size_bytes))
            .execute(&mut **transaction.sql())
            .await
            .map_err(|_| ContentVaultError::database())?;
            transaction
                .commit()
                .await
                .map_err(|_| ContentVaultError::database())?;
            receipt
        } else {
            let existing = streaming_receipt_in_tx::<UploadSession>(
                transaction.sql(),
                &scope,
                request.idempotency_key(),
                &digest,
            )
            .await;
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            existing?
        };

        self.resume_streaming_upload(grant, receipt.session_id())
            .await
    }

    pub async fn resume_streaming_upload(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
    ) -> Result<StreamingUpload, ContentVaultError> {
        let session = self.streaming_session(grant, session_id).await?;
        let now = Utc::now();
        match session.state.as_str() {
            "rejected" => {
                return Err(ContentVaultError::new(
                    ContentVaultErrorCode::UploadRejected,
                    session
                        .terminal_reason
                        .unwrap_or_else(|| "streaming upload was rejected".to_owned()),
                ));
            }
            "expired" => {
                return Err(ContentVaultError::new(
                    ContentVaultErrorCode::UploadExpired,
                    "streaming upload reservation has expired",
                ));
            }
            "committed" => {}
            "reserved" => {
                if session.expires_at <= now {
                    self.expire_streaming_session(grant, session_id, now)
                        .await?;
                    return Err(ContentVaultError::new(
                        ContentVaultErrorCode::UploadExpired,
                        "streaming upload reservation has expired",
                    ));
                }
            }
            "staging" => {
                let stale = session.staging_started_at.is_some_and(|started_at| {
                    started_at + Duration::seconds(i64::from(self.config.staging_lease_seconds))
                        <= now
                });
                if !stale {
                    return Err(ContentVaultError::conflict(
                        "streaming upload already has an active writer",
                    ));
                }
                if session.expires_at <= now {
                    self.expire_streaming_session(grant, session_id, now)
                        .await?;
                    return Err(ContentVaultError::new(
                        ContentVaultErrorCode::UploadExpired,
                        "streaming upload reservation has expired",
                    ));
                }
            }
            _ => return Err(ContentVaultError::database()),
        }

        let expected_size_bytes = session.expected_size_bytes()?;
        let next_offset = if session.state == "committed" {
            expected_size_bytes
        } else {
            self.streaming_progress(session_id, session.chunk_size_bytes()?)
                .await?
        };
        Ok(StreamingUpload {
            vault: self.clone(),
            grant: grant.clone(),
            session_id,
            next_offset,
            expected_size_bytes,
            expires_at: session.expires_at,
        })
    }

    pub async fn fetch_verified(
        &self,
        grant: &OwnerGrant,
        content_id: ContentId,
    ) -> Result<VerifiedContent, ContentVaultError> {
        let row = self.streaming_descriptor(grant, content_id).await?;
        let descriptor = row.descriptor()?;
        let parts = sqlx::query_as::<_, BlobPartRow>(
            "SELECT part_index, byte_offset, size_bytes, sha256 \
             FROM content_vault.blob_parts \
             WHERE tenant_id = $1 AND blob_id = $2 \
             ORDER BY part_index",
        )
        .bind(&row.tenant_id)
        .bind(row.blob_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;

        if parts.is_empty() {
            let legacy = self.read_content(grant, content_id).await?;
            return Ok(VerifiedContent {
                descriptor,
                vault: self.clone(),
                row,
                parts,
                stream: None,
                buffered: BytesMut::new(),
                next_part: 0,
                delivered_bytes: 0,
                legacy_chunk: Some(Bytes::copy_from_slice(legacy.bytes())),
                failed: false,
            });
        }
        validate_blob_part_manifest(&parts, descriptor.size_bytes())?;

        let read = self
            .protected
            .read_stream(&row.protected_key)
            .await
            .map_err(|error| map_store_error(&error))?;
        let Some(read) = read else {
            self.observe_streaming_integrity(&row, "missing", None, None)
                .await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::IntegrityMissing,
                "protected content is missing",
            ));
        };
        if read.size_bytes() != descriptor.size_bytes() {
            self.observe_streaming_integrity(&row, "size_mismatch", None, Some(read.size_bytes()))
                .await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::IntegrityMismatch,
                "protected content size does not match its committed descriptor",
            ));
        }

        Ok(VerifiedContent {
            descriptor,
            vault: self.clone(),
            row,
            parts,
            stream: Some(read.into_stream()),
            buffered: BytesMut::new(),
            next_part: 0,
            delivered_bytes: 0,
            legacy_chunk: None,
            failed: false,
        })
    }

    async fn commit_streaming_upload<R>(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        expected_offset: u64,
        mut source: R,
    ) -> Result<ContentDescriptor, ContentVaultError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let observed = self.streaming_session(grant, session_id).await?;
        if observed.state == "committed" {
            return self
                .streaming_descriptor_for_session(grant, session_id)
                .await;
        }
        let attempt_token = self
            .claim_streaming_lease(grant, session_id, &observed)
            .await?;
        let heartbeat = StreamingLeaseHeartbeat::start(
            self.pool.clone(),
            session_id,
            attempt_token,
            self.config.staging_lease_seconds,
        );
        let result = self
            .commit_streaming_under_lease(
                grant,
                session_id,
                expected_offset,
                attempt_token,
                &mut source,
            )
            .await;
        drop(heartbeat);
        if result.is_err() {
            self.release_streaming_lease(session_id, attempt_token)
                .await;
        }
        result
    }

    async fn commit_streaming_under_lease<R>(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        expected_offset: u64,
        attempt_token: Uuid,
        source: &mut R,
    ) -> Result<ContentDescriptor, ContentVaultError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let session = self
            .ingest_streaming_source(grant, session_id, expected_offset, attempt_token, source)
            .await?;
        let expected_size = session.expected_size_bytes()?;
        let parts = self.uploaded_streaming_parts(session_id).await?;
        let validation = self
            .validate_streaming_parts(
                &parts,
                expected_size,
                &session.expected_sha256,
                &session.media_type,
            )
            .await;
        if let Err(failure) = validation {
            return match failure {
                StreamingValidationFailure::Rejected(reason) => {
                    self.reject_streaming_upload(session_id, attempt_token, &reason)
                        .await
                }
                StreamingValidationFailure::Unavailable(error) => Err(error),
            };
        }

        let protected_key = streaming_protected_key(grant.tenant_id(), &session.expected_sha256);
        let bytes = streaming_part_source(self.quarantine.clone(), parts.clone());
        self.protected
            .put_stream_immutable(&protected_key, expected_size, bytes)
            .await
            .map_err(|error| map_store_error(&error))?;

        self.commit_streaming_validated(
            grant,
            session_id,
            attempt_token,
            &session,
            &parts,
            &protected_key,
        )
        .await
    }

    async fn ingest_streaming_source<R>(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        expected_offset: u64,
        attempt_token: Uuid,
        source: &mut R,
    ) -> Result<StreamingSessionRow, ContentVaultError>
    where
        R: AsyncRead + Unpin + Send,
    {
        sqlx::query(
            "UPDATE content_vault.upload_parts \
             SET state = 'abandoned', updated_at = $1 \
             WHERE session_id = $2 AND state = 'pending' AND attempt_token <> $3",
        )
        .bind(Utc::now())
        .bind(session_id.as_uuid())
        .bind(attempt_token)
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;

        let session = self.streaming_session(grant, session_id).await?;
        let chunk_size = session.chunk_size_bytes()?;
        let expected_size = session.expected_size_bytes()?;
        let mut offset = self.streaming_progress(session_id, chunk_size).await?;
        if offset != expected_offset {
            return Err(ContentVaultError::conflict(
                "streaming upload progress changed; resume before retrying",
            ));
        }

        while offset < expected_size {
            offset += self
                .write_streaming_part(
                    grant,
                    session_id,
                    attempt_token,
                    offset,
                    expected_size,
                    chunk_size,
                    source,
                )
                .await?;
        }

        let mut extra = [0_u8; 1];
        match source.read(&mut extra).await {
            Ok(0) => {}
            Ok(_) => {
                return Err(ContentVaultError::invalid(
                    "streaming source exceeds the reserved byte size",
                ));
            }
            Err(_) => {
                return Err(ContentVaultError::new(
                    ContentVaultErrorCode::UploadInterrupted,
                    "streaming source ended without a complete boundary",
                ));
            }
        }

        Ok(session)
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_streaming_part<R>(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        attempt_token: Uuid,
        offset: u64,
        expected_size: u64,
        chunk_size: u64,
        source: &mut R,
    ) -> Result<u64, ContentVaultError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let capacity = (expected_size - offset).min(chunk_size);
        let part_index =
            u32::try_from(offset / chunk_size).map_err(|_| ContentVaultError::database())?;
        let part_id = Uuid::now_v7();
        let part_key = streaming_part_key(grant.tenant_id(), session_id, attempt_token, part_index);
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO content_vault.upload_parts (\
                part_id, session_id, part_index, byte_offset, quarantine_key, attempt_token, \
                state, created_at, updated_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $7)",
        )
        .bind(part_id)
        .bind(session_id.as_uuid())
        .bind(i32::try_from(part_index).map_err(|_| ContentVaultError::database())?)
        .bind(as_i64(offset)?)
        .bind(&part_key)
        .bind(attempt_token)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;

        let bytes = match read_exact_part(source, capacity).await {
            Ok(bytes) => bytes,
            Err(error) => {
                self.abandon_streaming_part(part_id).await;
                return Err(error);
            }
        };
        let part_sha256 = sha256_hex(&bytes);
        if let Err(error) = self.quarantine.put_immutable(&part_key, bytes).await {
            let put_error = map_store_error(&error);
            self.clean_unreconciled_streaming_part(part_id, session_id, attempt_token, &part_key)
                .await?;
            return Err(put_error);
        }

        let advanced = sqlx::query(
            "UPDATE content_vault.upload_parts AS parts \
             SET state = 'uploaded', size_bytes = $1, sha256 = $2, \
                 writer_resolved_at = $3, updated_at = $3 \
             FROM content_vault.upload_sessions AS sessions \
             WHERE parts.part_id = $4 AND parts.state = 'pending' \
               AND sessions.session_id = parts.session_id \
               AND sessions.state = 'staging' AND sessions.staging_token = $5",
        )
        .bind(as_i64(capacity)?)
        .bind(&part_sha256)
        .bind(Utc::now())
        .bind(part_id)
        .bind(attempt_token)
        .execute(&self.pool)
        .await;
        match advanced {
            Ok(result) if result.rows_affected() == 1 => Ok(capacity),
            Ok(_) => {
                self.clean_unreconciled_streaming_part(
                    part_id,
                    session_id,
                    attempt_token,
                    &part_key,
                )
                .await?;
                Err(ContentVaultError::conflict(
                    "streaming lease was superseded before part reconciliation",
                ))
            }
            Err(error) => {
                let reconciliation_error = map_part_reconciliation_error(&error);
                self.clean_unreconciled_streaming_part(
                    part_id,
                    session_id,
                    attempt_token,
                    &part_key,
                )
                .await?;
                Err(reconciliation_error)
            }
        }
    }

    async fn claim_streaming_lease(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        observed: &StreamingSessionRow,
    ) -> Result<Uuid, ContentVaultError> {
        let now = Utc::now();
        if observed.state == "rejected" {
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::UploadRejected,
                observed
                    .terminal_reason
                    .clone()
                    .unwrap_or_else(|| "streaming upload was rejected".to_owned()),
            ));
        }
        if observed.state == "expired" || observed.expires_at <= now {
            self.expire_streaming_session(grant, session_id, now)
                .await?;
            return Err(ContentVaultError::new(
                ContentVaultErrorCode::UploadExpired,
                "streaming upload reservation has expired",
            ));
        }
        let stale_before = now - Duration::seconds(i64::from(self.config.staging_lease_seconds));
        let attempt_token = Uuid::now_v7();
        let claimed = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'staging', staging_started_at = $1, staging_token = $2, updated_at = $1 \
             WHERE session_id = $3 AND tenant_id = $4 AND owner_module = $5 \
               AND owner_resource_type = $6 AND owner_resource_id = $7 \
               AND owner_revision_id = $8 AND ingestion_mode = 'streaming' \
               AND staging_token = $9 AND expires_at > $1 \
               AND (state = 'reserved' OR (state = 'staging' AND staging_started_at <= $10))",
        )
        .bind(now)
        .bind(attempt_token)
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .bind(observed.staging_token)
        .bind(stale_before)
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if claimed.rows_affected() != 1 {
            let current = self.streaming_session(grant, session_id).await?;
            if current.state == "committed" {
                return Err(ContentVaultError::conflict(
                    "streaming upload committed while the writer was starting",
                ));
            }
            return Err(ContentVaultError::conflict(
                "streaming upload already has an active writer",
            ));
        }
        Ok(attempt_token)
    }

    async fn release_streaming_lease(&self, session_id: UploadSessionId, token: Uuid) {
        let _ = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'reserved', staging_started_at = NULL, updated_at = $1 \
             WHERE session_id = $2 AND state = 'staging' AND staging_token = $3",
        )
        .bind(Utc::now())
        .bind(session_id.as_uuid())
        .bind(token)
        .execute(&self.pool)
        .await;
    }

    async fn abandon_streaming_part(&self, part_id: Uuid) {
        let _ = sqlx::query(
            "UPDATE content_vault.upload_parts \
             SET state = 'abandoned', writer_resolved_at = $1, updated_at = $1 \
             WHERE part_id = $2 AND state = 'pending'",
        )
        .bind(Utc::now())
        .bind(part_id)
        .execute(&self.pool)
        .await;
    }

    async fn clean_unreconciled_streaming_part(
        &self,
        part_id: Uuid,
        session_id: UploadSessionId,
        attempt_token: Uuid,
        quarantine_key: &str,
    ) -> Result<(), ContentVaultError> {
        // The object-store put cannot share a transaction with PostgreSQL. This is the first
        // durable boundary after a successful or uncertain put, so it deliberately re-arms an
        // earlier missing-object cleanup before deleting the exact attempt key. Until this runs,
        // the nullable writer-resolution marker keeps sweeps eligible instead of recording
        // terminal cleanup success.
        let now = Utc::now();
        let fenced = sqlx::query_scalar::<_, Uuid>(
            "UPDATE content_vault.upload_parts AS parts \
             SET state = 'abandoned', writer_resolved_at = $1, \
                 quarantine_cleanup_succeeded_at = NULL, updated_at = $1 \
             FROM content_vault.upload_sessions AS sessions \
             WHERE parts.part_id = $2 AND parts.session_id = $3 \
               AND parts.attempt_token = $4 AND parts.quarantine_key = $5 \
               AND parts.state IN ('pending', 'abandoned') \
               AND sessions.session_id = parts.session_id \
               AND sessions.ingestion_mode = 'streaming' \
             RETURNING parts.part_id",
        )
        .bind(now)
        .bind(part_id)
        .bind(session_id.as_uuid())
        .bind(attempt_token)
        .bind(quarantine_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if fenced.is_none() {
            // A reconciliation response can be uncertain. Never delete an object once the exact
            // part has reached `uploaded`, because a resumed writer may rely on it.
            return Ok(());
        }

        let deleted = self.quarantine.delete_exact(quarantine_key).await.is_ok();
        let updated = sqlx::query(
            "UPDATE content_vault.upload_parts \
             SET quarantine_cleanup_attempted_at = $1, \
                 quarantine_cleanup_succeeded_at = CASE WHEN $6 THEN $1 ELSE NULL END, \
                 quarantine_cleanup_attempts = quarantine_cleanup_attempts + 1, \
                 updated_at = $1 \
             WHERE part_id = $2 AND session_id = $3 AND attempt_token = $4 \
               AND quarantine_key = $5 AND state = 'abandoned'",
        )
        .bind(Utc::now())
        .bind(part_id)
        .bind(session_id.as_uuid())
        .bind(attempt_token)
        .bind(quarantine_key)
        .bind(deleted)
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if updated.rows_affected() != 1 {
            return Err(ContentVaultError::database());
        }
        if !deleted {
            return Err(ContentVaultError::storage());
        }
        Ok(())
    }

    async fn streaming_session(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
    ) -> Result<StreamingSessionRow, ContentVaultError> {
        sqlx::query_as::<_, StreamingSessionRow>(
            "SELECT session_id, candidate_content_id, staging_token, staging_started_at, \
                    expected_sha256, expected_size_bytes, expected_media_type AS media_type, \
                    state, terminal_reason, expires_at, stream_chunk_size_bytes \
             FROM content_vault.upload_sessions \
             WHERE session_id = $1 AND tenant_id = $2 AND owner_module = $3 \
               AND owner_resource_type = $4 AND owner_resource_id = $5 \
               AND owner_revision_id = $6 AND ingestion_mode = 'streaming'",
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

    async fn streaming_progress(
        &self,
        session_id: UploadSessionId,
        chunk_size: u64,
    ) -> Result<u64, ContentVaultError> {
        let parts = self.uploaded_streaming_parts(session_id).await?;
        let mut offset = 0_u64;
        for (index, part) in parts.iter().enumerate() {
            let expected_index = u32::try_from(index).map_err(|_| ContentVaultError::database())?;
            if part.part_index()? != expected_index || part.byte_offset()? != offset {
                return Err(ContentVaultError::database());
            }
            let size = part.size_bytes()?;
            if size > chunk_size || (size != chunk_size && index + 1 != parts.len()) {
                return Err(ContentVaultError::database());
            }
            offset = offset
                .checked_add(size)
                .ok_or_else(ContentVaultError::database)?;
        }
        Ok(offset)
    }

    async fn uploaded_streaming_parts(
        &self,
        session_id: UploadSessionId,
    ) -> Result<Vec<UploadPartRow>, ContentVaultError> {
        sqlx::query_as::<_, UploadPartRow>(
            "SELECT part_id, part_index, byte_offset, size_bytes, sha256, quarantine_key \
             FROM content_vault.upload_parts \
             WHERE session_id = $1 AND state = 'uploaded' \
             ORDER BY part_index",
        )
        .bind(session_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())
    }

    async fn validate_streaming_parts(
        &self,
        parts: &[UploadPartRow],
        expected_size: u64,
        expected_sha256: &str,
        media_type: &str,
    ) -> Result<(), StreamingValidationFailure> {
        if media_type != "text/plain" {
            return Err(StreamingValidationFailure::rejected(
                "streaming validator does not support this media type",
            ));
        }
        let mut global = Sha256::new();
        let mut utf8 = StreamingTextValidator::default();
        let mut total = 0_u64;
        for part in parts {
            let read = self
                .quarantine
                .read_stream(&part.quarantine_key)
                .await
                .map_err(|error| StreamingValidationFailure::Unavailable(map_store_error(&error)))?
                .ok_or_else(|| {
                    StreamingValidationFailure::Unavailable(ContentVaultError::storage())
                })?;
            let expected_part_size = part
                .size_bytes()
                .map_err(StreamingValidationFailure::Unavailable)?;
            if read.size_bytes() != expected_part_size {
                return Err(StreamingValidationFailure::rejected(
                    "streaming quarantine part size does not match",
                ));
            }
            let mut stream = read.into_stream();
            let mut part_digest = Sha256::new();
            let mut actual_part_size = 0_u64;
            while let Some(bytes) = stream
                .try_next()
                .await
                .map_err(|error| StreamingValidationFailure::Unavailable(map_store_error(&error)))?
            {
                actual_part_size = actual_part_size
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| {
                        StreamingValidationFailure::rejected("streaming part size overflow")
                    })?;
                part_digest.update(&bytes);
                global.update(&bytes);
                utf8.push(&bytes)
                    .map_err(StreamingValidationFailure::Rejected)?;
            }
            if actual_part_size != expected_part_size
                || hex::encode(part_digest.finalize()) != part.sha256.as_deref().unwrap_or_default()
            {
                return Err(StreamingValidationFailure::rejected(
                    "streaming quarantine part digest does not match",
                ));
            }
            total = total.checked_add(actual_part_size).ok_or_else(|| {
                StreamingValidationFailure::rejected("streaming upload size overflow")
            })?;
        }
        utf8.finish()
            .map_err(StreamingValidationFailure::Rejected)?;
        if total != expected_size {
            return Err(StreamingValidationFailure::rejected(
                "streaming byte size does not match the reservation",
            ));
        }
        if hex::encode(global.finalize()) != expected_sha256 {
            return Err(StreamingValidationFailure::rejected(
                "streaming SHA-256 does not match the reservation",
            ));
        }
        Ok(())
    }

    async fn reject_streaming_upload(
        &self,
        session_id: UploadSessionId,
        attempt_token: Uuid,
        reason: &str,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        let reason = reason.chars().take(500).collect::<String>();
        let now = Utc::now();
        let updated = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'rejected', staging_started_at = NULL, terminal_reason = $1, \
                 terminal_at = $2, updated_at = $2 \
             WHERE session_id = $3 AND state = 'staging' AND staging_token = $4",
        )
        .bind(&reason)
        .bind(now)
        .bind(session_id.as_uuid())
        .bind(attempt_token)
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if updated.rows_affected() != 1 {
            return Err(ContentVaultError::conflict(
                "streaming lease changed before rejection was recorded",
            ));
        }
        Err(ContentVaultError::new(
            ContentVaultErrorCode::UploadRejected,
            reason,
        ))
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_streaming_validated(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        attempt_token: Uuid,
        observed: &StreamingSessionRow,
        parts: &[UploadPartRow],
        protected_key: &str,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        let now = Utc::now();
        let mut transaction = LinkedTransaction::begin(&self.pool)
            .await
            .map_err(|_| ContentVaultError::database())?;
        let session = sqlx::query_as::<_, StreamingSessionRow>(
            "SELECT session_id, candidate_content_id, staging_token, staging_started_at, \
                    expected_sha256, expected_size_bytes, expected_media_type AS media_type, \
                    state, terminal_reason, expires_at, stream_chunk_size_bytes \
             FROM content_vault.upload_sessions \
             WHERE session_id = $1 AND tenant_id = $2 AND owner_module = $3 \
               AND owner_resource_type = $4 AND owner_resource_id = $5 \
               AND owner_revision_id = $6 AND ingestion_mode = 'streaming' \
             FOR UPDATE",
        )
        .bind(session_id.as_uuid())
        .bind(grant.tenant_id())
        .bind(grant.owner().module())
        .bind(grant.owner().resource_type())
        .bind(grant.owner().resource_id())
        .bind(grant.owner().revision_for_store())
        .fetch_optional(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?
        .ok_or_else(ContentVaultError::not_found)?;
        if session.state == "committed" {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return self
                .streaming_descriptor_for_session(grant, session_id)
                .await;
        }
        if session.state != "staging"
            || session.staging_token != attempt_token
            || session.expected_sha256 != observed.expected_sha256
            || session.expected_size_bytes != observed.expected_size_bytes
            || session.media_type != observed.media_type
        {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "streaming upload changed before commit",
            ));
        }

        let blob = sqlx::query_as::<_, StreamingBlobRow>(
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
        .bind(protected_key)
        .bind(now)
        .fetch_one(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?;
        if blob.size_bytes != session.expected_size_bytes
            || blob.media_type != session.media_type
            || blob.protected_key != protected_key
        {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "tenant digest is already bound to incompatible blob metadata",
            ));
        }

        for part in parts {
            sqlx::query(
                "INSERT INTO content_vault.blob_parts (\
                    tenant_id, blob_id, part_index, byte_offset, size_bytes, sha256\
                 ) VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (tenant_id, blob_id, part_index) DO NOTHING",
            )
            .bind(grant.tenant_id())
            .bind(blob.blob_id)
            .bind(part.part_index)
            .bind(part.byte_offset)
            .bind(part.size_bytes)
            .bind(&part.sha256)
            .execute(&mut **transaction.sql())
            .await
            .map_err(|_| ContentVaultError::database())?;
        }
        let stored_parts = sqlx::query_as::<_, BlobPartRow>(
            "SELECT part_index, byte_offset, size_bytes, sha256 \
             FROM content_vault.blob_parts \
             WHERE tenant_id = $1 AND blob_id = $2 ORDER BY part_index",
        )
        .bind(grant.tenant_id())
        .bind(blob.blob_id)
        .fetch_all(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?;
        if !parts_match(parts, &stored_parts) {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "tenant digest already has an incompatible streaming manifest",
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
        .execute(&mut **transaction.sql())
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
        .execute(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?;
        let committed = sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'committed', staging_started_at = NULL, committed_content_id = $1, \
                 terminal_at = $2, updated_at = $2 \
             WHERE session_id = $3 AND state = 'staging' AND staging_token = $4",
        )
        .bind(session.candidate_content_id)
        .bind(now)
        .bind(session.session_id)
        .bind(attempt_token)
        .execute(&mut **transaction.sql())
        .await
        .map_err(|_| ContentVaultError::database())?;
        if committed.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|_| ContentVaultError::database())?;
            return Err(ContentVaultError::conflict(
                "streaming lease changed before commit",
            ));
        }
        transaction
            .commit()
            .await
            .map_err(|_| ContentVaultError::database())?;

        let expected_size_bytes = session.expected_size_bytes()?;
        Ok(ContentDescriptor::new(
            ContentId::from_uuid(session.candidate_content_id),
            session.expected_sha256,
            expected_size_bytes,
            session.media_type,
            now,
        ))
    }

    async fn streaming_descriptor(
        &self,
        grant: &OwnerGrant,
        content_id: ContentId,
    ) -> Result<VerifiedDescriptorRow, ContentVaultError> {
        sqlx::query_as::<_, VerifiedDescriptorRow>(
            "SELECT contents.content_id, contents.tenant_id, contents.blob_id, blobs.sha256, \
                    blobs.size_bytes, blobs.media_type, blobs.protected_key, contents.created_at \
             FROM content_vault.content_objects AS contents \
             JOIN content_vault.blobs AS blobs \
               ON blobs.tenant_id = contents.tenant_id AND blobs.blob_id = contents.blob_id \
             JOIN content_vault.content_claims AS claims \
               ON claims.tenant_id = contents.tenant_id AND claims.content_id = contents.content_id \
             WHERE contents.content_id = $1 AND contents.tenant_id = $2 \
               AND claims.owner_module = $3 AND claims.owner_resource_type = $4 \
               AND claims.owner_resource_id = $5 AND claims.owner_revision_id = $6 \
               AND claims.state = 'active' LIMIT 1",
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

    async fn streaming_descriptor_for_session(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
    ) -> Result<ContentDescriptor, ContentVaultError> {
        sqlx::query_as::<_, StreamingDescriptorRow>(
            "SELECT contents.content_id, blobs.sha256, blobs.size_bytes, blobs.media_type, \
                    contents.created_at \
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

    async fn observe_streaming_integrity(
        &self,
        row: &VerifiedDescriptorRow,
        kind: &str,
        actual_sha256: Option<&str>,
        actual_size_bytes: Option<u64>,
    ) -> Result<(), ContentVaultError> {
        let inserted = sqlx::query(
            "INSERT INTO content_vault.integrity_observations (\
                observation_id, tenant_id, content_id, kind, expected_sha256, actual_sha256, \
                expected_size_bytes, actual_size_bytes, observed_at\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(Uuid::now_v7())
        .bind(&row.tenant_id)
        .bind(row.content_id)
        .bind(kind)
        .bind(&row.sha256)
        .bind(actual_sha256)
        .bind(row.size_bytes)
        .bind(actual_size_bytes.map(as_i64).transpose()?)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|_| ContentVaultError::database())?;
        if inserted.rows_affected() != 1 {
            return Err(ContentVaultError::database());
        }
        Ok(())
    }

    async fn expire_streaming_session(
        &self,
        grant: &OwnerGrant,
        session_id: UploadSessionId,
        now: DateTime<Utc>,
    ) -> Result<(), ContentVaultError> {
        sqlx::query(
            "UPDATE content_vault.upload_sessions \
             SET state = 'expired', staging_started_at = NULL, \
                 terminal_reason = 'upload reservation expired', terminal_at = $1, updated_at = $1 \
             WHERE session_id = $2 AND tenant_id = $3 AND owner_module = $4 \
               AND owner_resource_type = $5 AND owner_resource_id = $6 \
               AND owner_revision_id = $7 AND ingestion_mode = 'streaming' \
               AND state IN ('reserved', 'staging') AND expires_at <= $1 \
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

    fn validate_streaming_reservation(
        &self,
        request: &ReserveUploadRequest,
    ) -> Result<(), ContentVaultError> {
        validate_idempotency_key(request.idempotency_key())?;
        if request.expected_size_bytes() == 0
            || request.expected_size_bytes() > self.streaming_config.maximum_upload_size_bytes
        {
            return Err(ContentVaultError::invalid(
                "expected byte size exceeds the configured streaming upload bounds",
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
        if request.media_type() != "text/plain" {
            return Err(ContentVaultError::invalid(
                "streaming V2 currently supports only text/plain",
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
}

async fn read_exact_part<R>(source: &mut R, size: u64) -> Result<Vec<u8>, ContentVaultError>
where
    R: AsyncRead + Unpin + Send,
{
    let size = usize::try_from(size).map_err(|_| ContentVaultError::database())?;
    let mut bytes = vec![0_u8; size];
    let mut read = 0_usize;
    while read < size {
        match source.read(&mut bytes[read..]).await {
            Ok(0) => {
                return Err(ContentVaultError::new(
                    ContentVaultErrorCode::UploadInterrupted,
                    "streaming source ended before the reserved byte size",
                ));
            }
            Ok(count) => read += count,
            Err(_) => {
                return Err(ContentVaultError::new(
                    ContentVaultErrorCode::UploadInterrupted,
                    "streaming source was interrupted",
                ));
            }
        }
    }
    Ok(bytes)
}

fn map_part_reconciliation_error(error: &sqlx::Error) -> ContentVaultError {
    if error
        .as_database_error()
        .is_some_and(|error| error.code().as_deref() == Some("23505"))
    {
        ContentVaultError::conflict("streaming part was already committed by another writer")
    } else {
        ContentVaultError::database()
    }
}

fn streaming_part_source(
    quarantine: Arc<dyn crate::storage::QuarantineStore>,
    parts: Vec<UploadPartRow>,
) -> StoreByteStream {
    Box::pin(async_stream::try_stream! {
        for part in parts {
            let read = quarantine
                .read_stream(&part.quarantine_key)
                .await?
                .ok_or_else(|| crate::storage::StoreError::new(
                    crate::storage::StoreErrorKind::Unavailable,
                    "streaming quarantine part is missing",
                ))?;
            let mut stream = read.into_stream();
            while let Some(bytes) = stream.try_next().await? {
                yield bytes;
            }
        }
    })
}

fn validate_blob_part_manifest(
    parts: &[BlobPartRow],
    expected_size: u64,
) -> Result<(), ContentVaultError> {
    let mut offset = 0_u64;
    for (index, part) in parts.iter().enumerate() {
        if part.part_index()? != u32::try_from(index).map_err(|_| ContentVaultError::database())?
            || part.byte_offset()? != offset
        {
            return Err(ContentVaultError::database());
        }
        offset = offset
            .checked_add(part.size_bytes()?)
            .ok_or_else(ContentVaultError::database)?;
    }
    if offset != expected_size {
        return Err(ContentVaultError::database());
    }
    Ok(())
}

fn parts_match(uploaded: &[UploadPartRow], stored: &[BlobPartRow]) -> bool {
    uploaded.len() == stored.len()
        && uploaded.iter().zip(stored).all(|(left, right)| {
            left.part_index == right.part_index
                && left.byte_offset == right.byte_offset
                && left.size_bytes == Some(right.size_bytes)
                && left.sha256.as_deref() == Some(right.sha256.as_str())
        })
}

enum StreamingValidationFailure {
    Rejected(String),
    Unavailable(ContentVaultError),
}

struct StreamingLeaseHeartbeat {
    task: JoinHandle<()>,
}

impl StreamingLeaseHeartbeat {
    fn start(
        pool: DbPool,
        session_id: UploadSessionId,
        attempt_token: Uuid,
        lease_seconds: u32,
    ) -> Self {
        let interval_seconds = u64::from((lease_seconds / 3).max(1));
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(StdDuration::from_secs(interval_seconds));
            interval.tick().await;
            loop {
                interval.tick().await;
                let now = Utc::now();
                let updated = sqlx::query(
                    "UPDATE content_vault.upload_sessions \
                     SET staging_started_at = $1, updated_at = $1 \
                     WHERE session_id = $2 AND state = 'staging' AND staging_token = $3",
                )
                .bind(now)
                .bind(session_id.as_uuid())
                .bind(attempt_token)
                .execute(&pool)
                .await;
                if !matches!(updated, Ok(result) if result.rows_affected() == 1) {
                    break;
                }
            }
        });
        Self { task }
    }
}

impl Drop for StreamingLeaseHeartbeat {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl StreamingValidationFailure {
    fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected(message.into())
    }
}

#[derive(Debug, Default)]
struct StreamingTextValidator {
    trailing: Vec<u8>,
}

impl StreamingTextValidator {
    fn push(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.contains(&0) {
            return Err("text/plain content contains a NUL byte".to_owned());
        }
        let mut candidate = std::mem::take(&mut self.trailing);
        candidate.extend_from_slice(bytes);
        match std::str::from_utf8(&candidate) {
            Ok(_) => Ok(()),
            Err(error) if error.error_len().is_none() => {
                let valid_up_to = error.valid_up_to();
                self.trailing.extend_from_slice(&candidate[valid_up_to..]);
                if self.trailing.len() > 3 {
                    return Err("text/plain content is not valid UTF-8".to_owned());
                }
                Ok(())
            }
            Err(_) => Err("text/plain content is not valid UTF-8".to_owned()),
        }
    }

    fn finish(self) -> Result<(), String> {
        if self.trailing.is_empty() {
            Ok(())
        } else {
            Err("text/plain content ends with incomplete UTF-8".to_owned())
        }
    }
}

fn streaming_session_key(tenant_id: &str, session_id: UploadSessionId) -> String {
    format!(
        "tenants/{}/streaming-sessions/{session_id}",
        tenant_storage_token(tenant_id)
    )
}

fn streaming_part_key(
    tenant_id: &str,
    session_id: UploadSessionId,
    attempt_token: Uuid,
    part_index: u32,
) -> String {
    format!(
        "{STREAMING_ATTEMPT_PREFIX}/tenants/{}/streaming-sessions/{session_id}/attempts/{attempt_token}/parts/{part_index:08}",
        tenant_storage_token(tenant_id)
    )
}

fn streaming_protected_key(tenant_id: &str, sha256: &str) -> String {
    format!(
        "tenants/{}/sha256/{}/{sha256}",
        tenant_storage_token(tenant_id),
        &sha256[..2]
    )
}

fn streaming_command_scope(
    operation: &str,
    grant: &OwnerGrant,
) -> Result<String, ContentVaultError> {
    let identity = streaming_request_digest(&(grant.tenant_id(), grant.owner()))?;
    Ok(format!("content-vault:streaming-{operation}:{identity}"))
}

fn streaming_request_digest(value: &impl Serialize) -> Result<String, ContentVaultError> {
    serde_json::to_vec(value)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|_| ContentVaultError::invalid("request cannot be canonicalized"))
}

async fn insert_streaming_receipt(
    transaction: &mut lenso::host::transaction::DbTransaction<'_>,
    scope: &str,
    idempotency_key: &str,
    tenant_id: &str,
    digest: &str,
    response: &Value,
    now: DateTime<Utc>,
) -> Result<bool, ContentVaultError> {
    let inserted = sqlx::query(
        "INSERT INTO content_vault.command_receipts (\
            scope, idempotency_key, tenant_id, operation, request_digest, response, created_at\
         ) VALUES ($1, $2, $3, 'reserve_streaming', $4, $5, $6) \
         ON CONFLICT (scope, idempotency_key) DO NOTHING",
    )
    .bind(scope)
    .bind(idempotency_key)
    .bind(tenant_id)
    .bind(digest)
    .bind(response)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|_| ContentVaultError::database())?;
    Ok(inserted.rows_affected() == 1)
}

async fn streaming_receipt_in_tx<T: DeserializeOwned>(
    transaction: &mut lenso::host::transaction::DbTransaction<'_>,
    scope: &str,
    idempotency_key: &str,
    digest: &str,
) -> Result<T, ContentVaultError> {
    let row = sqlx::query_as::<_, StreamingReceiptRow>(
        "SELECT request_digest, response FROM content_vault.command_receipts \
         WHERE scope = $1 AND idempotency_key = $2",
    )
    .bind(scope)
    .bind(idempotency_key)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| ContentVaultError::database())?;
    if row.request_digest != digest {
        return Err(ContentVaultError::conflict(
            "idempotency key was already used with a different request",
        ));
    }
    serde_json::from_value(row.response).map_err(|_| ContentVaultError::database())
}

#[derive(Debug, FromRow)]
struct StreamingSessionRow {
    session_id: Uuid,
    candidate_content_id: Uuid,
    staging_token: Uuid,
    staging_started_at: Option<DateTime<Utc>>,
    expected_sha256: String,
    expected_size_bytes: i64,
    media_type: String,
    state: String,
    terminal_reason: Option<String>,
    expires_at: DateTime<Utc>,
    stream_chunk_size_bytes: Option<i64>,
}

impl StreamingSessionRow {
    fn expected_size_bytes(&self) -> Result<u64, ContentVaultError> {
        u64::try_from(self.expected_size_bytes).map_err(|_| ContentVaultError::database())
    }

    fn chunk_size_bytes(&self) -> Result<u64, ContentVaultError> {
        self.stream_chunk_size_bytes
            .and_then(|value| u64::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or_else(ContentVaultError::database)
    }
}

#[derive(Debug, Clone, FromRow)]
struct UploadPartRow {
    #[allow(dead_code)]
    part_id: Uuid,
    part_index: i32,
    byte_offset: i64,
    size_bytes: Option<i64>,
    sha256: Option<String>,
    quarantine_key: String,
}

impl UploadPartRow {
    fn part_index(&self) -> Result<u32, ContentVaultError> {
        u32::try_from(self.part_index).map_err(|_| ContentVaultError::database())
    }

    fn byte_offset(&self) -> Result<u64, ContentVaultError> {
        u64::try_from(self.byte_offset).map_err(|_| ContentVaultError::database())
    }

    fn size_bytes(&self) -> Result<u64, ContentVaultError> {
        self.size_bytes
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(ContentVaultError::database)
    }
}

#[derive(Debug, Clone, FromRow)]
struct BlobPartRow {
    part_index: i32,
    byte_offset: i64,
    size_bytes: i64,
    sha256: String,
}

impl BlobPartRow {
    fn part_index(&self) -> Result<u32, ContentVaultError> {
        u32::try_from(self.part_index).map_err(|_| ContentVaultError::database())
    }

    fn byte_offset(&self) -> Result<u64, ContentVaultError> {
        u64::try_from(self.byte_offset).map_err(|_| ContentVaultError::database())
    }

    fn size_bytes(&self) -> Result<u64, ContentVaultError> {
        u64::try_from(self.size_bytes).map_err(|_| ContentVaultError::database())
    }
}

#[derive(Debug, FromRow)]
struct StreamingBlobRow {
    blob_id: Uuid,
    size_bytes: i64,
    media_type: String,
    protected_key: String,
}

#[derive(Debug, Clone, FromRow)]
struct VerifiedDescriptorRow {
    content_id: Uuid,
    tenant_id: String,
    blob_id: Uuid,
    sha256: String,
    size_bytes: i64,
    media_type: String,
    protected_key: String,
    created_at: DateTime<Utc>,
}

impl VerifiedDescriptorRow {
    fn descriptor(&self) -> Result<ContentDescriptor, ContentVaultError> {
        Ok(ContentDescriptor::new(
            ContentId::from_uuid(self.content_id),
            self.sha256.clone(),
            u64::try_from(self.size_bytes).map_err(|_| ContentVaultError::database())?,
            self.media_type.clone(),
            self.created_at,
        ))
    }
}

#[derive(Debug, FromRow)]
struct StreamingDescriptorRow {
    content_id: Uuid,
    sha256: String,
    size_bytes: i64,
    media_type: String,
    created_at: DateTime<Utc>,
}

impl StreamingDescriptorRow {
    fn descriptor(self) -> Result<ContentDescriptor, ContentVaultError> {
        Ok(ContentDescriptor::new(
            ContentId::from_uuid(self.content_id),
            self.sha256,
            u64::try_from(self.size_bytes).map_err(|_| ContentVaultError::database())?,
            self.media_type,
            self.created_at,
        ))
    }
}

#[derive(Debug, Deserialize, FromRow)]
struct StreamingReceiptRow {
    request_digest: String,
    response: Value,
}
