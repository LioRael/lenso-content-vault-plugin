//! Plugin-first Lenso provider for `lenso.content-vault@1`.

use crate::{
    ContentClaimRole, ContentDescriptor, ContentId, ContentVault, ContentVaultError,
    ContentVaultErrorCode, ContentVaultStores, OwnerGrant as VaultOwnerGrant, OwnerRef,
    ReserveUploadRequest, UploadSessionId,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Datelike as _, Duration as ChronoDuration, Utc};
use lenso::{
    Ctx, DeactivateContext, Lifecycle, ManagedTasks, PluginError, PluginResult, Port,
    PrepareContext, ProviderStream, ProviderStreamChannel, RuntimeFailure, StreamInput,
};
use lenso_capability_content_vault as capability;
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use object_store::aws::{AmazonS3Builder, S3CopyIfNotExists};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncWriteExt as _, DuplexStream},
    sync::oneshot,
};
use zeroize::Zeroizing;

pub const PLUGIN_ID: &str = "lenso.content-vault";
const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_QUARANTINE_GRACE_SECONDS: u64 = 900;
const MAX_QUARANTINE_GRACE_SECONDS: u64 = 365 * 24 * 60 * 60;
const DEFAULT_SWEEP_BATCH_LIMIT: u32 = 100;
const MAX_SWEEP_BATCH_LIMIT: u32 = 1_000;
const DEFAULT_STREAM_CHANNEL_CAPACITY: usize = 4;
const MAX_STREAM_CHANNEL_CAPACITY: usize = 64;
const MAX_MAINTENANCE_CALLERS: usize = 64;
const UPLOAD_PIPE_CAPACITY_BYTES: usize = 64 * 1_024;
const MAX_STREAM_UPLOAD_SIZE_BYTES: u64 = 1_024 * 1_024 * 1_024;
const MAX_ENCODED_CHUNK_BYTES: usize =
    (crate::CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES as usize).div_ceil(3) * 4;

macro_rules! map_content_error {
    ($error:expr, $domain:ident) => {{
        let error = $error;
        match error.code() {
            ContentVaultErrorCode::InvalidInput => {
                PluginError::Domain(capability::$domain::InvalidInput)
            }
            ContentVaultErrorCode::Conflict => PluginError::Domain(capability::$domain::Conflict),
            ContentVaultErrorCode::NotFound => PluginError::Domain(capability::$domain::NotFound),
            ContentVaultErrorCode::UploadMissing => {
                PluginError::Domain(capability::$domain::UploadMissing)
            }
            ContentVaultErrorCode::UploadInterrupted => {
                PluginError::Domain(capability::$domain::UploadInterrupted)
            }
            ContentVaultErrorCode::UploadRejected => {
                PluginError::Domain(capability::$domain::UploadRejected)
            }
            ContentVaultErrorCode::UploadExpired => {
                PluginError::Domain(capability::$domain::UploadExpired)
            }
            ContentVaultErrorCode::IntegrityMissing => {
                PluginError::Domain(capability::$domain::IntegrityMissing)
            }
            ContentVaultErrorCode::IntegrityMismatch => {
                PluginError::Domain(capability::$domain::IntegrityMismatch)
            }
            ContentVaultErrorCode::StorageUnavailable
            | ContentVaultErrorCode::DatabaseUnavailable => {
                PluginError::Runtime(RuntimeFailure::PluginFailure {
                    detail: "Content Vault dependency is unavailable".to_owned(),
                })
            }
        }
    }};
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContentVaultPluginConfig {
    database_url_secret: String,
    s3_bucket: String,
    s3_region: String,
    #[serde(default)]
    s3_endpoint: Option<String>,
    s3_access_key_id_secret: String,
    s3_secret_access_key_secret: String,
    #[serde(default)]
    s3_session_token_secret: Option<String>,
    #[serde(default)]
    s3_allow_http: bool,
    #[serde(default = "default_quarantine_grace_seconds")]
    quarantine_grace_seconds: u64,
    #[serde(default = "default_sweep_batch_limit")]
    sweep_batch_limit: u32,
    #[serde(default = "default_stream_channel_capacity")]
    stream_channel_capacity: usize,
    maintenance_callers: Vec<String>,
}

impl ContentVaultPluginConfig {
    /// Creates a validated configuration with bounded maintenance and Stream defaults.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_url_secret: impl Into<String>,
        s3_bucket: impl Into<String>,
        s3_region: impl Into<String>,
        s3_access_key_id_secret: impl Into<String>,
        s3_secret_access_key_secret: impl Into<String>,
        maintenance_callers: Vec<String>,
    ) -> Result<Self, ContentVaultConfigError> {
        let config = Self {
            database_url_secret: database_url_secret.into(),
            s3_bucket: s3_bucket.into(),
            s3_region: s3_region.into(),
            s3_endpoint: None,
            s3_access_key_id_secret: s3_access_key_id_secret.into(),
            s3_secret_access_key_secret: s3_secret_access_key_secret.into(),
            s3_session_token_secret: None,
            s3_allow_http: false,
            quarantine_grace_seconds: DEFAULT_QUARANTINE_GRACE_SECONDS,
            sweep_batch_limit: DEFAULT_SWEEP_BATCH_LIMIT,
            stream_channel_capacity: DEFAULT_STREAM_CHANNEL_CAPACITY,
            maintenance_callers,
        };
        config.validate()?;
        Ok(config)
    }

    /// Selects an explicit S3-compatible endpoint and transport policy.
    pub fn with_s3_endpoint(
        mut self,
        endpoint: impl Into<String>,
        allow_http: bool,
    ) -> Result<Self, ContentVaultConfigError> {
        self.s3_endpoint = Some(endpoint.into());
        self.s3_allow_http = allow_http;
        self.validate()?;
        Ok(self)
    }

    /// Selects an optional session-token secret reference.
    pub fn with_s3_session_token_secret(
        mut self,
        reference: impl Into<String>,
    ) -> Result<Self, ContentVaultConfigError> {
        self.s3_session_token_secret = Some(reference.into());
        self.validate()?;
        Ok(self)
    }

    /// Overrides bounded deployment-owned maintenance and Stream limits.
    pub fn with_bounds(
        mut self,
        quarantine_grace_seconds: u64,
        sweep_batch_limit: u32,
        stream_channel_capacity: usize,
    ) -> Result<Self, ContentVaultConfigError> {
        self.quarantine_grace_seconds = quarantine_grace_seconds;
        self.sweep_batch_limit = sweep_batch_limit;
        self.stream_channel_capacity = stream_channel_capacity;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), ContentVaultConfigError> {
        let mut references = vec![
            self.database_url_secret.as_str(),
            self.s3_access_key_id_secret.as_str(),
            self.s3_secret_access_key_secret.as_str(),
        ];
        if let Some(reference) = self.s3_session_token_secret.as_deref() {
            references.push(reference);
        }
        if references
            .iter()
            .any(|reference| !valid_secret_reference(reference))
        {
            return Err(ContentVaultConfigError::InvalidSecretReference);
        }
        if references.iter().copied().collect::<BTreeSet<_>>().len() != references.len() {
            return Err(ContentVaultConfigError::DuplicateSecretReference);
        }
        if self.s3_bucket.trim().is_empty()
            || self.s3_bucket.len() > 255
            || self.s3_region.trim().is_empty()
            || self.s3_region.len() > 100
        {
            return Err(ContentVaultConfigError::InvalidObjectStore);
        }
        if let Some(endpoint) = self.s3_endpoint.as_deref() {
            let parsed =
                url::Url::parse(endpoint).map_err(|_| ContentVaultConfigError::InvalidEndpoint)?;
            let transport_is_exact = match parsed.scheme() {
                "http" => self.s3_allow_http,
                "https" => !self.s3_allow_http,
                _ => false,
            };
            if endpoint.len() > 2_048
                || !transport_is_exact
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(ContentVaultConfigError::InvalidEndpoint);
            }
        } else if self.s3_allow_http {
            return Err(ContentVaultConfigError::InvalidEndpoint);
        }
        if self.quarantine_grace_seconds > MAX_QUARANTINE_GRACE_SECONDS {
            return Err(ContentVaultConfigError::InvalidSweepBounds);
        }
        if !(1..=MAX_SWEEP_BATCH_LIMIT).contains(&self.sweep_batch_limit) {
            return Err(ContentVaultConfigError::InvalidSweepBounds);
        }
        if !(1..=MAX_STREAM_CHANNEL_CAPACITY).contains(&self.stream_channel_capacity) {
            return Err(ContentVaultConfigError::InvalidStreamCapacity);
        }
        if self.maintenance_callers.is_empty()
            || self.maintenance_callers.len() > MAX_MAINTENANCE_CALLERS
            || self
                .maintenance_callers
                .iter()
                .any(|caller| !valid_plugin_instance(caller))
            || self
                .maintenance_callers
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != self.maintenance_callers.len()
        {
            return Err(ContentVaultConfigError::InvalidMaintenanceCallers);
        }
        Ok(())
    }

    fn quarantine_grace(&self) -> ChronoDuration {
        ChronoDuration::seconds(
            i64::try_from(self.quarantine_grace_seconds)
                .expect("configuration validation bounds the grace duration"),
        )
    }
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum ContentVaultConfigError {
    #[error("invalid secret reference")]
    InvalidSecretReference,
    #[error("database and object-store credentials require distinct secret references")]
    DuplicateSecretReference,
    #[error("invalid S3 bucket or region")]
    InvalidObjectStore,
    #[error("S3 HTTP authority must exactly match one explicit endpoint")]
    InvalidEndpoint,
    #[error("invalid terminal-quarantine sweep bounds")]
    InvalidSweepBounds,
    #[error("stream channel capacity must be between 1 and 64")]
    InvalidStreamCapacity,
    #[error("maintenance callers must be a non-empty unique Plugin Instance allowlist")]
    InvalidMaintenanceCallers,
}

const fn default_quarantine_grace_seconds() -> u64 {
    DEFAULT_QUARANTINE_GRACE_SECONDS
}

const fn default_sweep_batch_limit() -> u32 {
    DEFAULT_SWEEP_BATCH_LIMIT
}

const fn default_stream_channel_capacity() -> usize {
    DEFAULT_STREAM_CHANNEL_CAPACITY
}

fn validate_config(config: &ContentVaultPluginConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: error.to_string(),
        })
}

#[lenso::plugin(
    lifecycle,
    configuration_schema = "configuration.schema.json",
    validate = validate_config
)]
#[derive(Clone)]
struct ContentVaultPlugin {
    #[config]
    config: ContentVaultPluginConfig,
    secrets: Port<secrets::SecretsClient>,
    #[tasks]
    tasks: ManagedTasks,
    state: Rc<RefCell<Option<PreparedContentVault>>>,
}

#[derive(Clone)]
struct PreparedContentVault {
    vault: ContentVault,
    operator: crate::operator::ContentVaultOperator,
}

impl fmt::Debug for PreparedContentVault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedContentVault")
            .finish_non_exhaustive()
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for ContentVaultPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentVaultPlugin")
            .field("prepared", &self.state.borrow().is_some())
            .field(
                "maintenance_caller_count",
                &self.config.maintenance_callers.len(),
            )
            .field(
                "stream_channel_capacity",
                &self.config.stream_channel_capacity,
            )
            .finish()
    }
}

#[lenso::provides(capability::ContentVault)]
impl ContentVaultPlugin {
    fn prepared(&self) -> Result<PreparedContentVault, RuntimeFailure> {
        self.state
            .borrow()
            .clone()
            .ok_or_else(|| RuntimeFailure::PluginFailure {
                detail: "Content Vault is not active".to_owned(),
            })
    }

    fn owner_authorized(context: &Ctx, owner: &capability::Owner) -> bool {
        context
            .caller_instance()
            .is_some_and(|caller| caller == owner.plugin_instance)
    }

    fn maintenance_authorized(&self, context: &Ctx) -> bool {
        context.caller_instance().is_some_and(|caller| {
            self.config
                .maintenance_callers
                .iter()
                .any(|allowed| allowed == caller)
        })
    }

    async fn reserve(
        &self,
        context: Ctx,
        request: capability::ReserveRequest,
    ) -> PluginResult<capability::ReserveResponse, capability::ReserveError> {
        let grant =
            vault_grant(&request.grant).map_err(|error| map_content_error!(error, ReserveError))?;
        if !Self::owner_authorized(&context, &request.grant.owner) {
            return Err(PluginError::Domain(capability::ReserveError::Unauthorized));
        }
        let expected_size_bytes = u64::try_from(request.expected_size_bytes)
            .map_err(|_| PluginError::Domain(capability::ReserveError::InvalidInput))?;
        let ttl_seconds = u32::try_from(request.ttl_seconds)
            .map_err(|_| PluginError::Domain(capability::ReserveError::InvalidInput))?;
        validate_reserve_boundary(
            &request.idempotency_key,
            &request.expected_sha256,
            expected_size_bytes,
            &request.media_type,
            ttl_seconds,
        )
        .map_err(|error| map_content_error!(error, ReserveError))?;
        let reservation = ReserveUploadRequest::new(
            request.idempotency_key,
            request.expected_sha256,
            expected_size_bytes,
            request.media_type,
            ttl_seconds,
        );
        let prepared = self.prepared().map_err(PluginError::Runtime)?;
        let upload = prepared
            .vault
            .reserve_streaming_upload(&grant, &reservation)
            .await
            .map_err(|error| map_content_error!(error, ReserveError))?;
        let expires_at = contract_timestamp(upload.expires_at()).map_err(PluginError::Runtime)?;
        Ok(capability::ReserveResponse {
            session_id: contract_uuid(upload.session_id().as_uuid())
                .map_err(PluginError::Runtime)?,
            state: capability::UploadState::Reserved,
            expected_sha256: reservation.expected_sha256().to_owned(),
            expected_size_bytes: checked_i64(reservation.expected_size_bytes())
                .map_err(PluginError::Runtime)?,
            media_type: reservation.media_type().to_owned(),
            expires_at,
            next_offset: bounded_upload_offset(upload.next_offset())
                .map_err(PluginError::Runtime)?,
        })
    }

    async fn describe(
        &self,
        context: Ctx,
        request: capability::DescribeRequest,
    ) -> PluginResult<capability::DescribeResponse, capability::DescribeError> {
        let grant = vault_grant(&request.grant)
            .map_err(|error| map_content_error!(error, DescribeError))?;
        let content_id = parse_content_id(&request.content_id)
            .map_err(|error| map_content_error!(error, DescribeError))?;
        if !Self::owner_authorized(&context, &request.grant.owner) {
            return Err(PluginError::Domain(capability::DescribeError::Unauthorized));
        }
        let prepared = self.prepared().map_err(PluginError::Runtime)?;
        let descriptor = prepared
            .vault
            .describe_content(&grant, content_id)
            .await
            .map_err(|error| map_content_error!(error, DescribeError))?;
        Ok(capability::DescribeResponse {
            content: capability_descriptor(&descriptor).map_err(PluginError::Runtime)?,
        })
    }

    async fn claim(
        &self,
        context: Ctx,
        request: capability::ClaimRequest,
    ) -> PluginResult<capability::ClaimResponse, capability::ClaimError> {
        let source =
            vault_grant(&request.grant).map_err(|error| map_content_error!(error, ClaimError))?;
        let target = target_grant(&request.grant, &request.target)
            .map_err(|error| map_content_error!(error, ClaimError))?;
        let content_id = parse_content_id(&request.content_id)
            .map_err(|error| map_content_error!(error, ClaimError))?;
        let role = ContentClaimRole::new(request.role)
            .map_err(|error| map_content_error!(error, ClaimError))?;
        if !Self::owner_authorized(&context, &request.grant.owner)
            || !Self::owner_authorized(&context, &request.target)
        {
            return Err(PluginError::Domain(capability::ClaimError::Unauthorized));
        }
        let prepared = self.prepared().map_err(PluginError::Runtime)?;
        let mut transaction = prepared
            .vault
            .begin_transaction()
            .await
            .map_err(|error| map_content_error!(error, ClaimError))?;
        prepared
            .vault
            .claim_content_in_tx(&mut transaction, &source, &target, content_id, &role)
            .await
            .map_err(|error| map_content_error!(error, ClaimError))?;
        transaction
            .commit()
            .await
            .map_err(|error| map_content_error!(error, ClaimError))?;
        Ok(capability::ClaimResponse { active: true })
    }

    async fn release_claim(
        &self,
        context: Ctx,
        request: capability::ReleaseClaimRequest,
    ) -> PluginResult<capability::ReleaseClaimResponse, capability::ReleaseClaimError> {
        let _source = vault_grant(&request.grant)
            .map_err(|error| map_content_error!(error, ReleaseClaimError))?;
        let target = target_grant(&request.grant, &request.target)
            .map_err(|error| map_content_error!(error, ReleaseClaimError))?;
        let content_id = parse_content_id(&request.content_id)
            .map_err(|error| map_content_error!(error, ReleaseClaimError))?;
        let role = ContentClaimRole::new(request.role)
            .map_err(|error| map_content_error!(error, ReleaseClaimError))?;
        if !Self::owner_authorized(&context, &request.grant.owner)
            || !Self::owner_authorized(&context, &request.target)
        {
            return Err(PluginError::Domain(
                capability::ReleaseClaimError::Unauthorized,
            ));
        }
        let prepared = self.prepared().map_err(PluginError::Runtime)?;
        let mut transaction = prepared
            .vault
            .begin_transaction()
            .await
            .map_err(|error| map_content_error!(error, ReleaseClaimError))?;
        prepared
            .vault
            .release_claim_in_tx(&mut transaction, &target, content_id, &role)
            .await
            .map_err(|error| map_content_error!(error, ReleaseClaimError))?;
        transaction
            .commit()
            .await
            .map_err(|error| map_content_error!(error, ReleaseClaimError))?;
        Ok(capability::ReleaseClaimResponse { released: true })
    }

    async fn sweep(
        &self,
        context: Ctx,
        _request: capability::SweepRequest,
    ) -> PluginResult<capability::SweepResponse, capability::SweepError> {
        if !self.maintenance_authorized(&context) {
            return Err(PluginError::Domain(capability::SweepError::Unauthorized));
        }
        let prepared = self.prepared().map_err(PluginError::Runtime)?;
        let report = prepared
            .vault
            .sweep_terminal_quarantine(
                self.config.quarantine_grace(),
                self.config.sweep_batch_limit,
            )
            .await
            .map_err(|error| map_content_error!(error, SweepError))?;
        Ok(capability::SweepResponse {
            expired_sessions: bounded_sweep_count(report.expired_sessions)
                .map_err(PluginError::Runtime)?,
            cleaned_objects: bounded_sweep_count(report.cleaned_objects)
                .map_err(PluginError::Runtime)?,
            failed_objects: bounded_sweep_count(report.failed_objects)
                .map_err(PluginError::Runtime)?,
        })
    }

    async fn upload(
        &self,
        context: Ctx,
        request: capability::UploadRequest,
    ) -> PluginResult<ProviderStream<capability::ContentVaultUpload>, capability::UploadError> {
        let grant =
            vault_grant(&request.grant).map_err(|error| map_content_error!(error, UploadError))?;
        let session_id = parse_session_id(&request.session_id)
            .map_err(|error| map_content_error!(error, UploadError))?;
        if !Self::owner_authorized(&context, &request.grant.owner) {
            return Err(PluginError::Domain(capability::UploadError::Unauthorized));
        }
        let prepared = self.prepared().map_err(PluginError::Runtime)?;
        let upload = prepared
            .vault
            .resume_streaming_upload(&grant, session_id)
            .await
            .map_err(|error| map_content_error!(error, UploadError))?;
        let (stream, channel) = ProviderStream::<capability::ContentVaultUpload>::channel(
            &context,
            self.config.stream_channel_capacity,
        );
        self.tasks
            .spawn_local(run_upload(channel, upload))
            .map_err(|_| PluginError::Runtime(inactive_task_scope()))?;
        Ok(stream)
    }

    async fn download(
        &self,
        context: Ctx,
        request: capability::DownloadRequest,
    ) -> PluginResult<ProviderStream<capability::ContentVaultDownload>, capability::DownloadError>
    {
        let grant = vault_grant(&request.grant)
            .map_err(|error| map_content_error!(error, DownloadError))?;
        let content_id = parse_content_id(&request.content_id)
            .map_err(|error| map_content_error!(error, DownloadError))?;
        if !Self::owner_authorized(&context, &request.grant.owner) {
            return Err(PluginError::Domain(capability::DownloadError::Unauthorized));
        }
        let prepared = self.prepared().map_err(PluginError::Runtime)?;
        let verified = prepared
            .vault
            .fetch_verified(&grant, content_id)
            .await
            .map_err(|error| map_content_error!(error, DownloadError))?;
        let (stream, channel) = ProviderStream::<capability::ContentVaultDownload>::channel(
            &context,
            self.config.stream_channel_capacity,
        );
        self.tasks
            .spawn_local(run_download(channel, verified))
            .map_err(|_| PluginError::Runtime(inactive_task_scope()))?;
        Ok(stream)
    }
}

impl Lifecycle for ContentVaultPlugin {
    async fn prepare(&self, context: PrepareContext) -> Result<(), RuntimeFailure> {
        let config = self.config.clone();
        let dependencies = context.dependencies().clone();
        let cancellation = context.cancellation();
        let secrets = SecretsClient::from_dependencies(&dependencies)?;
        let database_invocation =
            dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation.clone())?;
        let database_url =
            resolve_secret(&secrets, database_invocation, &config.database_url_secret).await?;
        let access_key_invocation =
            dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation.clone())?;
        let access_key = resolve_secret(
            &secrets,
            access_key_invocation,
            &config.s3_access_key_id_secret,
        )
        .await?;
        let secret_access_key_invocation =
            dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation.clone())?;
        let secret_access_key = resolve_secret(
            &secrets,
            secret_access_key_invocation,
            &config.s3_secret_access_key_secret,
        )
        .await?;
        let session_token = if let Some(reference) = config.s3_session_token_secret.as_deref() {
            let invocation =
                dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
            Some(resolve_secret(&secrets, invocation, reference).await?)
        } else {
            None
        };

        let stores = explicit_s3_stores(
            &config,
            access_key.as_str(),
            secret_access_key.as_str(),
            session_token.as_ref().map(|token| token.as_str()),
        )?;
        let operator = crate::operator::ContentVaultOperator::connect(database_url.as_str())
            .await
            .map_err(|_| RuntimeFailure::PluginFailure {
                detail: "Content Vault requires an already-installed current PostgreSQL schema"
                    .to_owned(),
            })?;
        let vault = ContentVault::from_stores(operator.pool().clone(), &stores);
        self.state
            .replace(Some(PreparedContentVault { vault, operator }));
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.state.borrow_mut().take();
        if let Some(prepared) = prepared {
            let PreparedContentVault { operator, .. } = prepared;
            operator.close().await;
        }
        Ok(())
    }
}

async fn run_upload(
    mut channel: ProviderStreamChannel<capability::ContentVaultUpload>,
    upload: crate::StreamingUpload,
) {
    let result = drive_upload(&mut channel, upload).await;
    let _ = channel.complete(result).await;
}

pub(crate) async fn drive_upload(
    channel: &mut ProviderStreamChannel<capability::ContentVaultUpload>,
    upload: crate::StreamingUpload,
) -> PluginResult<(), capability::UploadError> {
    let next_offset = upload.next_offset();
    let (reader, writer) = tokio::io::duplex(UPLOAD_PIPE_CAPACITY_BYTES);
    let (protocol_success, protocol_confirmation) = oneshot::channel();
    let descriptor = {
        let feeding = feed_upload(channel, writer, next_offset, protocol_success);
        let committing = upload.commit_after_protocol(reader, protocol_confirmation);
        tokio::pin!(feeding);
        tokio::pin!(committing);
        tokio::select! {
            feed_result = &mut feeding => {
                match feed_result {
                    Ok(()) => committing
                        .await
                        .map_err(|error| map_content_error!(error, UploadError))?,
                    Err(error) => {
                        let _ = committing.await;
                        return Err(error);
                    }
                }
            }
            commit_result = &mut committing => {
                commit_result.map_err(|error| map_content_error!(error, UploadError))?
            }
        }
    };
    channel
        .send(capability::UploadFrame {
            kind: capability::UploadFrameKind::Committed,
            offset: Some(
                bounded_upload_offset(descriptor.size_bytes()).map_err(PluginError::Runtime)?,
            ),
            bytes_base64: None,
            content: Some(Some(
                capability_descriptor(&descriptor).map_err(PluginError::Runtime)?,
            )),
        })
        .await
        .map_err(PluginError::Runtime)
}

async fn feed_upload(
    channel: &mut ProviderStreamChannel<capability::ContentVaultUpload>,
    mut writer: DuplexStream,
    mut expected_offset: u64,
    protocol_success: oneshot::Sender<()>,
) -> PluginResult<(), capability::UploadError> {
    let mut protocol_success = Some(protocol_success);
    loop {
        match channel.receive().await.map_err(PluginError::Runtime)? {
            StreamInput::PeerHalfClosed => {
                writer.shutdown().await.map_err(|_| {
                    PluginError::Runtime(RuntimeFailure::PluginFailure {
                        detail: "Content Vault upload pipe failed".to_owned(),
                    })
                })?;
                protocol_success
                    .take()
                    .expect("protocol success sender is consumed once")
                    .send(())
                    .map_err(|()| {
                        PluginError::Runtime(RuntimeFailure::PluginFailure {
                            detail: "Content Vault upload commit gate closed".to_owned(),
                        })
                    })?;
                return Ok(());
            }
            StreamInput::Message(frame) => {
                if frame.kind != capability::UploadFrameKind::Chunk
                    || frame.content.flatten().is_some()
                {
                    return Err(PluginError::Domain(capability::UploadError::InvalidInput));
                }
                let offset = frame
                    .offset
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or(PluginError::Domain(capability::UploadError::InvalidInput))?;
                if offset > MAX_STREAM_UPLOAD_SIZE_BYTES {
                    return Err(PluginError::Domain(capability::UploadError::InvalidInput));
                }
                if offset != expected_offset {
                    return Err(PluginError::Domain(capability::UploadError::Conflict));
                }
                let encoded = frame
                    .bytes_base64
                    .flatten()
                    .ok_or(PluginError::Domain(capability::UploadError::InvalidInput))?;
                if encoded.is_empty() || encoded.len() > MAX_ENCODED_CHUNK_BYTES {
                    return Err(PluginError::Domain(capability::UploadError::InvalidInput));
                }
                let bytes = STANDARD
                    .decode(encoded)
                    .map_err(|_| PluginError::Domain(capability::UploadError::InvalidInput))?;
                if bytes.is_empty()
                    || bytes.len() > crate::CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES as usize
                {
                    return Err(PluginError::Domain(capability::UploadError::InvalidInput));
                }
                writer.write_all(&bytes).await.map_err(|_| {
                    PluginError::Runtime(RuntimeFailure::PluginFailure {
                        detail: "Content Vault upload pipe failed".to_owned(),
                    })
                })?;
                expected_offset = expected_offset
                    .checked_add(bytes.len() as u64)
                    .ok_or(PluginError::Domain(capability::UploadError::InvalidInput))?;
            }
        }
    }
}

async fn run_download(
    mut channel: ProviderStreamChannel<capability::ContentVaultDownload>,
    mut verified: crate::VerifiedContent,
) {
    let result = drive_download(&mut channel, &mut verified).await;
    let _ = channel.complete(result).await;
}

async fn drive_download(
    channel: &mut ProviderStreamChannel<capability::ContentVaultDownload>,
    verified: &mut crate::VerifiedContent,
) -> PluginResult<(), capability::DownloadError> {
    let descriptor = capability_descriptor(verified.descriptor()).map_err(PluginError::Runtime)?;
    channel
        .send(capability::DownloadFrame {
            kind: capability::DownloadFrameKind::Descriptor,
            offset: Some(0),
            bytes_base64: None,
            content: Some(Some(descriptor)),
        })
        .await
        .map_err(PluginError::Runtime)?;
    let mut offset = 0_u64;
    while let Some(bytes) = verified
        .next_chunk()
        .await
        .map_err(|error| map_content_error!(error, DownloadError))?
    {
        if bytes.is_empty()
            || bytes.len() > crate::CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES as usize
        {
            return Err(PluginError::Runtime(invalid_runtime_value()));
        }
        channel
            .send(capability::DownloadFrame {
                kind: capability::DownloadFrameKind::Chunk,
                offset: Some(bounded_upload_offset(offset).map_err(PluginError::Runtime)?),
                bytes_base64: Some(Some(STANDARD.encode(&bytes))),
                content: None,
            })
            .await
            .map_err(PluginError::Runtime)?;
        offset = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| PluginError::Runtime(invalid_runtime_value()))?;
    }
    Ok(())
}

fn vault_grant(grant: &capability::OwnerGrant) -> Result<VaultOwnerGrant, ContentVaultError> {
    VaultOwnerGrant::new(
        grant.tenant_id.clone(),
        vault_owner(&grant.owner)?,
        grant.actor_id.clone(),
        grant.correlation_id.clone(),
    )
}

fn validate_reserve_boundary(
    idempotency_key: &str,
    expected_sha256: &str,
    expected_size_bytes: u64,
    media_type: &str,
    ttl_seconds: u32,
) -> Result<(), ContentVaultError> {
    crate::public::validate_idempotency_key(idempotency_key)?;
    if expected_sha256.len() != 64
        || expected_sha256
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(ContentVaultError::invalid(
            "expected SHA-256 must be 64 lowercase hexadecimal characters",
        ));
    }
    if !(1..=MAX_STREAM_UPLOAD_SIZE_BYTES).contains(&expected_size_bytes) {
        return Err(ContentVaultError::invalid(
            "expected byte size exceeds the Contract bounds",
        ));
    }
    if media_type != "text/plain" {
        return Err(ContentVaultError::invalid(
            "streaming upload media type is outside the Contract",
        ));
    }
    if !(60..=86_400).contains(&ttl_seconds) {
        return Err(ContentVaultError::invalid(
            "reservation TTL exceeds the Contract bounds",
        ));
    }
    Ok(())
}

fn target_grant(
    source: &capability::OwnerGrant,
    target: &capability::Owner,
) -> Result<VaultOwnerGrant, ContentVaultError> {
    VaultOwnerGrant::new(
        source.tenant_id.clone(),
        vault_owner(target)?,
        source.actor_id.clone(),
        source.correlation_id.clone(),
    )
}

fn vault_owner(owner: &capability::Owner) -> Result<OwnerRef, ContentVaultError> {
    if !valid_capability_plugin_instance(&owner.plugin_instance) {
        return Err(ContentVaultError::invalid(
            "owner Plugin Instance is outside the Contract",
        ));
    }
    OwnerRef::new(
        owner.plugin_instance.clone(),
        owner.resource_type.clone(),
        owner.resource_id.clone(),
        owner.revision_id.clone().flatten(),
    )
}

fn valid_capability_plugin_instance(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && value.len() <= 200
        && bytes.all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

fn parse_content_id(value: &str) -> Result<ContentId, ContentVaultError> {
    parse_contract_uuid(value)
        .map(ContentId::from_uuid)
        .ok_or_else(|| {
            ContentVaultError::new(ContentVaultErrorCode::InvalidInput, "invalid content id")
        })
}

fn parse_session_id(value: &str) -> Result<UploadSessionId, ContentVaultError> {
    parse_contract_uuid(value)
        .map(UploadSessionId::from_uuid)
        .ok_or_else(|| {
            ContentVaultError::new(
                ContentVaultErrorCode::InvalidInput,
                "invalid upload session id",
            )
        })
}

fn parse_contract_uuid(value: &str) -> Option<uuid::Uuid> {
    let bytes = value.as_bytes();
    let contract_shape = bytes.len() == 36
        && [8, 13, 18, 23]
            .iter()
            .all(|index| bytes.get(*index) == Some(&b'-'))
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 8 | 13 | 18 | 23) || byte.is_ascii_hexdigit())
        && matches!(bytes[14], b'1'..=b'8')
        && matches!(bytes[19], b'8' | b'9' | b'A' | b'B' | b'a' | b'b');
    if !contract_shape {
        return None;
    }
    uuid::Uuid::parse_str(value).ok()
}

pub(crate) fn capability_descriptor(
    descriptor: &ContentDescriptor,
) -> Result<capability::ContentDescriptor, RuntimeFailure> {
    if descriptor.sha256().len() != 64
        || descriptor
            .sha256()
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
        || !(1..=MAX_STREAM_UPLOAD_SIZE_BYTES).contains(&descriptor.size_bytes())
        || !matches!(
            descriptor.media_type(),
            "image/png" | "image/jpeg" | "text/plain"
        )
    {
        return Err(invalid_runtime_value());
    }
    let created_at = contract_timestamp(descriptor.created_at())?;
    Ok(capability::ContentDescriptor {
        content_id: contract_uuid(descriptor.content_id().as_uuid())?,
        sha256: descriptor.sha256().to_owned(),
        size_bytes: checked_i64(descriptor.size_bytes())?,
        media_type: descriptor.media_type().to_owned(),
        created_at,
    })
}

fn contract_uuid(value: uuid::Uuid) -> Result<String, RuntimeFailure> {
    let encoded = value.hyphenated().to_string();
    parse_contract_uuid(&encoded)
        .is_some()
        .then_some(encoded)
        .ok_or_else(invalid_runtime_value)
}

fn contract_timestamp(value: DateTime<Utc>) -> Result<String, RuntimeFailure> {
    if !(0..=9_999).contains(&value.year()) {
        return Err(invalid_runtime_value());
    }
    let encoded = value.to_rfc3339();
    let bytes = encoded.as_bytes();
    if encoded.len() > 64
        || bytes.len() < 20
        || !bytes[..4].iter().all(u8::is_ascii_digit)
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
    {
        return Err(invalid_runtime_value());
    }
    Ok(encoded)
}

fn checked_i64(value: u64) -> Result<i64, RuntimeFailure> {
    i64::try_from(value).map_err(|_| invalid_runtime_value())
}

fn bounded_sweep_count(value: u64) -> Result<i64, RuntimeFailure> {
    if value > u64::from(MAX_SWEEP_BATCH_LIMIT) {
        return Err(invalid_runtime_value());
    }
    checked_i64(value)
}

fn bounded_upload_offset(value: u64) -> Result<i64, RuntimeFailure> {
    if value > MAX_STREAM_UPLOAD_SIZE_BYTES {
        return Err(invalid_runtime_value());
    }
    checked_i64(value)
}

fn invalid_runtime_value() -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: "Content Vault produced an out-of-range Contract value".to_owned(),
    }
}

fn inactive_task_scope() -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: "Content Vault stream task scope is inactive".to_owned(),
    }
}

fn explicit_s3_stores(
    config: &ContentVaultPluginConfig,
    access_key: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
) -> Result<ContentVaultStores, RuntimeFailure> {
    if access_key.trim().is_empty()
        || secret_access_key.trim().is_empty()
        || session_token.is_some_and(|token| token.trim().is_empty())
    {
        return Err(RuntimeFailure::PluginFailure {
            detail: "Content Vault object-store credentials are missing".to_owned(),
        });
    }
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.s3_bucket)
        .with_region(&config.s3_region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_access_key)
        .with_allow_http(config.s3_allow_http)
        .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
    if let Some(endpoint) = config.s3_endpoint.as_deref() {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(token) = session_token {
        builder = builder.with_token(token);
    }
    let store = builder.build().map_err(|_| RuntimeFailure::PluginFailure {
        detail: "Content Vault object-store configuration is invalid".to_owned(),
    })?;
    ContentVaultStores::from_object_store(Arc::new(store)).map_err(|_| {
        RuntimeFailure::PluginFailure {
            detail: "Content Vault object-store namespaces are invalid".to_owned(),
        }
    })
}

async fn resolve_secret(
    client: &SecretsClient,
    context: Ctx,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    client
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|value| Zeroizing::new(value.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
}

fn valid_secret_reference(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= 256
        && !reference.starts_with('/')
        && !reference.ends_with('/')
        && !reference.contains("//")
        && reference
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
}

fn valid_plugin_instance(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_app_plan::{
        AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
        PluginInstancePlan, ResolvedAppPlan,
    };
    use lenso_capability_secrets::{
        CAPABILITY_ID as SECRETS_CAPABILITY_ID, DESCRIPTOR_VERSION as SECRETS_DESCRIPTOR_VERSION,
        RESOLVE_OPERATION, ResolveError, ResolveRequest, ResolveResponse, Secrets, SecretsEndpoint,
        SecretsProvider,
    };
    use lenso_kernel::{
        CancellationToken, NativeRequestEndpoint, NativeRequestFuture, NativeStreamEndpoint,
        NativeStreamItem, NativeStreamSession, NoopPluginLifecycle, ShutdownOutcome,
    };
    use lenso_native_adapter::{
        NativePluginFactory, NativePluginFactoryContext, NativePluginInstance,
    };
    use lenso_test::TestApp;
    use std::{collections::BTreeMap, time::Duration};

    const CONSUMER_PACKAGE_ID: &str = "test.content-vault-consumer";
    const FIXTURE_PACKAGE_ID: &str = "test.content-vault-provider";
    const SECRETS_PACKAGE_ID: &str = "test.content-vault-secrets";
    const EMPTY_PACKAGE_ID: &str = "test.without-content-vault";

    fn config() -> ContentVaultPluginConfig {
        ContentVaultPluginConfig::new(
            "content-vault/database",
            "content-vault-test",
            "us-east-1",
            "content-vault/access-key",
            "content-vault/secret-key",
            vec!["maintenance.jobs".to_owned()],
        )
        .unwrap()
        .with_s3_endpoint("https://s3.example.test", false)
        .unwrap()
        .with_bounds(DEFAULT_QUARANTINE_GRACE_SECONDS, 100, 1)
        .unwrap()
    }

    fn context(caller: Option<&str>) -> Ctx {
        let context = Ctx::new(1, None, CancellationToken::new());
        if let Some(caller) = caller {
            context.with_caller_instance(caller)
        } else {
            context
        }
    }

    fn plugin() -> ContentVaultPlugin {
        ContentVaultPlugin {
            config: config(),
            secrets: Port::default(),
            tasks: ManagedTasks::default(),
            state: Rc::default(),
        }
    }

    #[test]
    fn generated_descriptor_uses_plugin_identity_and_contracts() {
        let descriptor: serde_json::Value =
            serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).expect("valid Plugin Descriptor");
        assert_eq!(PACKAGE_ID, PLUGIN_ID);
        assert_eq!(descriptor["plugin_id"], PLUGIN_ID);
        assert_eq!(descriptor["root_slot"], "content-vault");
        assert_eq!(
            descriptor["provided_capabilities"][0]["capability_id"],
            capability::CAPABILITY_ID
        );
        assert_eq!(
            descriptor["required_capabilities"][0]["capability_id"],
            SECRETS_CAPABILITY_ID
        );
    }

    #[test]
    fn config_rejects_insecure_or_embedded_storage_authority() {
        let base = ContentVaultPluginConfig::new(
            "content-vault/database",
            "content-vault-test",
            "us-east-1",
            "content-vault/access-key",
            "content-vault/secret-key",
            vec!["maintenance.jobs".to_owned()],
        )
        .unwrap();
        assert_eq!(
            base.clone()
                .with_s3_endpoint("http://s3.example.test", false),
            Err(ContentVaultConfigError::InvalidEndpoint)
        );
        assert_eq!(
            base.clone()
                .with_s3_endpoint("https://user:password@s3.example.test", false),
            Err(ContentVaultConfigError::InvalidEndpoint)
        );
        assert_eq!(
            base.clone()
                .with_s3_endpoint("https://s3.example.test?token=secret", false),
            Err(ContentVaultConfigError::InvalidEndpoint)
        );
        assert_eq!(
            base.clone()
                .with_s3_endpoint("https://s3.example.test", true),
            Err(ContentVaultConfigError::InvalidEndpoint)
        );
        assert!(
            base.clone()
                .with_s3_endpoint("http://s3.example.test", true)
                .is_ok()
        );
        let mut implicit_http = base.clone();
        implicit_http.s3_allow_http = true;
        assert_eq!(
            implicit_http.validate(),
            Err(ContentVaultConfigError::InvalidEndpoint)
        );

        let too_many_callers = (0..=MAX_MAINTENANCE_CALLERS)
            .map(|index| format!("maintenance.{index}"))
            .collect();
        assert_eq!(
            ContentVaultPluginConfig::new(
                "content-vault/database",
                "content-vault-test",
                "us-east-1",
                "content-vault/access-key",
                "content-vault/secret-key",
                too_many_callers,
            ),
            Err(ContentVaultConfigError::InvalidMaintenanceCallers)
        );

        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../configuration.schema.json"))
                .expect("valid configuration schema");
        assert_configuration_schema_uses_app_plan_subset(&schema);
        assert_eq!(
            schema["properties"]["maintenance_callers"]["items"]["type"],
            "string"
        );
    }

    #[test]
    fn config_bounds_quarantine_grace_to_one_year() {
        assert!(
            config()
                .with_bounds(MAX_QUARANTINE_GRACE_SECONDS, 100, 1)
                .is_ok()
        );
        assert_eq!(
            config().with_bounds(MAX_QUARANTINE_GRACE_SECONDS + 1, 100, 1),
            Err(ContentVaultConfigError::InvalidSweepBounds)
        );
    }

    fn assert_configuration_schema_uses_app_plan_subset(schema: &serde_json::Value) {
        const SUPPORTED: &[&str] = &[
            "$schema",
            "additionalProperties",
            "items",
            "minimum",
            "properties",
            "required",
            "title",
            "type",
        ];
        let object = schema.as_object().expect("schema node is an object");
        for keyword in object.keys() {
            assert!(
                SUPPORTED.contains(&keyword.as_str()),
                "configuration schema keyword `{keyword}` is outside the App-plan subset"
            );
        }
        if let Some(properties) = object.get("properties") {
            for child in properties
                .as_object()
                .expect("schema properties are an object")
                .values()
            {
                assert_configuration_schema_uses_app_plan_subset(child);
            }
        }
        if let Some(items) = object.get("items") {
            assert_configuration_schema_uses_app_plan_subset(items);
        }
    }

    #[test]
    fn resolved_s3_credentials_fail_closed_without_exposing_values() {
        for (access_key, secret_key, token) in [
            ("", "secret-sentinel", None),
            ("access-sentinel", "", None),
            ("access-sentinel", "secret-sentinel", Some("")),
        ] {
            let Err(error) = explicit_s3_stores(&config(), access_key, secret_key, token) else {
                panic!("empty resolved credential must be rejected")
            };
            let RuntimeFailure::PluginFailure { detail } = error else {
                panic!("credential failure must remain a generic Plugin failure")
            };
            assert_eq!(detail, "Content Vault object-store credentials are missing");
            assert!(!detail.contains("sentinel"));
        }
    }

    #[tokio::test]
    async fn arbitrary_bound_caller_is_rejected_before_vault_access() {
        let plugin = plugin();
        let grant = owner_grant("owner.plugin");
        assert!(matches!(
            plugin
                .describe(
                    context(Some("bound.consumer")),
                    capability::DescribeRequest {
                        grant,
                        content_id: uuid::Uuid::now_v7().to_string(),
                    },
                )
                .await,
            Err(PluginError::Domain(capability::DescribeError::Unauthorized))
        ));
        assert!(matches!(
            plugin
                .sweep(context(Some("bound.consumer")), capability::SweepRequest {},)
                .await,
            Err(PluginError::Domain(capability::SweepError::Unauthorized))
        ));
        assert!(plugin.maintenance_authorized(&context(Some("maintenance.jobs"))));
    }

    #[tokio::test]
    async fn native_handlers_reject_directly_constructed_out_of_contract_values() {
        let plugin = plugin();
        let mut oversized_grant = owner_grant("owner.plugin");
        oversized_grant.owner.resource_id = "x".repeat(501);
        assert!(matches!(
            plugin
                .describe(
                    context(Some("owner.plugin")),
                    capability::DescribeRequest {
                        grant: oversized_grant,
                        content_id: uuid::Uuid::now_v7().to_string(),
                    },
                )
                .await,
            Err(PluginError::Domain(capability::DescribeError::InvalidInput))
        ));

        assert!(matches!(
            plugin
                .reserve(
                    context(Some("owner.plugin")),
                    capability::ReserveRequest {
                        grant: owner_grant("owner.plugin"),
                        idempotency_key: "x".repeat(301),
                        expected_sha256: "a".repeat(64),
                        expected_size_bytes: 1,
                        media_type: "text/plain".to_owned(),
                        ttl_seconds: 60,
                    },
                )
                .await,
            Err(PluginError::Domain(capability::ReserveError::InvalidInput))
        ));

        assert!(matches!(
            plugin
                .claim(
                    context(Some("owner.plugin")),
                    capability::ClaimRequest {
                        grant: owner_grant("owner.plugin"),
                        target: owner_grant("owner.plugin").owner,
                        content_id: uuid::Uuid::now_v7().to_string(),
                        role: "r".repeat(101),
                    },
                )
                .await,
            Err(PluginError::Domain(capability::ClaimError::InvalidInput))
        ));
    }

    #[test]
    fn native_uuid_parsing_matches_the_portable_contract() {
        let id = uuid::Uuid::now_v7();
        assert!(parse_content_id(&id.hyphenated().to_string()).is_ok());
        assert!(parse_session_id(&id.hyphenated().to_string().to_uppercase()).is_ok());
        assert!(parse_content_id(&id.simple().to_string()).is_err());
        assert!(parse_content_id(&id.braced().to_string()).is_err());
        assert!(parse_content_id(&id.urn().to_string()).is_err());
        assert!(parse_content_id("00000000-0000-0000-0000-000000000000").is_err());
        assert!(parse_session_id("018f0000-0000-7000-7000-000000000000").is_err());
    }

    #[test]
    fn outbound_uuid_and_timestamp_projection_fail_closed() {
        use chrono::TimeZone as _;

        let valid_id = uuid::Uuid::now_v7();
        assert_eq!(contract_uuid(valid_id).unwrap(), valid_id.to_string());
        assert!(contract_uuid(uuid::Uuid::nil()).is_err());

        let ordinary = Utc.with_ymd_and_hms(2026, 8, 30, 0, 0, 0).unwrap();
        assert!(contract_timestamp(ordinary).is_ok());
        let extreme = Utc.with_ymd_and_hms(10_000, 1, 1, 0, 0, 0).unwrap();
        assert!(contract_timestamp(extreme).is_err());

        let nil_descriptor = ContentDescriptor::new(
            ContentId::from_uuid(uuid::Uuid::nil()),
            "a".repeat(64),
            1,
            "text/plain".to_owned(),
            ordinary,
        );
        assert!(capability_descriptor(&nil_descriptor).is_err());
        let extreme_descriptor = ContentDescriptor::new(
            ContentId::from_uuid(valid_id),
            "a".repeat(64),
            1,
            "text/plain".to_owned(),
            extreme,
        );
        assert!(capability_descriptor(&extreme_descriptor).is_err());
    }

    #[tokio::test]
    async fn upload_resume_offset_and_consumer_half_close_are_preserved() {
        let invocation = context(Some("owner.plugin"));
        let (stream, mut provider) =
            ProviderStream::<capability::ContentVaultUpload>::channel(&invocation, 1);
        let consumer = async {
            stream
                .send(Box::new(capability::UploadFrame {
                    kind: capability::UploadFrameKind::Chunk,
                    offset: Some(8),
                    bytes_base64: Some(Some(STANDARD.encode(b"next"))),
                    content: None,
                }))
                .await
                .expect("resumed frame is admitted");
            stream
                .close_send()
                .await
                .expect("consumer half-close is admitted");
        };
        let plugin = async {
            let (mut reader, writer) = tokio::io::duplex(32);
            let (protocol_success, protocol_confirmation) = oneshot::channel();
            feed_upload(&mut provider, writer, 8, protocol_success)
                .await
                .expect("half-close terminates only the byte source");
            protocol_confirmation
                .await
                .expect("half-close confirms protocol success");
            let mut received = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut received)
                .await
                .unwrap();
            assert_eq!(received, b"next");
        };
        futures::join!(consumer, plugin);

        let invocation = context(Some("owner.plugin"));
        let (stream, mut provider) =
            ProviderStream::<capability::ContentVaultUpload>::channel(&invocation, 1);
        let consumer = stream.send(Box::new(capability::UploadFrame {
            kind: capability::UploadFrameKind::Chunk,
            offset: Some(0),
            bytes_base64: Some(Some(STANDARD.encode(b"wrong offset"))),
            content: None,
        }));
        let plugin = async {
            let (_reader, writer) = tokio::io::duplex(32);
            let (protocol_success, _protocol_confirmation) = oneshot::channel();
            assert!(matches!(
                feed_upload(&mut provider, writer, 8, protocol_success).await,
                Err(PluginError::Domain(capability::UploadError::Conflict))
            ));
        };
        let (admission, ()) = futures::join!(consumer, plugin);
        admission.expect("conflicting frame reaches the provider");

        let invocation = context(Some("owner.plugin"));
        let (stream, mut provider) =
            ProviderStream::<capability::ContentVaultUpload>::channel(&invocation, 1);
        let consumer = stream.send(Box::new(capability::UploadFrame {
            kind: capability::UploadFrameKind::Chunk,
            offset: Some(i64::try_from(MAX_STREAM_UPLOAD_SIZE_BYTES + 1).unwrap()),
            bytes_base64: Some(Some(STANDARD.encode(b"bounded"))),
            content: None,
        }));
        let plugin = async {
            let (_reader, writer) = tokio::io::duplex(32);
            let (protocol_success, _protocol_confirmation) = oneshot::channel();
            assert!(matches!(
                feed_upload(
                    &mut provider,
                    writer,
                    MAX_STREAM_UPLOAD_SIZE_BYTES + 1,
                    protocol_success,
                )
                .await,
                Err(PluginError::Domain(capability::UploadError::InvalidInput))
            ));
        };
        let (admission, ()) = futures::join!(consumer, plugin);
        admission.expect("out-of-contract frame reaches native validation");
    }

    #[test]
    fn stream_backpressure_cancellation_and_one_terminal_are_observable() {
        futures::executor::block_on(async {
            let invocation = context(Some("owner.plugin"));
            let (stream, mut provider) =
                ProviderStream::<capability::ContentVaultUpload>::channel(&invocation, 1);
            provider
                .send(capability::UploadFrame {
                    kind: capability::UploadFrameKind::Committed,
                    offset: Some(0),
                    bytes_base64: None,
                    content: None,
                })
                .await
                .expect("first frame fills the bounded channel");
            let blocked = provider.send(capability::UploadFrame {
                kind: capability::UploadFrameKind::Committed,
                offset: Some(1),
                bytes_base64: None,
                content: None,
            });
            futures::pin_mut!(blocked);
            assert!(futures::poll!(blocked.as_mut()).is_pending());
            stream.cancel();
            assert!(matches!(
                blocked.await,
                Err(RuntimeFailure::AdmissionClosed)
            ));

            let invocation = context(Some("owner.plugin"));
            let (stream, provider) =
                ProviderStream::<capability::ContentVaultUpload>::channel(&invocation, 1);
            let completing = provider.complete(Err(PluginError::Domain(
                capability::UploadError::UploadRejected,
            )));
            let observing = async {
                assert!(matches!(
                    stream.receive().await,
                    Ok(NativeStreamItem::PeerHalfClosed)
                ));
                let NativeStreamItem::Terminal(Err(error)) = stream
                    .receive()
                    .await
                    .expect("terminal outcome follows provider half-close")
                else {
                    panic!("expected a Domain terminal outcome");
                };
                assert_eq!(
                    *error
                        .downcast::<capability::UploadError>()
                        .expect("typed generated Domain Error"),
                    capability::UploadError::UploadRejected
                );
                assert!(matches!(
                    stream.receive().await,
                    Err(RuntimeFailure::AdmissionClosed)
                ));
            };
            let (completion, ()) = futures::join!(completing, observing);
            completion.expect("exactly one terminal path is admitted");
        });
    }

    #[test]
    fn generated_client_observes_success_domain_and_runtime_outcomes() {
        let app = TestApp::builder(fixture_plan())
            .with_factory(FixtureFactory)
            .with_factory(ConsumerFactory)
            .start()
            .expect("fixture App starts");
        let client = app
            .client::<capability::ContentVaultClient>("consumer")
            .expect("generated Client binds through the Plan");

        let success = app
            .run(client.describe(describe_request("success")))
            .expect("fixture success");
        assert_eq!(success.content.content_id, "success");
        assert!(matches!(
            app.run(client.describe(describe_request("missing"))),
            Err(capability::ContentVaultDescribeInvocationError::Domain(
                capability::DescribeError::NotFound
            ))
        ));
        assert!(matches!(
            app.run(client.describe(describe_request("runtime"))),
            Err(capability::ContentVaultDescribeInvocationError::Runtime(
                RuntimeFailure::PluginFailure { detail }
            )) if detail == "fixture unavailable"
        ));
        assert_eq!(app.shutdown(Duration::from_secs(1)), ShutdownOutcome::Clean);
    }

    #[test]
    fn linked_factory_rejects_invalid_configuration_before_startup() {
        let invalid = r#"{
            "database_url_secret":"same",
            "s3_bucket":"bucket",
            "s3_region":"us-east-1",
            "s3_access_key_id_secret":"same",
            "s3_secret_access_key_secret":"same",
            "quarantine_grace_seconds":900,
            "sweep_batch_limit":100,
            "stream_channel_capacity":1,
            "maintenance_callers":["maintenance.jobs"]
        }"#;
        let error = TestApp::builder(actual_plan(invalid))
            .with_linked_factories()
            .with_factory(StaticSecretsFactory::default())
            .start()
            .expect_err("semantic configuration validation must reject startup");
        assert!(matches!(error, RuntimeFailure::InvalidResolvedPlan { .. }));
    }

    #[test]
    fn removing_plugin_selection_removes_behavior_without_kernel_changes() {
        let plan = AppComposition::new(
            vec![PluginInstancePlan::new("empty", EMPTY_PACKAGE_ID)],
            Vec::new(),
        )
        .resolve()
        .unwrap();
        let app = TestApp::builder(plan)
            .with_linked_factories()
            .with_factory(EmptyFactory)
            .start()
            .expect("unselected Content Vault Plugin is inert");
        assert_eq!(app.shutdown(Duration::from_secs(1)), ShutdownOutcome::Clean);
    }

    fn owner_grant(owner: &str) -> capability::OwnerGrant {
        capability::OwnerGrant {
            tenant_id: "tenant-1".to_owned(),
            owner: capability::Owner {
                plugin_instance: owner.to_owned(),
                resource_type: "document".to_owned(),
                resource_id: "doc-1".to_owned(),
                revision_id: None,
            },
            actor_id: "actor-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
        }
    }

    fn describe_request(content_id: &str) -> capability::DescribeRequest {
        capability::DescribeRequest {
            grant: owner_grant("consumer"),
            content_id: content_id.to_owned(),
        }
    }

    #[derive(Clone, Debug)]
    struct FixtureProvider;

    #[allow(clippy::unused_async)]
    impl FixtureProvider {
        async fn describe(
            &self,
            _context: Ctx,
            request: capability::DescribeRequest,
        ) -> PluginResult<capability::DescribeResponse, capability::DescribeError> {
            match request.content_id.as_str() {
                "missing" => Err(PluginError::Domain(capability::DescribeError::NotFound)),
                "runtime" => Err(PluginError::Runtime(RuntimeFailure::PluginFailure {
                    detail: "fixture unavailable".to_owned(),
                })),
                _ => Ok(capability::DescribeResponse {
                    content: capability::ContentDescriptor {
                        content_id: request.content_id,
                        sha256: "0".repeat(64),
                        size_bytes: 1,
                        media_type: "text/plain".to_owned(),
                        created_at: "2026-08-30T00:00:00Z".to_owned(),
                    },
                }),
            }
        }

        async fn reserve(
            &self,
            _context: Ctx,
            _request: capability::ReserveRequest,
        ) -> Result<capability::ReserveResponse, capability::ReserveError> {
            Err(capability::ReserveError::InvalidInput)
        }

        async fn claim(
            &self,
            _context: Ctx,
            _request: capability::ClaimRequest,
        ) -> Result<capability::ClaimResponse, capability::ClaimError> {
            Err(capability::ClaimError::InvalidInput)
        }

        async fn release_claim(
            &self,
            _context: Ctx,
            _request: capability::ReleaseClaimRequest,
        ) -> Result<capability::ReleaseClaimResponse, capability::ReleaseClaimError> {
            Err(capability::ReleaseClaimError::InvalidInput)
        }

        async fn sweep(
            &self,
            _context: Ctx,
            _request: capability::SweepRequest,
        ) -> Result<capability::SweepResponse, capability::SweepError> {
            Err(capability::SweepError::Unauthorized)
        }

        async fn upload(
            &self,
            _context: Ctx,
            _request: capability::UploadRequest,
        ) -> Result<ProviderStream<capability::ContentVaultUpload>, capability::UploadError>
        {
            Err(capability::UploadError::InvalidInput)
        }

        async fn download(
            &self,
            _context: Ctx,
            _request: capability::DownloadRequest,
        ) -> Result<ProviderStream<capability::ContentVaultDownload>, capability::DownloadError>
        {
            Err(capability::DownloadError::InvalidInput)
        }
    }

    capability::__lenso_native_lower_content_vault!(FixtureProvider, lenso::__private);

    #[derive(Clone, Debug)]
    struct FixtureFactory;

    impl NativePluginFactory for FixtureFactory {
        fn package_id(&self) -> &'static str {
            FIXTURE_PACKAGE_ID
        }

        fn instantiate(
            &self,
            _context: NativePluginFactoryContext<'_>,
        ) -> Result<NativePluginInstance, RuntimeFailure> {
            let endpoint = Rc::new(capability::ContentVaultEndpoint::new(FixtureProvider));
            let request: Rc<dyn NativeRequestEndpoint> = endpoint.clone();
            let stream: Rc<dyn NativeStreamEndpoint> = endpoint;
            Ok(NativePluginInstance::with_endpoints(
                vec![request],
                vec![stream],
                NoopPluginLifecycle,
            ))
        }
    }

    #[derive(Clone, Default)]
    struct StaticSecretsFactory {
        values: BTreeMap<String, String>,
    }

    impl fmt::Debug for StaticSecretsFactory {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("StaticSecretsFactory")
                .field("references", &self.values.keys().collect::<Vec<_>>())
                .finish()
        }
    }

    impl NativePluginFactory for StaticSecretsFactory {
        fn package_id(&self) -> &'static str {
            SECRETS_PACKAGE_ID
        }

        fn instantiate(
            &self,
            _context: NativePluginFactoryContext<'_>,
        ) -> Result<NativePluginInstance, RuntimeFailure> {
            let endpoint = Rc::new(SecretsEndpoint::new(StaticSecretsProvider {
                values: self.values.clone(),
            })) as Rc<dyn NativeRequestEndpoint>;
            Ok(NativePluginInstance::new(vec![endpoint]))
        }
    }

    #[derive(Clone)]
    struct StaticSecretsProvider {
        values: BTreeMap<String, String>,
    }

    impl fmt::Debug for StaticSecretsProvider {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("StaticSecretsProvider")
                .field("references", &self.values.keys().collect::<Vec<_>>())
                .finish()
        }
    }

    impl SecretsProvider for StaticSecretsProvider {
        fn resolve(&self, _context: Ctx, request: ResolveRequest) -> NativeRequestFuture<Secrets> {
            let result = self
                .values
                .get(&request.reference)
                .cloned()
                .map(|value| ResolveResponse { value })
                .ok_or(ResolveError::UnknownReference);
            Box::pin(async move { Ok(result) })
        }
    }

    #[derive(Clone, Debug)]
    struct EmptyFactory;

    impl NativePluginFactory for EmptyFactory {
        fn package_id(&self) -> &'static str {
            EMPTY_PACKAGE_ID
        }

        fn instantiate(
            &self,
            _context: NativePluginFactoryContext<'_>,
        ) -> Result<NativePluginInstance, RuntimeFailure> {
            Ok(NativePluginInstance::default())
        }
    }

    #[derive(Clone, Debug)]
    struct ConsumerFactory;

    impl NativePluginFactory for ConsumerFactory {
        fn package_id(&self) -> &'static str {
            CONSUMER_PACKAGE_ID
        }

        fn instantiate(
            &self,
            _context: NativePluginFactoryContext<'_>,
        ) -> Result<NativePluginInstance, RuntimeFailure> {
            Ok(NativePluginInstance::default())
        }
    }

    fn all_operations() -> [&'static str; 7] {
        [
            capability::CLAIM_OPERATION,
            capability::DESCRIBE_OPERATION,
            capability::DOWNLOAD_OPERATION,
            capability::RELEASE_CLAIM_OPERATION,
            capability::RESERVE_OPERATION,
            capability::SWEEP_OPERATION,
            capability::UPLOAD_OPERATION,
        ]
    }

    fn fixture_plan() -> ResolvedAppPlan {
        let consumer = PluginInstancePlan::new("consumer", CONSUMER_PACKAGE_ID).with_requirement(
            CapabilityRequirementPlan::one(
                capability::CAPABILITY_ID,
                capability::DESCRIPTOR_VERSION,
            ),
        );
        let provider = PluginInstancePlan::new("vault", FIXTURE_PACKAGE_ID).with_capability(
            CapabilityEndpointPlan::new(
                capability::CAPABILITY_ID,
                capability::DESCRIPTOR_VERSION,
                all_operations(),
            )
            .with_stream_operation(capability::DOWNLOAD_OPERATION)
            .with_stream_operation(capability::UPLOAD_OPERATION),
        );
        AppComposition::new(
            vec![consumer, provider],
            vec![CapabilityBinding::new(
                "consumer",
                capability::CAPABILITY_ID,
                capability::DESCRIPTOR_VERSION,
                "vault",
            )],
        )
        .resolve()
        .unwrap()
    }

    fn actual_plan(configuration: &str) -> ResolvedAppPlan {
        let vault = PluginInstancePlan::new("vault", PACKAGE_ID)
            .with_configuration(configuration)
            .with_requirement(CapabilityRequirementPlan::one(
                SECRETS_CAPABILITY_ID,
                SECRETS_DESCRIPTOR_VERSION,
            ))
            .with_capability(
                CapabilityEndpointPlan::new(
                    capability::CAPABILITY_ID,
                    capability::DESCRIPTOR_VERSION,
                    all_operations(),
                )
                .with_stream_operation(capability::DOWNLOAD_OPERATION)
                .with_stream_operation(capability::UPLOAD_OPERATION),
            );
        let secrets = PluginInstancePlan::new("secrets", SECRETS_PACKAGE_ID).with_capability(
            CapabilityEndpointPlan::new(
                SECRETS_CAPABILITY_ID,
                SECRETS_DESCRIPTOR_VERSION,
                [RESOLVE_OPERATION],
            ),
        );
        AppComposition::new(
            vec![vault, secrets],
            vec![CapabilityBinding::new(
                "vault",
                SECRETS_CAPABILITY_ID,
                SECRETS_DESCRIPTOR_VERSION,
                "secrets",
            )],
        )
        .resolve()
        .unwrap()
    }
}
