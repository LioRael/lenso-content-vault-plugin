use crate::errors::ContentVaultError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UploadSessionId(Uuid);

impl UploadSessionId {
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for UploadSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentId(Uuid);

impl ContentId {
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OwnerRef {
    module: String,
    resource_type: String,
    resource_id: String,
    revision_id: Option<String>,
}

impl OwnerRef {
    pub fn new(
        module: impl Into<String>,
        resource_type: impl Into<String>,
        resource_id: impl Into<String>,
        revision_id: Option<String>,
    ) -> Result<Self, ContentVaultError> {
        let value = Self {
            module: module.into(),
            resource_type: resource_type.into(),
            resource_id: resource_id.into(),
            revision_id,
        };
        validate_required("owner module", &value.module, 200)?;
        validate_required("owner resource type", &value.resource_type, 200)?;
        validate_required("owner resource id", &value.resource_id, 500)?;
        if let Some(revision_id) = &value.revision_id {
            validate_required("owner revision id", revision_id, 500)?;
        }
        Ok(value)
    }

    pub fn module(&self) -> &str {
        &self.module
    }

    pub fn resource_type(&self) -> &str {
        &self.resource_type
    }

    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }

    pub fn revision_id(&self) -> Option<&str> {
        self.revision_id.as_deref()
    }

    pub(crate) fn revision_for_store(&self) -> &str {
        self.revision_id.as_deref().unwrap_or("")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OwnerGrant {
    tenant_id: String,
    owner: OwnerRef,
    actor_id: String,
    correlation_id: String,
}

impl OwnerGrant {
    pub fn new(
        tenant_id: impl Into<String>,
        owner: OwnerRef,
        actor_id: impl Into<String>,
        correlation_id: impl Into<String>,
    ) -> Result<Self, ContentVaultError> {
        let value = Self {
            tenant_id: tenant_id.into(),
            owner,
            actor_id: actor_id.into(),
            correlation_id: correlation_id.into(),
        };
        validate_required("tenant id", &value.tenant_id, 200)?;
        validate_required("actor id", &value.actor_id, 500)?;
        validate_required("correlation id", &value.correlation_id, 500)?;
        Ok(value)
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub const fn owner(&self) -> &OwnerRef {
        &self.owner
    }

    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReserveUploadRequest {
    idempotency_key: String,
    expected_sha256: String,
    expected_size_bytes: u64,
    media_type: String,
    ttl_seconds: u32,
}

impl ReserveUploadRequest {
    pub fn new(
        idempotency_key: impl Into<String>,
        expected_sha256: impl Into<String>,
        expected_size_bytes: u64,
        media_type: impl Into<String>,
        ttl_seconds: u32,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            expected_sha256: expected_sha256.into(),
            expected_size_bytes,
            media_type: media_type.into(),
            ttl_seconds,
        }
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn expected_sha256(&self) -> &str {
        &self.expected_sha256
    }

    pub const fn expected_size_bytes(&self) -> u64 {
        self.expected_size_bytes
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub const fn ttl_seconds(&self) -> u32 {
        self.ttl_seconds
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompleteUploadRequest {
    session_id: UploadSessionId,
    idempotency_key: String,
}

impl CompleteUploadRequest {
    pub fn new(session_id: UploadSessionId, idempotency_key: impl Into<String>) -> Self {
        Self {
            session_id,
            idempotency_key: idempotency_key.into(),
        }
    }

    pub const fn session_id(&self) -> UploadSessionId {
        self.session_id
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadSessionState {
    Reserved,
    Staging,
    Committed,
    Rejected,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSession {
    session_id: UploadSessionId,
    state: UploadSessionState,
    expected_sha256: String,
    expected_size_bytes: u64,
    media_type: String,
    expires_at: DateTime<Utc>,
}

impl UploadSession {
    pub(crate) fn reserved(
        session_id: UploadSessionId,
        expected_sha256: String,
        expected_size_bytes: u64,
        media_type: String,
        expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            session_id,
            state: UploadSessionState::Reserved,
            expected_sha256,
            expected_size_bytes,
            media_type,
            expires_at,
        }
    }

    pub const fn session_id(&self) -> UploadSessionId {
        self.session_id
    }

    pub const fn state(&self) -> UploadSessionState {
        self.state
    }

    pub fn expected_sha256(&self) -> &str {
        &self.expected_sha256
    }

    pub const fn expected_size_bytes(&self) -> u64 {
        self.expected_size_bytes
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentDescriptor {
    content_id: ContentId,
    sha256: String,
    size_bytes: u64,
    media_type: String,
    created_at: DateTime<Utc>,
}

impl ContentDescriptor {
    pub(crate) fn new(
        content_id: ContentId,
        sha256: String,
        size_bytes: u64,
        media_type: String,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            content_id,
            sha256,
            size_bytes,
            media_type,
            created_at,
        }
    }

    pub const fn content_id(&self) -> ContentId {
        self.content_id
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentRead {
    descriptor: ContentDescriptor,
    bytes: Vec<u8>,
}

impl ContentRead {
    pub(crate) fn new(descriptor: ContentDescriptor, bytes: Vec<u8>) -> Self {
        Self { descriptor, bytes }
    }

    pub const fn descriptor(&self) -> &ContentDescriptor {
        &self.descriptor
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentClaimRole(String);

impl ContentClaimRole {
    pub fn new(value: impl Into<String>) -> Result<Self, ContentVaultError> {
        let value = value.into();
        validate_required("claim role", &value, 100)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn initial() -> Self {
        Self("source".to_owned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOutcome {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SweepReport {
    pub expired_sessions: u64,
    pub cleaned_objects: u64,
    pub failed_objects: u64,
}

fn validate_required(
    label: &str,
    value: &str,
    maximum_length: usize,
) -> Result<(), ContentVaultError> {
    let mut character_count = 0_usize;
    let mut contains_non_whitespace = false;
    for character in value.chars() {
        character_count += 1;
        let codepoint = u32::from(character);
        if matches!(codepoint, 0x00..=0x1f | 0x7f) {
            return Err(ContentVaultError::invalid(format!(
                "{label} contains control characters"
            )));
        }
        contains_non_whitespace |= !is_ecmascript_whitespace(character);
    }
    if !contains_non_whitespace {
        return Err(ContentVaultError::invalid(format!("{label} is required")));
    }
    if character_count > maximum_length {
        return Err(ContentVaultError::invalid(format!(
            "{label} exceeds {maximum_length} characters"
        )));
    }
    Ok(())
}

// JSON Schema regular expressions use ECMAScript `\s`, whose whitespace set differs from
// Rust's Unicode `char::is_whitespace` at U+0085 and U+FEFF. Keep the native boundary exact.
fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000A}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

pub(crate) fn validate_idempotency_key(value: &str) -> Result<(), ContentVaultError> {
    validate_required("idempotency key", value, 300)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_identifier_bounds_count_unicode_code_points_like_json_schema() {
        assert!(validate_required("identifier", &"界".repeat(200), 200).is_ok());
        assert!(validate_required("identifier", &"界".repeat(201), 200).is_err());
        assert!(validate_required("identifier", "\u{200b}", 1).is_ok());
    }

    #[test]
    fn opaque_identifier_control_and_whitespace_rules_match_the_contract_pattern() {
        for rejected in ["", "   ", "\u{00a0}", "\u{feff}"] {
            assert!(
                validate_required("identifier", rejected, 200).is_err(),
                "{rejected:?} must be rejected"
            );
        }
        assert!(validate_required("identifier", "\u{0080}", 200).is_ok());
        assert!(validate_required("identifier", "\u{0085}", 200).is_ok());
        assert!(validate_required("identifier", "租户-一", 200).is_ok());
    }
}
