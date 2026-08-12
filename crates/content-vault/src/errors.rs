use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentVaultErrorCode {
    InvalidInput,
    Conflict,
    NotFound,
    UploadMissing,
    UploadInterrupted,
    UploadRejected,
    UploadExpired,
    IntegrityMissing,
    IntegrityMismatch,
    StorageUnavailable,
    DatabaseUnavailable,
}

impl ContentVaultErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::UploadMissing => "upload_missing",
            Self::UploadInterrupted => "upload_interrupted",
            Self::UploadRejected => "upload_rejected",
            Self::UploadExpired => "upload_expired",
            Self::IntegrityMissing => "integrity_missing",
            Self::IntegrityMismatch => "integrity_mismatch",
            Self::StorageUnavailable => "storage_unavailable",
            Self::DatabaseUnavailable => "database_unavailable",
        }
    }
}

impl fmt::Display for ContentVaultErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {message}")]
pub struct ContentVaultError {
    code: ContentVaultErrorCode,
    message: String,
}

impl ContentVaultError {
    pub fn new(code: ContentVaultErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub const fn code(&self) -> ContentVaultErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(ContentVaultErrorCode::InvalidInput, message)
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::new(ContentVaultErrorCode::Conflict, message)
    }

    pub(crate) fn not_found() -> Self {
        Self::new(ContentVaultErrorCode::NotFound, "content was not found")
    }

    pub(crate) fn database() -> Self {
        Self::new(
            ContentVaultErrorCode::DatabaseUnavailable,
            "content vault database is unavailable",
        )
    }

    pub(crate) fn storage() -> Self {
        Self::new(
            ContentVaultErrorCode::StorageUnavailable,
            "content vault storage is unavailable",
        )
    }
}
