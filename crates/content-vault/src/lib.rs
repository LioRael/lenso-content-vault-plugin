//! A linked Lenso Module for tenant-scoped, immutable content ingestion.

mod engine;
mod errors;
pub mod migrations;
pub mod module;
pub mod public;
pub mod storage;
mod streaming;
pub mod validation;

pub use engine::{
    CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES, ContentVault, ContentVaultConfig,
    ContentVaultStreamingConfig, ContentVaultTransaction,
};
pub use errors::{ContentVaultError, ContentVaultErrorCode};
pub use public::{
    CompleteUploadRequest, ContentClaimRole, ContentDescriptor, ContentId, ContentRead, OwnerGrant,
    OwnerRef, ReserveUploadRequest, StageOutcome, SweepReport, UploadSession, UploadSessionId,
    UploadSessionState,
};
pub use storage::{
    CONTENT_VAULT_S3_BUCKET_ENV, ContentVaultStores, DEFAULT_PROTECTED_PREFIX,
    DEFAULT_QUARANTINE_PREFIX, ImmutablePut, ObjectStoreProtected, ObjectStoreQuarantine,
    ProtectedStore, QuarantineStore, STREAMING_ATTEMPT_PREFIX, StoreByteStream, StoreError,
    StoreErrorKind, StoreRead,
};
pub use streaming::{StreamingUpload, VerifiedContent};
pub use validation::{
    BasicContentValidator, ContentValidator, ValidationError, ValidationErrorKind,
};
