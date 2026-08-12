//! A linked Lenso Module for tenant-scoped, immutable content ingestion.

mod engine;
mod errors;
pub mod migrations;
pub mod module;
pub mod public;
pub mod storage;
pub mod validation;

pub use engine::{ContentVault, ContentVaultConfig, ContentVaultTransaction};
pub use errors::{ContentVaultError, ContentVaultErrorCode};
pub use public::{
    CompleteUploadRequest, ContentClaimRole, ContentDescriptor, ContentId, ContentRead, OwnerGrant,
    OwnerRef, ReserveUploadRequest, StageOutcome, SweepReport, UploadSession, UploadSessionId,
    UploadSessionState,
};
pub use storage::{
    CONTENT_VAULT_S3_BUCKET_ENV, ContentVaultStores, DEFAULT_PROTECTED_PREFIX,
    DEFAULT_QUARANTINE_PREFIX, ImmutablePut, ObjectStoreProtected, ObjectStoreQuarantine,
    ProtectedStore, QuarantineStore, StoreError, StoreErrorKind,
};
pub use validation::{
    BasicContentValidator, ContentValidator, ValidationError, ValidationErrorKind,
};
