//! A removable Lenso Plugin for tenant-scoped, immutable content ingestion.
//!
//! Application consumers bind the generated `lenso.content-vault@1` Capability rather than this
//! crate's former direct Rust engine API.
//!
//! ```compile_fail
//! use content_vault::ContentVault;
//! ```
//! ```compile_fail
//! use content_vault::ContentVaultTransaction;
//! ```
//! ```compile_fail
//! use content_vault::test_support::ContentVault;
//! ```

#[allow(
    dead_code,
    reason = "private implementation and crate-internal acceptance seam"
)]
mod engine;
#[allow(
    dead_code,
    reason = "private implementation and crate-internal acceptance seam"
)]
mod errors;
mod migrations;
mod module;
mod operator;
#[allow(
    dead_code,
    reason = "private implementation and crate-internal acceptance seam"
)]
mod public;
#[allow(
    dead_code,
    reason = "private implementation and crate-internal acceptance seam"
)]
mod storage;
#[allow(
    dead_code,
    reason = "private implementation and crate-internal acceptance seam"
)]
mod streaming;
#[allow(
    dead_code,
    reason = "private implementation and crate-internal acceptance seam"
)]
mod validation;

pub use lenso_postgres_kit::{SetupOutcome, UpgradeOutcome};
pub use module::{ContentVaultConfigError, ContentVaultPluginConfig, PLUGIN_ID};
pub use operator::{ContentVaultOperator, ContentVaultOperatorError, LegacyAdoptionOutcome};

pub(crate) use engine::{CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES, ContentVault};
pub(crate) use errors::{ContentVaultError, ContentVaultErrorCode};
pub(crate) use public::{
    ContentClaimRole, ContentDescriptor, ContentId, OwnerGrant, OwnerRef, ReserveUploadRequest,
    UploadSessionId,
};
pub(crate) use storage::ContentVaultStores;
pub(crate) use streaming::{StreamingUpload, VerifiedContent};

#[cfg(all(test, any(feature = "postgres-acceptance", feature = "s3-acceptance")))]
mod test_support {
    #[cfg(feature = "s3-acceptance")]
    pub use crate::engine::CONTENT_VAULT_STREAMING_CHUNK_SIZE_BYTES;
    pub use crate::engine::{ContentVault, ContentVaultConfig, ContentVaultStreamingConfig};
    pub use crate::errors::ContentVaultErrorCode;
    pub use crate::migrations::CONTENT_VAULT_MIGRATIONS;
    pub use crate::public::{
        CompleteUploadRequest, ContentClaimRole, ContentDescriptor, ContentId, OwnerGrant,
        OwnerRef, ReserveUploadRequest, StageOutcome, UploadSession,
    };
    #[cfg(feature = "s3-acceptance")]
    pub use crate::storage::ContentVaultStores;
    pub use crate::storage::{
        ImmutablePut, ProtectedStore, QuarantineStore, StoreByteStream, StoreError, StoreErrorKind,
        StoreRead,
    };
}

#[cfg(all(test, feature = "postgres-acceptance"))]
#[path = "../tests/operator_acceptance.rs"]
mod operator_acceptance;

#[cfg(all(test, feature = "postgres-acceptance"))]
#[path = "../tests/postgres_acceptance.rs"]
mod postgres_acceptance;

#[cfg(all(test, feature = "postgres-acceptance"))]
#[path = "../tests/postgres_streaming_acceptance.rs"]
mod postgres_streaming_acceptance;

#[cfg(all(test, feature = "s3-acceptance"))]
#[path = "../tests/s3_acceptance.rs"]
mod s3_acceptance;
