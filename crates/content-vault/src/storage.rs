use async_trait::async_trait;
use object_store::{ObjectStore, PutMode, PutOptions, path::Path as ObjectPath};
use std::sync::Arc;

pub const CONTENT_VAULT_S3_BUCKET_ENV: &str = "CONTENT_VAULT_S3_BUCKET";
pub const DEFAULT_QUARANTINE_PREFIX: &str = "content-vault/quarantine";
pub const DEFAULT_PROTECTED_PREFIX: &str = "content-vault/protected";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImmutablePut {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreErrorKind {
    Unavailable,
    ImmutableConflict,
    InvalidKey,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct StoreError {
    kind: StoreErrorKind,
    message: String,
}

impl StoreError {
    pub fn new(kind: StoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> StoreErrorKind {
        self.kind
    }
}

#[async_trait]
pub trait QuarantineStore: std::fmt::Debug + Send + Sync {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError>;
    async fn delete_exact(&self, key: &str) -> Result<(), StoreError>;
}

#[async_trait]
pub trait ProtectedStore: std::fmt::Debug + Send + Sync {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError>;
}

#[derive(Debug, Clone)]
struct ObjectStoreArea {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStoreArea {
    fn new(inner: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Result<Self, StoreError> {
        let prefix = prefix.into().trim_matches('/').to_owned();
        if !prefix.is_empty() && !valid_key(&prefix) {
            return Err(StoreError::new(
                StoreErrorKind::InvalidKey,
                "object-store prefix is not a safe relative path",
            ));
        }
        Ok(Self { inner, prefix })
    }

    fn path(&self, key: &str) -> Result<ObjectPath, StoreError> {
        if !valid_key(key) {
            return Err(StoreError::new(
                StoreErrorKind::InvalidKey,
                "object key is not a safe relative path",
            ));
        }
        let full = if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        };
        Ok(ObjectPath::from(full))
    }

    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let path = self.path(key)?;
        match self.inner.get(&path).await {
            Ok(result) => result
                .bytes()
                .await
                .map(|bytes| Some(bytes.to_vec()))
                .map_err(|_| unavailable()),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(_) => Err(unavailable()),
        }
    }

    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        let path = self.path(key)?;
        match self
            .inner
            .put_opts(
                &path,
                bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(ImmutablePut::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                if self.read(key).await?.as_deref() == Some(bytes.as_slice()) {
                    Ok(ImmutablePut::AlreadyPresent)
                } else {
                    Err(StoreError::new(
                        StoreErrorKind::ImmutableConflict,
                        "immutable object key already contains different bytes",
                    ))
                }
            }
            Err(_) => Err(unavailable()),
        }
    }

    async fn delete_exact(&self, key: &str) -> Result<(), StoreError> {
        let path = self.path(key)?;
        match self.inner.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(_) => Err(unavailable()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreQuarantine(ObjectStoreArea);

impl ObjectStoreQuarantine {
    pub fn new(inner: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Result<Self, StoreError> {
        ObjectStoreArea::new(inner, prefix).map(Self)
    }
}

#[async_trait]
impl QuarantineStore for ObjectStoreQuarantine {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.0.read(key).await
    }

    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        self.0.put_immutable(key, bytes).await
    }

    async fn delete_exact(&self, key: &str) -> Result<(), StoreError> {
        self.0.delete_exact(key).await
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreProtected(ObjectStoreArea);

impl ObjectStoreProtected {
    pub fn new(inner: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Result<Self, StoreError> {
        ObjectStoreArea::new(inner, prefix).map(Self)
    }
}

#[async_trait]
impl ProtectedStore for ObjectStoreProtected {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.0.read(key).await
    }

    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError> {
        self.0.put_immutable(key, bytes).await
    }
}

#[derive(Debug, Clone)]
pub struct ContentVaultStores {
    quarantine: Arc<dyn QuarantineStore>,
    protected: Arc<dyn ProtectedStore>,
}

impl ContentVaultStores {
    pub fn from_object_store(inner: Arc<dyn ObjectStore>) -> Result<Self, StoreError> {
        Self::with_prefixes(inner, DEFAULT_QUARANTINE_PREFIX, DEFAULT_PROTECTED_PREFIX)
    }

    pub fn with_prefixes(
        inner: Arc<dyn ObjectStore>,
        quarantine_prefix: impl Into<String>,
        protected_prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let quarantine_prefix = quarantine_prefix.into().trim_matches('/').to_owned();
        let protected_prefix = protected_prefix.into().trim_matches('/').to_owned();
        if prefixes_overlap(&quarantine_prefix, &protected_prefix) {
            return Err(StoreError::new(
                StoreErrorKind::InvalidKey,
                "quarantine and protected object-store prefixes must not overlap",
            ));
        }
        Ok(Self {
            quarantine: Arc::new(ObjectStoreQuarantine::new(
                inner.clone(),
                quarantine_prefix,
            )?),
            protected: Arc::new(ObjectStoreProtected::new(inner, protected_prefix)?),
        })
    }

    pub fn quarantine(&self) -> Arc<dyn QuarantineStore> {
        self.quarantine.clone()
    }

    pub fn protected(&self) -> Arc<dyn ProtectedStore> {
        self.protected.clone()
    }

    #[cfg(feature = "s3")]
    pub fn from_s3_env() -> Result<Self, StoreError> {
        use object_store::aws::AmazonS3Builder;

        let bucket = s3_bucket_name(std::env::var(CONTENT_VAULT_S3_BUCKET_ENV))?;
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|_| {
                StoreError::new(
                    StoreErrorKind::Unavailable,
                    "content vault S3 configuration is invalid",
                )
            })?;
        Self::from_object_store(Arc::new(store))
    }
}

#[cfg(feature = "s3")]
fn s3_bucket_name(value: Result<String, std::env::VarError>) -> Result<String, StoreError> {
    let bucket = value.map_err(|_| {
        StoreError::new(
            StoreErrorKind::Unavailable,
            format!("{CONTENT_VAULT_S3_BUCKET_ENV} is required for S3 storage"),
        )
    })?;
    if bucket.trim().is_empty() {
        return Err(StoreError::new(
            StoreErrorKind::Unavailable,
            format!("{CONTENT_VAULT_S3_BUCKET_ENV} must not be empty"),
        ));
    }
    Ok(bucket)
}

fn prefixes_overlap(left: &str, right: &str) -> bool {
    left.is_empty()
        || right.is_empty()
        || left == right
        || left
            .strip_prefix(right)
            .is_some_and(|rest| rest.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn valid_key(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn unavailable() -> StoreError {
    StoreError::new(StoreErrorKind::Unavailable, "object store is unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn create_only_adapter_accepts_same_bytes_and_rejects_different_bytes() {
        let store = Arc::new(InMemory::new());
        let quarantine = ObjectStoreQuarantine::new(store, "quarantine").unwrap();

        assert_eq!(
            quarantine
                .put_immutable("tenant/session", b"first".to_vec())
                .await
                .unwrap(),
            ImmutablePut::Created
        );
        assert_eq!(
            quarantine
                .put_immutable("tenant/session", b"first".to_vec())
                .await
                .unwrap(),
            ImmutablePut::AlreadyPresent
        );
        assert_eq!(
            quarantine
                .put_immutable("tenant/session", b"second".to_vec())
                .await
                .unwrap_err()
                .kind(),
            StoreErrorKind::ImmutableConflict
        );
    }

    #[test]
    fn store_capabilities_require_disjoint_non_empty_prefixes() {
        let store = Arc::new(InMemory::new());
        for (quarantine, protected) in [
            ("", "protected"),
            ("quarantine", ""),
            ("shared", "shared"),
            ("shared", "shared/protected"),
        ] {
            assert_eq!(
                ContentVaultStores::with_prefixes(store.clone(), quarantine, protected)
                    .unwrap_err()
                    .kind(),
                StoreErrorKind::InvalidKey
            );
        }

        ContentVaultStores::with_prefixes(store, "quarantine", "protected").unwrap();
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_configuration_fails_closed_without_a_bucket() {
        assert_eq!(
            s3_bucket_name(Err(std::env::VarError::NotPresent))
                .unwrap_err()
                .kind(),
            StoreErrorKind::Unavailable
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_configuration_rejects_an_empty_bucket() {
        assert_eq!(
            s3_bucket_name(Ok("   ".to_owned())).unwrap_err().kind(),
            StoreErrorKind::Unavailable
        );
    }
}
