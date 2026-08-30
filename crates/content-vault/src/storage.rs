use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, stream::BoxStream};
use object_store::{ObjectStore, PutMode, PutOptions, WriteMultipart, path::Path as ObjectPath};
use std::sync::Arc;
use uuid::Uuid;

pub const DEFAULT_QUARANTINE_PREFIX: &str = "content-vault/quarantine";
pub const DEFAULT_PROTECTED_PREFIX: &str = "content-vault/protected";

// S3-compatible multipart parts must normally be at least 5 MiB. Keeping a single 8 MiB part in
// flight bounds adapter-owned memory while remaining portable across supported object stores.
const STREAMING_CHUNK_SIZE_BYTES: usize = 8 * 1024 * 1024;
const STREAMING_UPLOAD_CONCURRENCY: usize = 1;
/// Relative namespace used for streaming attempts inside each configured storage area.
///
/// Completed calls delete their exact attempt object. Deployments must also expire this prefix
/// and abandoned multipart uploads as a backstop for process termination during an upload.
pub const STREAMING_ATTEMPT_PREFIX: &str = ".content-vault-attempts";

pub type StoreByteStream = BoxStream<'static, Result<Bytes, StoreError>>;

pub struct StoreRead {
    size_bytes: u64,
    stream: StoreByteStream,
}

impl std::fmt::Debug for StoreRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreRead")
            .field("size_bytes", &self.size_bytes)
            .finish_non_exhaustive()
    }
}

impl StoreRead {
    /// Creates a streamed object result.
    ///
    /// Adapters must yield exactly `size_bytes` bytes or yield an error before ending the stream.
    pub const fn new(size_bytes: u64, stream: StoreByteStream) -> Self {
        Self { size_bytes, stream }
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn into_stream(self) -> StoreByteStream {
        self.stream
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImmutablePut {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreErrorKind {
    Unavailable,
    ImmutableConflict,
    InvalidLength,
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

    async fn read_stream(&self, _key: &str) -> Result<Option<StoreRead>, StoreError> {
        Err(streaming_unsupported())
    }

    async fn put_stream_immutable(
        &self,
        _key: &str,
        _expected_size_bytes: u64,
        _bytes: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        Err(streaming_unsupported())
    }
}

#[async_trait]
pub trait ProtectedStore: std::fmt::Debug + Send + Sync {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<ImmutablePut, StoreError>;

    async fn read_stream(&self, _key: &str) -> Result<Option<StoreRead>, StoreError> {
        Err(streaming_unsupported())
    }

    async fn put_stream_immutable(
        &self,
        _key: &str,
        _expected_size_bytes: u64,
        _bytes: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        Err(streaming_unsupported())
    }
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

    fn attempt_path(&self) -> Result<ObjectPath, StoreError> {
        self.path(&format!("{STREAMING_ATTEMPT_PREFIX}/{}", Uuid::now_v7()))
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

    async fn read_stream(&self, key: &str) -> Result<Option<StoreRead>, StoreError> {
        let path = self.path(key)?;
        let metadata = match self.inner.head(&path).await {
            Ok(metadata) => metadata,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => return Err(unavailable()),
        };
        let size_bytes = metadata.size;
        let stream = futures::stream::try_unfold(
            (self.inner.clone(), path, 0_u64, size_bytes),
            |(store, path, offset, size)| async move {
                if offset >= size {
                    return Ok(None);
                }
                let end = offset
                    .saturating_add(STREAMING_CHUNK_SIZE_BYTES as u64)
                    .min(size);
                let bytes = store
                    .get_range(&path, offset..end)
                    .await
                    .map_err(|_| unavailable())?;
                Ok(Some((bytes, (store, path, end, size))))
            },
        )
        .boxed();
        Ok(Some(StoreRead::new(size_bytes, stream)))
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

    async fn put_stream_immutable(
        &self,
        key: &str,
        expected_size_bytes: u64,
        mut bytes: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        let destination = self.path(key)?;
        let attempt = self.attempt_path()?;
        let mut received_size_bytes = 0_u64;

        let first = loop {
            match bytes.next().await {
                Some(Ok(chunk)) if chunk.is_empty() => {}
                Some(Ok(chunk)) => {
                    received_size_bytes = checked_stream_length(
                        received_size_bytes,
                        chunk.len(),
                        expected_size_bytes,
                    )?;
                    break Some(chunk);
                }
                Some(Err(error)) => return Err(error),
                None => break None,
            }
        };

        if let Some(first) = first {
            let upload = self
                .inner
                .put_multipart(&attempt)
                .await
                .map_err(|_| unavailable())?;
            let mut upload =
                WriteMultipart::new_with_chunk_size(upload, STREAMING_CHUNK_SIZE_BYTES);

            if let Err(error) = put_bounded_chunk(&mut upload, first).await {
                return self.abort_and_clean(upload, &attempt, error).await;
            }
            while let Some(next) = bytes.next().await {
                let chunk = match next {
                    Ok(chunk) => chunk,
                    Err(error) => return self.abort_and_clean(upload, &attempt, error).await,
                };
                received_size_bytes = match checked_stream_length(
                    received_size_bytes,
                    chunk.len(),
                    expected_size_bytes,
                ) {
                    Ok(length) => length,
                    Err(error) => return self.abort_and_clean(upload, &attempt, error).await,
                };
                if let Err(error) = put_bounded_chunk(&mut upload, chunk).await {
                    return self.abort_and_clean(upload, &attempt, error).await;
                }
            }
            if received_size_bytes != expected_size_bytes {
                return self
                    .abort_and_clean(upload, &attempt, invalid_stream_length())
                    .await;
            }
            if upload.finish().await.is_err() {
                return self.clean_after_failed_upload(&attempt).await;
            }
        } else {
            if expected_size_bytes != 0 {
                return Err(invalid_stream_length());
            }
            if self.inner.put(&attempt, Bytes::new().into()).await.is_err() {
                return Err(unavailable());
            }
        }

        let result = match self.inner.copy_if_not_exists(&attempt, &destination).await {
            Ok(()) => Ok(ImmutablePut::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                match self.objects_equal(&attempt, &destination).await {
                    Ok(true) => Ok(ImmutablePut::AlreadyPresent),
                    Ok(false) => Err(StoreError::new(
                        StoreErrorKind::ImmutableConflict,
                        "immutable object key already contains different bytes",
                    )),
                    Err(error) => Err(error),
                }
            }
            Err(_) => Err(unavailable()),
        };

        self.delete_path(&attempt).await?;
        result
    }

    async fn objects_equal(
        &self,
        left: &ObjectPath,
        right: &ObjectPath,
    ) -> Result<bool, StoreError> {
        let left_metadata = self.inner.head(left).await.map_err(|_| unavailable())?;
        let right_metadata = self.inner.head(right).await.map_err(|_| unavailable())?;
        if left_metadata.size != right_metadata.size {
            return Ok(false);
        }

        let mut offset = 0_u64;
        while offset < left_metadata.size {
            let end = offset
                .saturating_add(STREAMING_CHUNK_SIZE_BYTES as u64)
                .min(left_metadata.size);
            let (left_bytes, right_bytes) = futures::try_join!(
                self.inner.get_range(left, offset..end),
                self.inner.get_range(right, offset..end),
            )
            .map_err(|_| unavailable())?;
            if left_bytes != right_bytes {
                return Ok(false);
            }
            offset = end;
        }
        Ok(true)
    }

    async fn abort_and_clean(
        &self,
        upload: WriteMultipart,
        attempt: &ObjectPath,
        source: StoreError,
    ) -> Result<ImmutablePut, StoreError> {
        let abort_result = upload.abort().await;
        let delete_result = self.delete_path(attempt).await;
        if abort_result.is_err() || delete_result.is_err() {
            Err(unavailable())
        } else {
            Err(source)
        }
    }

    async fn clean_after_failed_upload(
        &self,
        attempt: &ObjectPath,
    ) -> Result<ImmutablePut, StoreError> {
        self.delete_path(attempt).await?;
        Err(unavailable())
    }

    async fn delete_path(&self, path: &ObjectPath) -> Result<(), StoreError> {
        match self.inner.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
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

    async fn read_stream(&self, key: &str) -> Result<Option<StoreRead>, StoreError> {
        self.0.read_stream(key).await
    }

    async fn put_stream_immutable(
        &self,
        key: &str,
        expected_size_bytes: u64,
        bytes: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        self.0
            .put_stream_immutable(key, expected_size_bytes, bytes)
            .await
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

    async fn read_stream(&self, key: &str) -> Result<Option<StoreRead>, StoreError> {
        self.0.read_stream(key).await
    }

    async fn put_stream_immutable(
        &self,
        key: &str,
        expected_size_bytes: u64,
        bytes: StoreByteStream,
    ) -> Result<ImmutablePut, StoreError> {
        self.0
            .put_stream_immutable(key, expected_size_bytes, bytes)
            .await
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
}

async fn put_bounded_chunk(
    upload: &mut WriteMultipart,
    mut chunk: Bytes,
) -> Result<(), StoreError> {
    while !chunk.is_empty() {
        upload
            .wait_for_capacity(STREAMING_UPLOAD_CONCURRENCY)
            .await
            .map_err(|_| unavailable())?;
        let length = chunk.len().min(STREAMING_CHUNK_SIZE_BYTES);
        upload.put(chunk.split_to(length));
    }
    Ok(())
}

fn checked_stream_length(
    received_size_bytes: u64,
    chunk_size_bytes: usize,
    expected_size_bytes: u64,
) -> Result<u64, StoreError> {
    let chunk_size_bytes = u64::try_from(chunk_size_bytes).map_err(|_| invalid_stream_length())?;
    let received_size_bytes = received_size_bytes
        .checked_add(chunk_size_bytes)
        .ok_or_else(invalid_stream_length)?;
    if received_size_bytes > expected_size_bytes {
        return Err(invalid_stream_length());
    }
    Ok(received_size_bytes)
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

fn streaming_unsupported() -> StoreError {
    StoreError::new(
        StoreErrorKind::Unavailable,
        "object-store adapter does not support streaming I/O",
    )
}

fn invalid_stream_length() -> StoreError {
    StoreError::new(
        StoreErrorKind::InvalidLength,
        "stream length does not match its declared size",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{TryStreamExt as _, stream};
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

    #[tokio::test]
    async fn streaming_adapter_promotes_immutably_and_removes_attempt_objects() {
        let store = Arc::new(InMemory::new());
        let quarantine = ObjectStoreQuarantine::new(store.clone(), "quarantine").unwrap();

        let first = stream::iter([
            Ok(Bytes::from_static(b"streamed ")),
            Ok(Bytes::from_static(b"content")),
        ])
        .boxed();
        assert_eq!(
            quarantine
                .put_stream_immutable("tenant/session", 16, first)
                .await
                .unwrap(),
            ImmutablePut::Created
        );

        let read = quarantine
            .read_stream("tenant/session")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.size_bytes(), 16);
        assert_eq!(
            read.into_stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .concat(),
            b"streamed content"
        );

        let same = stream::iter([Ok(Bytes::from_static(b"streamed content"))]).boxed();
        assert_eq!(
            quarantine
                .put_stream_immutable("tenant/session", 16, same)
                .await
                .unwrap(),
            ImmutablePut::AlreadyPresent
        );

        let different = stream::iter([Ok(Bytes::from_static(b"different"))]).boxed();
        assert_eq!(
            quarantine
                .put_stream_immutable("tenant/session", 9, different)
                .await
                .unwrap_err()
                .kind(),
            StoreErrorKind::ImmutableConflict
        );

        let objects = store
            .list(Some(&ObjectPath::from("quarantine")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].location.as_ref(), "quarantine/tenant/session");
    }

    #[tokio::test]
    async fn streaming_adapter_aborts_and_cleans_an_interrupted_source() {
        let store = Arc::new(InMemory::new());
        let quarantine = ObjectStoreQuarantine::new(store.clone(), "quarantine").unwrap();
        let source_error = StoreError::new(StoreErrorKind::Unavailable, "source failed");
        let source = stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(source_error.clone()),
        ])
        .boxed();

        assert_eq!(
            quarantine
                .put_stream_immutable("tenant/session", 14, source)
                .await
                .unwrap_err(),
            source_error
        );
        assert!(
            store
                .list(Some(&ObjectPath::from("quarantine")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn streaming_adapter_rejects_a_truncated_stream_and_cleans_its_attempt() {
        let store = Arc::new(InMemory::new());
        let quarantine = ObjectStoreQuarantine::new(store.clone(), "quarantine").unwrap();
        let source = stream::iter([Ok(Bytes::from_static(b"short"))]).boxed();

        assert_eq!(
            quarantine
                .put_stream_immutable("tenant/session", 6, source)
                .await
                .unwrap_err()
                .kind(),
            StoreErrorKind::InvalidLength
        );
        assert!(
            store
                .list(Some(&ObjectPath::from("quarantine")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn streamed_reads_never_yield_more_than_the_fixed_chunk_bound() {
        let store = Arc::new(InMemory::new());
        let protected = ObjectStoreProtected::new(store, "protected").unwrap();
        protected
            .put_immutable("tenant/content", vec![7_u8; STREAMING_CHUNK_SIZE_BYTES + 1])
            .await
            .unwrap();

        let read = protected
            .read_stream("tenant/content")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            read.into_stream()
                .map_ok(|chunk| chunk.len())
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![STREAMING_CHUNK_SIZE_BYTES, 1]
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
}
