//! Authoritative source for the Content Vault Capability contract.

use lenso_contract_authoring as lenso;

#[derive(serde::Deserialize)]
pub struct NullableOffset(Option<u64>);

impl lenso::JsonSchema for NullableOffset {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NullableOffset".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        "NullableOffset".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let mut schema = <Option<u64> as lenso::JsonSchema>::json_schema(generator);
        schema
            .as_object_mut()
            .expect("nullable scalar schema is an object")
            .insert("maximum".to_owned(), serde_json::json!(1_073_741_824_u64));
        schema
    }
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct Owner {
    #[schemars(
        length(min = 1, max = 200),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")
    )]
    pub plugin_instance: String,
    #[schemars(
        length(min = 1, max = 200),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub resource_type: String,
    #[schemars(
        length(min = 1, max = 500),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub resource_id: String,
    #[schemars(
        length(min = 1, max = 500),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub revision_id: Option<String>,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct OwnerGrant {
    #[schemars(
        length(min = 1, max = 200),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub tenant_id: String,
    pub owner: Owner,
    #[schemars(
        length(min = 1, max = 500),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub actor_id: String,
    #[schemars(
        length(min = 1, max = 500),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub correlation_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ContentDescriptor {
    #[schemars(
        length(min = 36, max = 36),
        regex(
            pattern = r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1-8][0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
        )
    )]
    pub content_id: String,
    #[schemars(length(min = 64, max = 64), regex(pattern = r"^[0-9a-f]{64}$"))]
    pub sha256: String,
    #[schemars(range(min = 1, max = 1_073_741_824))]
    pub size_bytes: u64,
    #[schemars(
        length(min = 9, max = 10),
        regex(pattern = r"^(?:image/png|image/jpeg|text/plain)$")
    )]
    pub media_type: String,
    #[schemars(length(min = 1, max = 64), extend("format" = "date-time"))]
    pub created_at: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadState {
    Reserved,
    Staging,
    Committed,
    Rejected,
    Expired,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReserveRequest {
    pub grant: OwnerGrant,
    #[schemars(
        length(min = 1, max = 300),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub idempotency_key: String,
    #[schemars(length(min = 64, max = 64), regex(pattern = r"^[0-9a-f]{64}$"))]
    pub expected_sha256: String,
    #[schemars(range(min = 1, max = 1_073_741_824))]
    pub expected_size_bytes: u64,
    #[schemars(length(min = 10, max = 10), regex(pattern = r"^text/plain$"))]
    pub media_type: String,
    #[schemars(range(min = 60, max = 86_400))]
    pub ttl_seconds: u32,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReserveResponse {
    #[schemars(
        length(min = 36, max = 36),
        regex(
            pattern = r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1-8][0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
        )
    )]
    pub session_id: String,
    pub state: UploadState,
    #[schemars(length(min = 64, max = 64), regex(pattern = r"^[0-9a-f]{64}$"))]
    pub expected_sha256: String,
    #[schemars(range(min = 1, max = 1_073_741_824))]
    pub expected_size_bytes: u64,
    #[schemars(length(min = 10, max = 10), regex(pattern = r"^text/plain$"))]
    pub media_type: String,
    #[schemars(length(min = 1, max = 64), extend("format" = "date-time"))]
    pub expires_at: String,
    #[schemars(range(min = 0, max = 1_073_741_824))]
    pub next_offset: u64,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DescribeRequest {
    pub grant: OwnerGrant,
    #[schemars(
        length(min = 36, max = 36),
        regex(
            pattern = r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1-8][0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
        )
    )]
    pub content_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DescribeResponse {
    pub content: ContentDescriptor,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ClaimRequest {
    pub grant: OwnerGrant,
    pub target: Owner,
    #[schemars(
        length(min = 36, max = 36),
        regex(
            pattern = r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1-8][0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
        )
    )]
    pub content_id: String,
    #[schemars(
        length(min = 1, max = 100),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub role: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ClaimResponse {
    pub active: bool,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReleaseClaimRequest {
    pub grant: OwnerGrant,
    pub target: Owner,
    #[schemars(
        length(min = 36, max = 36),
        regex(
            pattern = r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1-8][0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
        )
    )]
    pub content_id: String,
    #[schemars(
        length(min = 1, max = 100),
        regex(pattern = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$")
    )]
    pub role: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct ReleaseClaimResponse {
    pub released: bool,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct SweepRequest {}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct SweepResponse {
    #[schemars(range(min = 0, max = 1_000))]
    pub expired_sessions: u64,
    #[schemars(range(min = 0, max = 1_000))]
    pub cleaned_objects: u64,
    #[schemars(range(min = 0, max = 1_000))]
    pub failed_objects: u64,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct UploadRequest {
    pub grant: OwnerGrant,
    #[schemars(
        length(min = 36, max = 36),
        regex(
            pattern = r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1-8][0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
        )
    )]
    pub session_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadFrameKind {
    Chunk,
    Committed,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
#[schemars(transform = upload_frame_schema)]
pub struct UploadFrame {
    pub kind: UploadFrameKind,
    pub offset: NullableOffset,
    #[schemars(
        length(min = 4, max = 11_184_812),
        regex(pattern = r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"),
        extend("x-lenso-sensitive" = true)
    )]
    pub bytes_base64: Option<String>,
    pub content: Option<ContentDescriptor>,
}

fn upload_frame_schema(schema: &mut schemars::Schema) {
    schema.insert(
        "if".to_owned(),
        serde_json::json!({ "properties": { "kind": { "const": "chunk" } } }),
    );
    schema.insert(
        "then".to_owned(),
        serde_json::json!({
            "required": ["bytes_base64"],
            "properties": {
                "offset": { "not": { "type": "null" } },
                "bytes_base64": { "not": { "type": "null" } },
                "content": { "type": "null" }
            }
        }),
    );
    schema.insert(
        "else".to_owned(),
        serde_json::json!({
            "required": ["content"],
            "properties": {
                "offset": { "not": { "type": "null" } },
                "bytes_base64": { "type": "null" },
                "content": { "not": { "type": "null" } }
            }
        }),
    );
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DownloadRequest {
    pub grant: OwnerGrant,
    #[schemars(
        length(min = 36, max = 36),
        regex(
            pattern = r"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[1-8][0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
        )
    )]
    pub content_id: String,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadFrameKind {
    Descriptor,
    Chunk,
}

#[derive(lenso::JsonSchema, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
#[schemars(transform = download_frame_schema)]
pub struct DownloadFrame {
    pub kind: DownloadFrameKind,
    pub offset: NullableOffset,
    #[schemars(
        length(min = 4, max = 11_184_812),
        regex(pattern = r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"),
        extend("x-lenso-sensitive" = true)
    )]
    pub bytes_base64: Option<String>,
    pub content: Option<ContentDescriptor>,
}

fn download_frame_schema(schema: &mut schemars::Schema) {
    schema.insert(
        "if".to_owned(),
        serde_json::json!({ "properties": { "kind": { "const": "descriptor" } } }),
    );
    schema.insert(
        "then".to_owned(),
        serde_json::json!({
            "required": ["content"],
            "properties": {
                "offset": { "const": 0 },
                "bytes_base64": { "type": "null" },
                "content": { "not": { "type": "null" } }
            }
        }),
    );
    schema.insert(
        "else".to_owned(),
        serde_json::json!({
            "required": ["bytes_base64"],
            "properties": {
                "offset": { "not": { "type": "null" } },
                "bytes_base64": { "not": { "type": "null" } },
                "content": { "type": "null" }
            }
        }),
    );
}

#[derive(lenso::DomainError)]
pub enum ContentVaultError {
    Unauthorized,
    InvalidInput,
    Conflict,
    NotFound,
    UploadMissing,
    UploadInterrupted,
    UploadRejected,
    UploadExpired,
    IntegrityMissing,
    IntegrityMismatch,
}

#[lenso::capability(
    id = "lenso.content-vault",
    major = 1,
    version = "1.0.0",
    portable = true,
    cross_lane_transfer = false
)]
pub trait ContentVault {
    async fn reserve(
        &self,
        context: lenso::Ctx<'_>,
        request: ReserveRequest,
    ) -> Result<ReserveResponse, ContentVaultError>;

    async fn describe(
        &self,
        context: lenso::Ctx<'_>,
        request: DescribeRequest,
    ) -> Result<DescribeResponse, ContentVaultError>;

    async fn claim(
        &self,
        context: lenso::Ctx<'_>,
        request: ClaimRequest,
    ) -> Result<ClaimResponse, ContentVaultError>;

    async fn release_claim(
        &self,
        context: lenso::Ctx<'_>,
        request: ReleaseClaimRequest,
    ) -> Result<ReleaseClaimResponse, ContentVaultError>;

    async fn sweep(
        &self,
        context: lenso::Ctx<'_>,
        request: SweepRequest,
    ) -> Result<SweepResponse, ContentVaultError>;

    async fn upload(
        &self,
        context: lenso::Ctx<'_>,
        request: UploadRequest,
    ) -> lenso::Stream<UploadFrame, ContentVaultError>;

    async fn download(
        &self,
        context: lenso::Ctx<'_>,
        request: DownloadRequest,
    ) -> lenso::Stream<DownloadFrame, ContentVaultError>;
}
