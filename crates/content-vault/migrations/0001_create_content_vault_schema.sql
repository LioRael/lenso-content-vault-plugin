CREATE SCHEMA IF NOT EXISTS content_vault;

CREATE TABLE content_vault.upload_sessions (
    session_id uuid PRIMARY KEY,
    tenant_id varchar(200) NOT NULL,
    owner_module varchar(200) NOT NULL,
    owner_resource_type varchar(200) NOT NULL,
    owner_resource_id varchar(500) NOT NULL,
    owner_revision_id varchar(500) NOT NULL DEFAULT '',
    actor_id varchar(500) NOT NULL,
    correlation_id varchar(500) NOT NULL,
    expected_sha256 char(64) NOT NULL,
    expected_size_bytes bigint NOT NULL CHECK (expected_size_bytes > 0),
    expected_media_type varchar(200) NOT NULL,
    quarantine_key text NOT NULL UNIQUE,
    staging_token uuid NOT NULL UNIQUE,
    staging_started_at timestamptz,
    candidate_content_id uuid NOT NULL UNIQUE,
    committed_content_id uuid,
    state varchar(32) NOT NULL CHECK (state IN ('reserved', 'staging', 'committed', 'rejected', 'expired')),
    terminal_reason varchar(500),
    expires_at timestamptz NOT NULL,
    terminal_at timestamptz,
    quarantine_cleanup_attempted_at timestamptz,
    quarantine_cleanup_succeeded_at timestamptz,
    quarantine_cleanup_attempts bigint NOT NULL DEFAULT 0 CHECK (quarantine_cleanup_attempts >= 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CHECK (expected_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((state = 'staging' AND staging_started_at IS NOT NULL) OR (state <> 'staging' AND staging_started_at IS NULL)),
    CHECK ((state IN ('reserved', 'staging') AND terminal_at IS NULL) OR (state NOT IN ('reserved', 'staging') AND terminal_at IS NOT NULL)),
    CHECK ((state = 'committed' AND committed_content_id IS NOT NULL) OR (state <> 'committed' AND committed_content_id IS NULL))
);

CREATE INDEX upload_sessions_expiry_idx
    ON content_vault.upload_sessions (expires_at, session_id)
    WHERE state IN ('reserved', 'staging');

CREATE INDEX upload_sessions_quarantine_cleanup_idx
    ON content_vault.upload_sessions (quarantine_cleanup_attempted_at, terminal_at, session_id)
    WHERE state IN ('committed', 'rejected', 'expired');

CREATE TABLE content_vault.blobs (
    blob_id uuid PRIMARY KEY,
    tenant_id varchar(200) NOT NULL,
    sha256 char(64) NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    media_type varchar(200) NOT NULL,
    protected_key text NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (tenant_id, blob_id),
    UNIQUE (tenant_id, sha256),
    UNIQUE (tenant_id, protected_key),
    CHECK (sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE content_vault.content_objects (
    content_id uuid PRIMARY KEY,
    tenant_id varchar(200) NOT NULL,
    blob_id uuid NOT NULL,
    committed_by_owner_module varchar(200) NOT NULL,
    committed_by_owner_resource_type varchar(200) NOT NULL,
    committed_by_owner_resource_id varchar(500) NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (tenant_id, content_id),
    FOREIGN KEY (tenant_id, blob_id)
        REFERENCES content_vault.blobs (tenant_id, blob_id)
);

ALTER TABLE content_vault.upload_sessions
    ADD CONSTRAINT upload_sessions_committed_content_fk
    FOREIGN KEY (tenant_id, committed_content_id)
    REFERENCES content_vault.content_objects (tenant_id, content_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE content_vault.content_claims (
    claim_id uuid PRIMARY KEY,
    tenant_id varchar(200) NOT NULL,
    content_id uuid NOT NULL,
    owner_module varchar(200) NOT NULL,
    owner_resource_type varchar(200) NOT NULL,
    owner_resource_id varchar(500) NOT NULL,
    owner_revision_id varchar(500) NOT NULL DEFAULT '',
    role varchar(100) NOT NULL,
    state varchar(32) NOT NULL CHECK (state IN ('active', 'released')),
    created_at timestamptz NOT NULL,
    released_at timestamptz,
    FOREIGN KEY (tenant_id, content_id)
        REFERENCES content_vault.content_objects (tenant_id, content_id),
    CHECK ((state = 'active' AND released_at IS NULL) OR (state = 'released' AND released_at IS NOT NULL))
);

CREATE UNIQUE INDEX content_claims_active_owner_idx
    ON content_vault.content_claims (
        tenant_id,
        content_id,
        owner_module,
        owner_resource_type,
        owner_resource_id,
        owner_revision_id,
        role
    )
    WHERE state = 'active';

CREATE INDEX content_claims_owner_lookup_idx
    ON content_vault.content_claims (
        tenant_id,
        owner_module,
        owner_resource_type,
        owner_resource_id,
        owner_revision_id,
        state
    );

CREATE TABLE content_vault.command_receipts (
    scope text NOT NULL,
    idempotency_key varchar(300) NOT NULL,
    tenant_id varchar(200) NOT NULL,
    operation varchar(100) NOT NULL,
    request_digest char(64) NOT NULL,
    response jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (scope, idempotency_key),
    CHECK (request_digest ~ '^[0-9a-f]{64}$')
);

CREATE TABLE content_vault.integrity_observations (
    observation_id uuid PRIMARY KEY,
    tenant_id varchar(200) NOT NULL,
    content_id uuid NOT NULL,
    kind varchar(50) NOT NULL CHECK (kind IN ('missing', 'digest_mismatch', 'size_mismatch')),
    expected_sha256 char(64) NOT NULL,
    actual_sha256 char(64),
    expected_size_bytes bigint NOT NULL,
    actual_size_bytes bigint,
    observed_at timestamptz NOT NULL,
    FOREIGN KEY (tenant_id, content_id)
        REFERENCES content_vault.content_objects (tenant_id, content_id),
    CHECK (expected_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (actual_sha256 IS NULL OR actual_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX integrity_observations_content_idx
    ON content_vault.integrity_observations (tenant_id, content_id, observed_at DESC);
