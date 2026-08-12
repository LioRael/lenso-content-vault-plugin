ALTER TABLE content_vault.upload_sessions
    ADD COLUMN ingestion_mode varchar(32) NOT NULL DEFAULT 'buffered'
        CHECK (ingestion_mode IN ('buffered', 'streaming')),
    ADD COLUMN stream_chunk_size_bytes bigint;

ALTER TABLE content_vault.upload_sessions
    ADD CONSTRAINT upload_sessions_streaming_shape_check
    CHECK (
        (ingestion_mode = 'buffered' AND stream_chunk_size_bytes IS NULL)
        OR
        (ingestion_mode = 'streaming' AND stream_chunk_size_bytes > 0)
    );

CREATE TABLE content_vault.upload_parts (
    part_id uuid PRIMARY KEY,
    session_id uuid NOT NULL
        REFERENCES content_vault.upload_sessions (session_id),
    part_index integer NOT NULL CHECK (part_index >= 0),
    byte_offset bigint NOT NULL CHECK (byte_offset >= 0),
    size_bytes bigint CHECK (size_bytes > 0),
    sha256 char(64),
    quarantine_key text NOT NULL UNIQUE,
    attempt_token uuid NOT NULL,
    state varchar(32) NOT NULL CHECK (state IN ('pending', 'uploaded', 'abandoned')),
    quarantine_cleanup_attempted_at timestamptz,
    quarantine_cleanup_succeeded_at timestamptz,
    quarantine_cleanup_attempts bigint NOT NULL DEFAULT 0
        CHECK (quarantine_cleanup_attempts >= 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CHECK (
        (state = 'uploaded' AND size_bytes IS NOT NULL AND sha256 IS NOT NULL)
        OR
        (state <> 'uploaded' AND size_bytes IS NULL AND sha256 IS NULL)
    ),
    CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX upload_parts_uploaded_index_idx
    ON content_vault.upload_parts (session_id, part_index)
    WHERE state = 'uploaded';

CREATE INDEX upload_parts_session_progress_idx
    ON content_vault.upload_parts (session_id, part_index, state);

CREATE INDEX upload_parts_abandoned_cleanup_idx
    ON content_vault.upload_parts (
        quarantine_cleanup_attempted_at,
        created_at,
        part_id
    )
    WHERE state = 'abandoned';

CREATE TABLE content_vault.blob_parts (
    tenant_id varchar(200) NOT NULL,
    blob_id uuid NOT NULL,
    part_index integer NOT NULL CHECK (part_index >= 0),
    byte_offset bigint NOT NULL CHECK (byte_offset >= 0),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    sha256 char(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id, blob_id, part_index),
    UNIQUE (tenant_id, blob_id, byte_offset),
    FOREIGN KEY (tenant_id, blob_id)
        REFERENCES content_vault.blobs (tenant_id, blob_id)
);
