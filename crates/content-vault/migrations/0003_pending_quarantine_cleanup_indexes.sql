ALTER TABLE content_vault.upload_parts
    ADD COLUMN writer_resolved_at timestamptz;

UPDATE content_vault.upload_parts
SET writer_resolved_at = updated_at
WHERE state = 'uploaded';

UPDATE content_vault.upload_parts
SET quarantine_cleanup_succeeded_at = NULL
WHERE writer_resolved_at IS NULL;

ALTER TABLE content_vault.upload_parts
    ADD CONSTRAINT upload_parts_writer_resolution_check
        CHECK (
            (state = 'pending' AND writer_resolved_at IS NULL)
            OR (state = 'uploaded' AND writer_resolved_at IS NOT NULL)
            OR state = 'abandoned'
        ),
    ADD CONSTRAINT upload_parts_cleanup_requires_resolved_writer_check
        CHECK (
            quarantine_cleanup_succeeded_at IS NULL
            OR writer_resolved_at IS NOT NULL
        );

CREATE INDEX upload_sessions_pending_quarantine_cleanup_idx
    ON content_vault.upload_sessions (
        quarantine_cleanup_attempted_at,
        terminal_at,
        session_id
    )
    WHERE state IN ('committed', 'rejected', 'expired')
      AND quarantine_cleanup_succeeded_at IS NULL;

CREATE INDEX upload_parts_pending_quarantine_cleanup_idx
    ON content_vault.upload_parts (
        session_id,
        state,
        created_at,
        part_id
    )
    WHERE quarantine_cleanup_succeeded_at IS NULL
       OR writer_resolved_at IS NULL;
