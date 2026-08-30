use lenso::host::Migration;

pub const CONTENT_VAULT_MIGRATIONS: &[Migration] = &[
    Migration {
        name: "content-vault/0001_create_content_vault_schema",
        sql: include_str!("../migrations/0001_create_content_vault_schema.sql"),
    },
    Migration {
        name: "content-vault/0002_streaming_io",
        sql: include_str!("../migrations/0002_streaming_io.sql"),
    },
    Migration {
        name: "content-vault/0003_pending_quarantine_cleanup_indexes",
        sql: include_str!("../migrations/0003_pending_quarantine_cleanup_indexes.sql"),
    },
];
