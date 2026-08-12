use lenso::host::Migration;

pub const CONTENT_VAULT_MIGRATIONS: &[Migration] = &[Migration {
    name: "content-vault/0001_create_content_vault_schema",
    sql: include_str!("../migrations/0001_create_content_vault_schema.sql"),
}];
