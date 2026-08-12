use crate::migrations::CONTENT_VAULT_MIGRATIONS;
use lenso::host::HostLinkedModule;
use lenso::{ModuleManifest, ModuleMigrationActivation, ModuleMigrationDeclaration};

pub const MODULE_NAME: &str = "content-vault";
pub const MODULE_ID: &str = "lenso/content-vault";

pub fn manifest() -> ModuleManifest {
    ModuleManifest::builder(MODULE_ID)
        .summary(
            "Tenant-scoped quarantine, validation, and immutable content references for linked Modules.",
        )
        .migrations(vec![
            ModuleMigrationDeclaration {
                migration_id: "content-vault/0001_create_content_vault_schema".to_owned(),
                order: 1,
                store: "host".to_owned(),
                destructive: false,
                reversible: false,
                activation: ModuleMigrationActivation::BeforeActivation,
            },
            ModuleMigrationDeclaration {
                migration_id: "content-vault/0002_streaming_io".to_owned(),
                order: 2,
                store: "host".to_owned(),
                destructive: false,
                reversible: false,
                activation: ModuleMigrationActivation::BeforeActivation,
            },
        ])
        .build()
}

pub fn linked_module() -> HostLinkedModule {
    HostLinkedModule::manifest_only(MODULE_NAME, manifest, CONTENT_VAULT_MIGRATIONS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso::host::HostBuilder;
    use lenso::{ModuleManifestLintSeverity, lint_module_manifest};

    #[test]
    fn manifest_is_lint_clean() {
        let manifest = manifest();
        let non_ok = lint_module_manifest(&manifest)
            .into_iter()
            .filter(|lint| lint.severity != ModuleManifestLintSeverity::Ok)
            .collect::<Vec<_>>();
        assert!(non_ok.is_empty(), "manifest lints: {non_ok:#?}");
        assert_eq!(manifest.migrations.len(), CONTENT_VAULT_MIGRATIONS.len());
        for (declaration, migration) in manifest.migrations.iter().zip(CONTENT_VAULT_MIGRATIONS) {
            assert_eq!(declaration.store, "host");
            assert_eq!(declaration.migration_id, migration.name);
        }
    }

    #[test]
    fn host_wiring_contains_the_linked_module_and_migration() {
        let composition = HostBuilder::new().linked_module(linked_module()).build();
        let linked = composition.linked_modules();
        assert_eq!(linked.len(), 1);
        assert_eq!(linked[0].module_name, MODULE_NAME);
        assert_eq!(linked[0].migrations.len(), CONTENT_VAULT_MIGRATIONS.len());
        assert_eq!(
            linked[0].migrations[0].name,
            CONTENT_VAULT_MIGRATIONS[0].name
        );
        assert_eq!(linked[0].migrations[0].sql, CONTENT_VAULT_MIGRATIONS[0].sql);
        assert!(linked[0].load.is_none());
        assert!(linked[0].http_binding.is_none());
    }
}
