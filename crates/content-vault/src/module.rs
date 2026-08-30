#[cfg(any(feature = "s3", test))]
use crate::migrations::CONTENT_VAULT_MIGRATIONS;
#[cfg(any(feature = "s3", test))]
use crate::{
    ContentVault, ContentVaultError, ContentVaultErrorCode, ContentVaultStores, SweepReport,
};
#[cfg(any(feature = "s3", test))]
use async_trait::async_trait;
#[cfg(any(feature = "s3", test))]
use chrono::Duration as ChronoDuration;
#[cfg(feature = "s3")]
use lenso::host::HostLinkedModule;
#[cfg(any(feature = "s3", test))]
use lenso::host::runtime::{
    AppContext, AppError, AppResult, ErrorCode, ExecutionContext, FunctionDefinition,
    FunctionHandler, LinkedBinding, Module, RetryPolicy, RuntimeDescriptor,
};
use lenso::{
    ModuleConfigActivation, ModuleConfigContract, ModuleConfigField, ModuleConfigFieldType,
    ModuleConfigMutability, ModuleConfigScope, ModuleConfigValidation, ModuleManifest,
    ModuleMigrationActivation, ModuleMigrationDeclaration, RuntimeFunctionDeclaration,
    RuntimeRetryPolicyDeclaration, RuntimeSurface, ScheduledFunctionDeclaration,
};
#[cfg(any(feature = "s3", test))]
use serde::Deserialize;
#[cfg(any(feature = "s3", test))]
use serde_json::Value;
use serde_json::json;
#[cfg(any(feature = "s3", test))]
use std::sync::Arc;
#[cfg(any(feature = "s3", test))]
use std::time::Duration;

pub const MODULE_NAME: &str = "content-vault";
pub const MODULE_ID: &str = "lenso/content-vault";
pub const SWEEP_FUNCTION_NAME: &str = "content_vault.sweep_terminal_quarantine.v1";
pub const SWEEP_FUNCTION_QUEUE: &str = "content-vault-maintenance";
pub const SWEEP_SCHEDULE_NAME: &str = "content-vault-terminal-quarantine-sweep";
pub const SWEEP_SCHEDULE_CRON: &str = "* * * * *";
pub const SWEEP_INPUT_SCHEMA: &str = SWEEP_FUNCTION_NAME;

const DEFAULT_QUARANTINE_GRACE_SECONDS: u64 = 900;
const DEFAULT_SWEEP_BATCH_LIMIT: u32 = 100;
const MAX_SWEEP_BATCH_LIMIT: u32 = 1_000;
const SWEEP_MAX_ATTEMPTS: u32 = 3;
const SWEEP_INITIAL_DELAY_MS: u64 = 5_000;
const CONFIG_READ_CAPABILITY: &str = "content_vault.maintenance.config.read";

#[cfg(any(feature = "s3", test))]
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct MaintenanceConfig {
    quarantine_grace_seconds: u64,
    sweep_batch_limit: u32,
}

#[cfg(any(feature = "s3", test))]
impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            quarantine_grace_seconds: DEFAULT_QUARANTINE_GRACE_SECONDS,
            sweep_batch_limit: DEFAULT_SWEEP_BATCH_LIMIT,
        }
    }
}

#[cfg(any(feature = "s3", test))]
impl MaintenanceConfig {
    fn validate(self) -> AppResult<Self> {
        i64::try_from(self.quarantine_grace_seconds).map_err(|_| {
            AppError::new(
                ErrorCode::Validation,
                "Content Vault quarantine grace exceeds the supported duration",
            )
        })?;
        if !(1..=MAX_SWEEP_BATCH_LIMIT).contains(&self.sweep_batch_limit) {
            return Err(AppError::new(
                ErrorCode::Validation,
                "Content Vault sweep batch limit must be between 1 and 1000",
            ));
        }
        Ok(self)
    }

    fn grace(self) -> ChronoDuration {
        ChronoDuration::seconds(
            i64::try_from(self.quarantine_grace_seconds)
                .expect("validated quarantine grace must fit in i64"),
        )
    }
}

pub fn manifest() -> ModuleManifest {
    ModuleManifest::builder(MODULE_ID)
        .summary(
            "Tenant-scoped quarantine, validation, and immutable content references for linked Modules.",
        )
        .capabilities(vec![CONFIG_READ_CAPABILITY.to_owned()])
        .config(ModuleConfigContract {
            fields: vec![
                ModuleConfigField {
                    key: "quarantine_grace_seconds".to_owned(),
                    field_type: ModuleConfigFieldType::Integer,
                    required: false,
                    scope: ModuleConfigScope::Environment,
                    sensitive: false,
                    secret_reference: false,
                    mutability: ModuleConfigMutability::Static,
                    activation: ModuleConfigActivation::ServiceRestart,
                    read_capability: Some(CONFIG_READ_CAPABILITY.to_owned()),
                    write_capability: None,
                    default: Some(json!(DEFAULT_QUARANTINE_GRACE_SECONDS)),
                    validation: Some(ModuleConfigValidation {
                        pattern: None,
                        minimum: Some(0),
                        maximum: Some(i64::MAX),
                        allowed_values: Vec::new(),
                    }),
                },
                ModuleConfigField {
                    key: "sweep_batch_limit".to_owned(),
                    field_type: ModuleConfigFieldType::Integer,
                    required: false,
                    scope: ModuleConfigScope::Environment,
                    sensitive: false,
                    secret_reference: false,
                    mutability: ModuleConfigMutability::Static,
                    activation: ModuleConfigActivation::ServiceRestart,
                    read_capability: Some(CONFIG_READ_CAPABILITY.to_owned()),
                    write_capability: None,
                    default: Some(json!(DEFAULT_SWEEP_BATCH_LIMIT)),
                    validation: Some(ModuleConfigValidation {
                        pattern: None,
                        minimum: Some(1),
                        maximum: Some(i64::from(MAX_SWEEP_BATCH_LIMIT)),
                        allowed_values: Vec::new(),
                    }),
                },
            ],
        })
        .runtime(RuntimeSurface {
            functions: vec![RuntimeFunctionDeclaration {
                name: SWEEP_FUNCTION_NAME.to_owned(),
                version: 1,
                queue: SWEEP_FUNCTION_QUEUE.to_owned(),
                input_schema: Some(SWEEP_INPUT_SCHEMA.to_owned()),
                retry_policy: Some(RuntimeRetryPolicyDeclaration {
                    max_attempts: SWEEP_MAX_ATTEMPTS,
                    initial_delay_ms: SWEEP_INITIAL_DELAY_MS,
                }),
                operation: None,
            }],
            schedules: vec![ScheduledFunctionDeclaration {
                name: SWEEP_SCHEDULE_NAME.to_owned(),
                function_name: SWEEP_FUNCTION_NAME.to_owned(),
                cron: SWEEP_SCHEDULE_CRON.to_owned(),
                input: json!({}),
            }],
            workflows: Vec::new(),
        })
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
            ModuleMigrationDeclaration {
                migration_id: "content-vault/0003_pending_quarantine_cleanup_indexes".to_owned(),
                order: 3,
                store: "host".to_owned(),
                destructive: false,
                reversible: false,
                activation: ModuleMigrationActivation::BeforeActivation,
            },
        ])
        .build()
}

#[cfg(feature = "s3")]
pub fn linked_module() -> HostLinkedModule {
    HostLinkedModule::try_linked(MODULE_NAME, manifest, load, CONTENT_VAULT_MIGRATIONS)
}

#[cfg(feature = "s3")]
fn load(context: &AppContext) -> AppResult<Module> {
    let config = context
        .config
        .module_local_config::<MaintenanceConfig>(MODULE_NAME)?
        .validate()?;
    let stores = ContentVaultStores::from_s3_env().map_err(|error| {
        AppError::new(
            ErrorCode::ExternalDependency,
            "Content Vault object storage is unavailable",
        )
        .with_source(error)
    })?;
    let vault = ContentVault::from_stores(context.db.clone(), &stores);
    Ok(runtime_module(vault, config))
}

#[cfg(any(feature = "s3", test))]
fn runtime_module(vault: ContentVault, config: MaintenanceConfig) -> Module {
    let handler = Arc::new(SweepTerminalQuarantine {
        sweeper: Arc::new(vault),
        config,
    });
    Module::linked(manifest(), runtime_binding(handler))
}

#[cfg(any(feature = "s3", test))]
fn runtime_binding(handler: Arc<dyn FunctionHandler>) -> LinkedBinding {
    LinkedBinding::builder()
        .runtime(RuntimeDescriptor {
            module: MODULE_NAME,
            functions: vec![FunctionDefinition {
                name: SWEEP_FUNCTION_NAME.to_owned(),
                version: 1,
                queue: SWEEP_FUNCTION_QUEUE.to_owned(),
                retry_policy: RetryPolicy::fixed(
                    SWEEP_MAX_ATTEMPTS,
                    Duration::from_millis(SWEEP_INITIAL_DELAY_MS),
                ),
                handler,
            }],
            ..RuntimeDescriptor::default()
        })
        .build()
}

#[cfg(any(feature = "s3", test))]
#[derive(Debug)]
struct SweepTerminalQuarantine {
    sweeper: Arc<dyn QuarantineSweeper>,
    config: MaintenanceConfig,
}

#[cfg(any(feature = "s3", test))]
impl SweepTerminalQuarantine {
    async fn execute(&self, _input: Value) -> AppResult<Value> {
        let report = self
            .sweeper
            .sweep(self.config.grace(), self.config.sweep_batch_limit)
            .await
            .map_err(map_sweep_error)?;
        Ok(sweep_report_json(report))
    }
}

#[cfg(any(feature = "s3", test))]
#[async_trait]
impl FunctionHandler for SweepTerminalQuarantine {
    async fn call(&self, _context: ExecutionContext, input: Value) -> AppResult<Value> {
        self.execute(input).await
    }
}

#[cfg(any(feature = "s3", test))]
#[async_trait]
trait QuarantineSweeper: std::fmt::Debug + Send + Sync {
    async fn sweep(
        &self,
        grace: ChronoDuration,
        limit: u32,
    ) -> Result<SweepReport, ContentVaultError>;
}

#[cfg(any(feature = "s3", test))]
#[async_trait]
impl QuarantineSweeper for ContentVault {
    async fn sweep(
        &self,
        grace: ChronoDuration,
        limit: u32,
    ) -> Result<SweepReport, ContentVaultError> {
        self.sweep_terminal_quarantine(grace, limit).await
    }
}

#[cfg(any(feature = "s3", test))]
fn sweep_report_json(report: SweepReport) -> Value {
    json!({
        "expired_sessions": report.expired_sessions,
        "cleaned_objects": report.cleaned_objects,
        "failed_objects": report.failed_objects,
    })
}

#[cfg(any(feature = "s3", test))]
fn map_sweep_error(error: ContentVaultError) -> AppError {
    match error.code() {
        ContentVaultErrorCode::DatabaseUnavailable | ContentVaultErrorCode::StorageUnavailable => {
            AppError::new(
                ErrorCode::ExternalDependency,
                "Content Vault quarantine sweep is unavailable",
            )
            .with_source(error)
            .retryable()
        }
        _ => AppError::new(ErrorCode::Internal, "Content Vault quarantine sweep failed")
            .with_source(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "s3")]
    use lenso::host::HostBuilder;
    use lenso::{ModuleManifestLintSeverity, lint_module_manifest};
    use platform_core::{
        AppConfig, AuthConfig, DatabaseConfig, DbPool, HttpConfig, LoggingEventPublisher,
        ModuleConfig, ModuleSourcesConfig, RedisConfig, ServiceConfig, TelemetryConfig,
    };
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    fn test_context(module_values: BTreeMap<String, Value>) -> AppContext {
        let db = DbPool::connect_lazy("postgres://localhost/content_vault_test")
            .expect("lazy PostgreSQL pool");
        let modules = BTreeMap::from([(
            MODULE_NAME.to_owned(),
            ModuleConfig {
                enabled: Some(true),
                values: module_values,
            },
        )]);
        AppContext::new(
            AppConfig {
                service: ServiceConfig::default(),
                database: DatabaseConfig {
                    url: "postgres://localhost/content_vault_test".to_owned(),
                    max_connections: 1,
                },
                redis: RedisConfig::default(),
                http: HttpConfig::default(),
                telemetry: TelemetryConfig::default(),
                auth: AuthConfig::default(),
                module_sources: ModuleSourcesConfig {
                    linked_profile: "core".to_owned(),
                },
                modules,
            },
            db,
            Arc::new(LoggingEventPublisher),
        )
    }

    #[test]
    fn manifest_is_lint_clean() {
        let manifest = manifest();
        let non_ok = lint_module_manifest(&manifest)
            .into_iter()
            .filter(|lint| lint.severity != ModuleManifestLintSeverity::Ok)
            .collect::<Vec<_>>();
        assert!(non_ok.is_empty(), "manifest lints: {non_ok:#?}");
        assert_eq!(manifest.migrations.len(), CONTENT_VAULT_MIGRATIONS.len());
        let runtime = manifest.runtime.as_ref().expect("runtime declaration");
        assert_eq!(runtime.functions.len(), 1);
        assert_eq!(runtime.functions[0].name, SWEEP_FUNCTION_NAME);
        assert_eq!(
            runtime.functions[0].input_schema.as_deref(),
            Some(SWEEP_INPUT_SCHEMA)
        );
        assert_eq!(runtime.schedules.len(), 1);
        assert_eq!(runtime.schedules[0].name, SWEEP_SCHEDULE_NAME);
        assert_eq!(runtime.schedules[0].cron, SWEEP_SCHEDULE_CRON);
        assert_eq!(runtime.schedules[0].input, json!({}));
        for (declaration, migration) in manifest.migrations.iter().zip(CONTENT_VAULT_MIGRATIONS) {
            assert_eq!(declaration.store, "host");
            assert_eq!(declaration.migration_id, migration.name);
        }
    }

    #[test]
    #[cfg(feature = "s3")]
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

    #[tokio::test]
    async fn runtime_binding_matches_the_manifest_declaration() {
        #[derive(Debug)]
        struct NoopHandler;

        #[async_trait]
        impl FunctionHandler for NoopHandler {
            async fn call(&self, _context: ExecutionContext, _input: Value) -> AppResult<Value> {
                Ok(Value::Null)
            }
        }

        let binding = runtime_binding(Arc::new(NoopHandler));
        assert_eq!(binding.runtime.module, MODULE_NAME);
        assert_eq!(binding.runtime.functions.len(), 1);
        let function = &binding.runtime.functions[0];
        assert_eq!(function.name, SWEEP_FUNCTION_NAME);
        assert_eq!(function.version, 1);
        assert_eq!(function.queue, SWEEP_FUNCTION_QUEUE);
        assert_eq!(function.retry_policy.max_attempts, SWEEP_MAX_ATTEMPTS);
        assert_eq!(
            function.retry_policy.initial_delay,
            Duration::from_millis(SWEEP_INITIAL_DELAY_MS)
        );

        let stores =
            ContentVaultStores::from_object_store(Arc::new(object_store::memory::InMemory::new()))
                .expect("disjoint in-memory stores");
        let context = test_context(BTreeMap::new());
        let module = runtime_module(
            ContentVault::from_stores(context.db.clone(), &stores),
            MaintenanceConfig::default(),
        );
        let registry = lenso_bootstrap::try_function_registry(&[module])
            .expect("manifest-declared runtime binding must be admitted by the Host");
        assert!(registry.get(SWEEP_FUNCTION_NAME).is_some());
    }

    #[test]
    fn maintenance_config_rejects_an_unbounded_batch() {
        let error = MaintenanceConfig {
            sweep_batch_limit: MAX_SWEEP_BATCH_LIMIT + 1,
            ..MaintenanceConfig::default()
        }
        .validate()
        .expect_err("oversized batch must fail module loading");
        assert_eq!(error.code, ErrorCode::Validation);
    }

    #[tokio::test]
    #[cfg(feature = "s3")]
    async fn fallible_loader_propagates_invalid_module_configuration() {
        let context = test_context(BTreeMap::from([(
            "sweep_batch_limit".to_owned(),
            json!(MAX_SWEEP_BATCH_LIMIT + 1),
        )]));
        let error = linked_module()
            .try_load_module(&context)
            .expect_err("invalid maintenance config must fail Host startup");
        assert_eq!(error.code, ErrorCode::Validation);
        assert!(!error.retryable);
    }

    #[test]
    fn sweep_report_has_a_stable_runtime_shape() {
        assert_eq!(
            sweep_report_json(SweepReport {
                expired_sessions: 2,
                cleaned_objects: 3,
                failed_objects: 1,
            }),
            json!({
                "expired_sessions": 2,
                "cleaned_objects": 3,
                "failed_objects": 1,
            })
        );
    }

    #[tokio::test]
    async fn runtime_payload_cannot_override_deployment_owned_sweep_bounds() {
        #[derive(Debug, Default)]
        struct RecordingSweeper {
            calls: Mutex<Vec<(ChronoDuration, u32)>>,
        }

        #[async_trait]
        impl QuarantineSweeper for RecordingSweeper {
            async fn sweep(
                &self,
                grace: ChronoDuration,
                limit: u32,
            ) -> Result<SweepReport, ContentVaultError> {
                self.calls
                    .lock()
                    .expect("recording lock")
                    .push((grace, limit));
                Ok(SweepReport::default())
            }
        }

        let sweeper = Arc::new(RecordingSweeper::default());
        let handler = SweepTerminalQuarantine {
            sweeper: sweeper.clone(),
            config: MaintenanceConfig::default(),
        };
        handler
            .execute(json!({
                "quarantine_grace_seconds": 0,
                "sweep_batch_limit": MAX_SWEEP_BATCH_LIMIT,
            }))
            .await
            .expect("runtime input must not control maintenance authority");

        assert_eq!(
            *sweeper.calls.lock().expect("recording lock"),
            vec![(
                ChronoDuration::seconds(i64::try_from(DEFAULT_QUARANTINE_GRACE_SECONDS).unwrap()),
                DEFAULT_SWEEP_BATCH_LIMIT,
            )]
        );
    }

    #[test]
    fn runtime_input_contract_matches_the_declared_schema_identity() {
        let contract: Value = serde_json::from_str(include_str!(
            "../contracts/runtime/functions/content_vault.sweep_terminal_quarantine.v1.schema.json"
        ))
        .expect("runtime input contract must be valid JSON");
        assert_eq!(contract["$id"], SWEEP_INPUT_SCHEMA);
        assert_eq!(contract["title"], SWEEP_INPUT_SCHEMA);
        assert_eq!(contract["additionalProperties"], false);
        assert_eq!(contract["properties"], json!({}));
    }
}
