use std::{env, path::Path};

use lenso_contract_codegen::{
    ProjectionLanguage, check_projection, check_source_snapshot, write_source_snapshot,
};

#[allow(dead_code)]
#[path = "src/contract.rs"]
mod contract_source;

fn main() {
    println!("cargo:rerun-if-changed=capability.json");
    println!("cargo:rerun-if-changed=schemas");
    println!("cargo:rerun-if-changed=src/contract.rs");
    println!("cargo:rerun-if-changed=src/generated.rs");
    println!("cargo:rerun-if-env-changed=LENSO_UPDATE_CONTRACT_SNAPSHOT");

    let mut snapshot = contract_source::__lenso_capability_snapshot();
    normalize_snapshot(&mut snapshot);
    if env::var_os("LENSO_UPDATE_CONTRACT_SNAPSHOT").is_some() {
        write_source_snapshot(&snapshot, Path::new("capability.json"))
            .unwrap_or_else(|error| panic!("failed to update Content Vault snapshot: {error}"));
    } else {
        check_source_snapshot(&snapshot, Path::new("capability.json")).unwrap_or_else(|error| {
            panic!("Content Vault Descriptor or Schemas are stale: {error}")
        });
    }

    check_projection(
        Path::new("capability.json"),
        ProjectionLanguage::Rust,
        Path::new("src/generated.rs"),
    )
    .unwrap_or_else(|error| panic!("Content Vault generated Rust projection is stale: {error}"));
}

fn normalize_snapshot(snapshot: &mut lenso_contract_authoring::CapabilitySnapshot) {
    for operation in &mut snapshot.operations {
        for schema in [
            &mut operation.request_schema,
            &mut operation.response_schema,
            &mut operation.domain_error_schema,
        ] {
            normalize_schema(schema);
        }
    }
}

fn normalize_schema(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            let nullable_integer = object
                .get("type")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|types| types.iter().any(|value| value.as_str() == Some("integer")));
            if nullable_integer {
                object.remove("format");
            }
            for value in object.values_mut() {
                normalize_schema(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_schema(value);
            }
        }
        _ => {}
    }
}
