//! Generated portable contract for the Content Vault Capability.

#[allow(dead_code)]
mod contract;

mod generated {
    include!("generated.rs");
}

pub use generated::*;

#[cfg(test)]
mod tests {
    use serde_json::Value;

    #[test]
    fn every_portable_string_and_integer_edge_is_explicitly_bounded() {
        let schema_directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas");
        for entry in std::fs::read_dir(schema_directory).expect("read generated schemas") {
            let path = entry.expect("read schema entry").path();
            if path.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
                continue;
            }
            let value: Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap_or_else(|error| {
                    panic!(
                        "failed to read generated schema {}: {error}",
                        path.display()
                    )
                }))
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to parse generated schema {}: {error}",
                        path.display()
                    )
                });
            assert_closed_and_bounded(&value, &path.display().to_string());
        }
    }

    #[test]
    fn identity_digest_and_stream_frame_limits_are_pinned() {
        let reserve: Value =
            serde_json::from_str(include_str!("../schemas/reserve-request.schema.json")).unwrap();
        assert_eq!(
            reserve.pointer("/$defs/Owner/properties/plugin_instance/maxLength"),
            Some(&Value::from(200))
        );
        assert_eq!(
            reserve.pointer("/properties/expected_sha256/pattern"),
            Some(&Value::from("^[0-9a-f]{64}$"))
        );
        assert_eq!(
            reserve.pointer("/properties/expected_size_bytes/maximum"),
            Some(&Value::from(1_073_741_824_u64))
        );
        assert_eq!(
            reserve.pointer("/properties/media_type/pattern"),
            Some(&Value::from("^text/plain$"))
        );

        let upload: Value =
            serde_json::from_str(include_str!("../schemas/upload-message.schema.json")).unwrap();
        assert_eq!(
            upload.pointer("/properties/bytes_base64/maxLength"),
            Some(&Value::from(11_184_812_u64))
        );
        assert_eq!(
            upload.pointer("/$defs/NullableOffset/maximum"),
            Some(&Value::from(1_073_741_824_u64))
        );
        assert_eq!(
            upload.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }

    #[test]
    fn opaque_identifier_patterns_pin_ecmascript_unicode_semantics() {
        let expected = r"^[^\x00-\x1F\x7F]*[^\s\x00-\x1F\x7F][^\x00-\x1F\x7F]*$";
        let mut patterns = Vec::new();
        for schema in [
            include_str!("../schemas/reserve-request.schema.json"),
            include_str!("../schemas/claim-request.schema.json"),
            include_str!("../schemas/release-claim-request.schema.json"),
        ] {
            collect_control_patterns(&serde_json::from_str(schema).unwrap(), &mut patterns);
        }
        assert!(
            patterns.len() >= 9,
            "all opaque identifier edges must be covered"
        );
        assert!(patterns.iter().all(|pattern| pattern == expected));
        assert!(!expected.contains("\\x80"));
    }

    #[test]
    fn upload_frame_discriminator_rejects_cross_kind_payloads() {
        let descriptor = descriptor();
        assert_frame_cases(
            "upload-message.schema.json",
            [
                (
                    serde_json::json!({"kind":"chunk","offset":0,"bytes_base64":"YQ=="}),
                    true,
                ),
                (
                    serde_json::json!({"kind":"committed","offset":1,"content":descriptor}),
                    true,
                ),
                (serde_json::json!({"kind":"chunk","offset":0}), false),
                (
                    serde_json::json!({"kind":"chunk","offset":0,"bytes_base64":null}),
                    false,
                ),
                (
                    serde_json::json!({"kind":"chunk","offset":null,"bytes_base64":"YQ=="}),
                    false,
                ),
                (
                    serde_json::json!({"kind":"chunk","offset":0,"bytes_base64":"YQ==","content":descriptor}),
                    false,
                ),
                (serde_json::json!({"kind":"committed","offset":1}), false),
                (
                    serde_json::json!({"kind":"committed","offset":1,"content":null}),
                    false,
                ),
                (
                    serde_json::json!({"kind":"committed","offset":null,"content":descriptor}),
                    false,
                ),
                (
                    serde_json::json!({"kind":"committed","offset":1,"bytes_base64":"YQ==","content":descriptor}),
                    false,
                ),
            ],
        );
    }

    #[test]
    fn download_frame_discriminator_rejects_cross_kind_payloads() {
        let descriptor = descriptor();
        assert_frame_cases(
            "download-message.schema.json",
            [
                (
                    serde_json::json!({"kind":"descriptor","offset":0,"content":descriptor}),
                    true,
                ),
                (
                    serde_json::json!({"kind":"chunk","offset":0,"bytes_base64":"YQ=="}),
                    true,
                ),
                (
                    serde_json::json!({"kind":"descriptor","offset":1,"content":descriptor}),
                    false,
                ),
                (serde_json::json!({"kind":"descriptor","offset":0}), false),
                (
                    serde_json::json!({"kind":"descriptor","offset":0,"content":null}),
                    false,
                ),
                (
                    serde_json::json!({"kind":"descriptor","offset":0,"bytes_base64":"YQ==","content":descriptor}),
                    false,
                ),
                (serde_json::json!({"kind":"chunk","offset":0}), false),
                (
                    serde_json::json!({"kind":"chunk","offset":null,"bytes_base64":"YQ=="}),
                    false,
                ),
                (
                    serde_json::json!({"kind":"chunk","offset":0,"bytes_base64":null}),
                    false,
                ),
                (
                    serde_json::json!({"kind":"chunk","offset":0,"bytes_base64":"YQ==","content":descriptor}),
                    false,
                ),
            ],
        );
    }

    fn descriptor() -> Value {
        serde_json::json!({
            "content_id": "018f0000-0000-7000-8000-000000000000",
            "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "size_bytes": 1,
            "media_type": "text/plain",
            "created_at": "2026-08-30T00:00:00Z"
        })
    }

    fn assert_frame_cases<const N: usize>(schema_name: &str, cases: [(Value, bool); N]) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("schemas")
            .join(schema_name);
        let schema: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        for (value, expected) in cases {
            assert!(
                lenso_contract_codegen::validate_wire_value(&path, &value).is_ok(),
                "test frame must satisfy the non-conditional portable profile: {value}"
            );
            assert_eq!(
                schema_matches(&schema, &schema, &value),
                expected,
                "unexpected discriminator outcome for {value}"
            );
        }
    }

    // The pinned codegen verifier intentionally implements only the portable value profile. This
    // small recursive verifier executes the standard structural keywords used by the frame
    // discriminator so its if/then/else rules are executable in this package's freshness gate.
    fn schema_matches(schema: &Value, root: &Value, value: &Value) -> bool {
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            let Some(resolved) = reference
                .strip_prefix('#')
                .and_then(|pointer| root.pointer(pointer))
            else {
                return false;
            };
            return schema_matches(resolved, root, value);
        }
        if schema
            .get("const")
            .is_some_and(|constant| constant != value)
        {
            return false;
        }
        if schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.contains(value))
        {
            return false;
        }
        if schema
            .get("type")
            .is_some_and(|kind| !type_matches(kind, value))
        {
            return false;
        }
        if schema
            .get("anyOf")
            .and_then(Value::as_array)
            .is_some_and(|alternatives| {
                !alternatives
                    .iter()
                    .any(|candidate| schema_matches(candidate, root, value))
            })
        {
            return false;
        }
        if schema
            .get("oneOf")
            .and_then(Value::as_array)
            .is_some_and(|alternatives| {
                alternatives
                    .iter()
                    .filter(|candidate| schema_matches(candidate, root, value))
                    .count()
                    != 1
            })
        {
            return false;
        }
        if schema
            .get("not")
            .is_some_and(|forbidden| schema_matches(forbidden, root, value))
        {
            return false;
        }
        if let Some(object) = value.as_object() {
            if schema
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|required| {
                    required
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|name| !object.contains_key(name))
                })
            {
                return false;
            }
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                for (name, property_schema) in properties {
                    if let Some(property) = object.get(name)
                        && !schema_matches(property_schema, root, property)
                    {
                        return false;
                    }
                }
            }
        }
        if let Some(condition) = schema.get("if") {
            let branch = if schema_matches(condition, root, value) {
                schema.get("then")
            } else {
                schema.get("else")
            };
            if branch.is_some_and(|branch| !schema_matches(branch, root, value)) {
                return false;
            }
        }
        true
    }

    fn type_matches(kind: &Value, value: &Value) -> bool {
        let matches = |kind: &str| match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => false,
        };
        kind.as_str().map_or_else(
            || {
                kind.as_array().is_some_and(|alternatives| {
                    alternatives.iter().filter_map(Value::as_str).any(matches)
                })
            },
            matches,
        )
    }

    fn assert_closed_and_bounded(value: &Value, location: &str) {
        match value {
            Value::Object(object) => {
                let types = object.get("type");
                let is_string = types == Some(&Value::from("string"))
                    || types
                        .and_then(Value::as_array)
                        .is_some_and(|values| values.iter().any(|value| value == "string"));
                if is_string && !object.contains_key("enum") {
                    assert!(
                        object.contains_key("minLength") && object.contains_key("maxLength"),
                        "unbounded string schema at {location}: {value}"
                    );
                }
                let is_integer = types == Some(&Value::from("integer"))
                    || types
                        .and_then(Value::as_array)
                        .is_some_and(|values| values.iter().any(|value| value == "integer"));
                if is_integer {
                    assert!(
                        object.contains_key("minimum") && object.contains_key("maximum"),
                        "unbounded integer schema at {location}: {value}"
                    );
                }
                if types == Some(&Value::from("object")) {
                    assert_eq!(
                        object.get("additionalProperties"),
                        Some(&Value::Bool(false)),
                        "open object schema at {location}: {value}"
                    );
                }
                for nested in object.values() {
                    assert_closed_and_bounded(nested, location);
                }
            }
            Value::Array(values) => {
                for nested in values {
                    assert_closed_and_bounded(nested, location);
                }
            }
            _ => {}
        }
    }

    fn collect_control_patterns(value: &Value, patterns: &mut Vec<String>) {
        match value {
            Value::Object(object) => {
                if let Some(pattern) = object.get("pattern").and_then(Value::as_str)
                    && pattern.contains("\\x00")
                {
                    patterns.push(pattern.to_owned());
                }
                for nested in object.values() {
                    collect_control_patterns(nested, patterns);
                }
            }
            Value::Array(values) => {
                for nested in values {
                    collect_control_patterns(nested, patterns);
                }
            }
            _ => {}
        }
    }
}
