//! Shell completion support for --set overrides
//!
//! This module provides intelligent autocompletion for the --set flag by
//! extracting paths from the JSON Schema of ProfileConfig.

use schemars::schema_for;
use schemars::Schema;
use serde_json::Value;
use std::collections::BTreeSet;

use crate::config::ProfileConfig;

/// Generate enhanced bash completion script with --set support
pub fn generate_bash_completion(bin_name: &str) -> String {
    format!(
        r#"# Bash completion for {bin_name}
# Installation:
#   Copy this file to ~/.local/share/bash-completion/completions/{bin_name}
#   Or source it in your ~/.bashrc:
#     source <(xylem completions bash)

_{bin_name}_complete_set_key() {{
    local keys
    keys=$({bin_name} complete-paths 2>/dev/null)
    COMPREPLY=( $(compgen -W "$keys" -- "${{COMP_WORDS[COMP_CWORD]}}") )
}}

_{bin_name}() {{
    local cur prev words cword
    _init_completion || return

    # Check if we're completing a --set value
    if [[ "$prev" == "--set" ]]; then
        # If the current word contains '=', we're completing the value part
        if [[ "$cur" == *"="* ]]; then
            # Don't provide completions for the value part
            return 0
        else
            # Complete the key part (before '=')
            _{bin_name}_complete_set_key
            # Add '=' suffix to completions
            if [[ ${{#COMPREPLY[@]}} -gt 0 ]]; then
                local i
                for i in "${{!COMPREPLY[@]}}"; do
                    COMPREPLY[$i]="${{COMPREPLY[$i]}}="
                done
            fi
            compopt -o nospace
            return 0
        fi
    fi

    # Standard completion for flags and subcommands
    case "$prev" in
        -P|--profile)
            _filedir toml
            return 0
            ;;
        -l|--log-level)
            COMPREPLY=( $(compgen -W "trace debug info warn error" -- "$cur") )
            return 0
            ;;
    esac

    # Complete flags and subcommands
    if [[ "$cur" == -* ]]; then
        COMPREPLY=( $(compgen -W "-P --profile --set -l --log-level -h --help -V --version" -- "$cur") )
        return 0
    fi

    # Check if we already have a subcommand
    local has_subcommand=0
    for ((i=1; i<cword; i++)); do
        case "${{words[i]}}" in
            completions|schema|help)
                has_subcommand=1
                break
                ;;
        esac
    done

    # Complete subcommands only if no flags and no subcommand yet
    if [[ $has_subcommand -eq 0 && "$cur" != -* ]]; then
        # If we're at position 1 or haven't seen a profile flag yet, suggest subcommands
        if [[ $cword -eq 1 ]]; then
            COMPREPLY=( $(compgen -W "completions schema help" -- "$cur") )
            return 0
        fi
    fi

    # Complete shell names for 'completions' subcommand
    if [[ "${{words[1]}}" == "completions" ]]; then
        COMPREPLY=( $(compgen -W "bash zsh fish" -- "$cur") )
        return 0
    fi
}}

complete -F _{bin_name} {bin_name}
"#,
        bin_name = bin_name
    )
}

/// Generate enhanced zsh completion script with --set support
pub fn generate_zsh_completion(bin_name: &str) -> String {
    format!(
        r#"#compdef {bin_name}
# Zsh completion for {bin_name}
# Installation:
#   Copy this file to a directory in your $fpath, e.g.:
#     ~/.zsh/completions/_{bin_name}
#   Then reload completions:
#     rm -f ~/.zcompdump; compinit

_{bin_name}_complete_set_keys() {{
    local -a keys
    keys=($({bin_name} complete-paths 2>/dev/null))
    _describe 'config keys' keys -S '='
}}

_{bin_name}() {{
    local line state

    _arguments -C \
        '(-P --profile)'{{'{{'}}-P,--profile{{'}}'}}'[Profile path]:file:_files -g "*.toml"' \
        '*--set[Override config]:key=value:_{bin_name}_complete_set_keys' \
        '(-l --log-level)'{{'{{'}}-l,--log-level{{'}}'}}'[Log level]:level:(trace debug info warn error)' \
        '1: :->command' \
        '*:: :->args'

    case $state in
        command)
            local -a subcommands
            subcommands=(
                'completions:Generate shell completions'
                'schema:Generate JSON Schema'
                'help:Print help'
            )
            _describe 'command' subcommands
            ;;
        args)
            case $line[1] in
                completions)
                    _arguments '1:shell:(bash zsh fish)'
                    ;;
            esac
            ;;
    esac
}}

_{bin_name} "$@"
"#,
        bin_name = bin_name
    )
}

/// Get all valid TOML paths for --set autocompletion
///
/// This extracts paths automatically from the ProfileConfig JSON Schema,
/// plus manually-added paths for protocol_config which uses dynamic typing.
pub fn get_config_paths() -> Vec<String> {
    let schema = schema_for!(ProfileConfig);
    let mut paths = BTreeSet::new();

    // Extract $defs for reference resolution
    let defs = if let Value::Object(obj) = schema.as_value() {
        obj.get("$defs").and_then(|v| v.as_object()).cloned()
    } else {
        None
    };

    // schema_for! now returns a Schema directly (which wraps serde_json::Value)
    extract_paths_from_schema_value(&schema, "", &mut paths, &defs);

    // Add protocol_config paths manually since it's typed as serde_json::Value
    // These are the common paths for Redis/Memcached/HTTP protocols
    add_protocol_config_paths(&mut paths);

    paths.into_iter().collect()
}

/// Add protocol_config paths manually for shell completions
///
/// Since protocol_config is typed as `serde_json::Value` for flexibility,
/// the schema walker can't discover nested paths. We add them manually here.
fn add_protocol_config_paths(paths: &mut BTreeSet<String>) {
    let base = "traffic_groups.0.protocol_config";

    // Keys configuration (Redis, Memcached)
    paths.insert(format!("{}.keys", base));
    paths.insert(format!("{}.keys.strategy", base));
    paths.insert(format!("{}.keys.n", base));
    paths.insert(format!("{}.keys.theta", base));
    paths.insert(format!("{}.keys.max", base));
    paths.insert(format!("{}.keys.start", base));
    paths.insert(format!("{}.keys.mean_pct", base));
    paths.insert(format!("{}.keys.std_dev_pct", base));
    paths.insert(format!("{}.keys.value_size", base));

    // Operations configuration (Redis)
    paths.insert(format!("{}.operations", base));
    paths.insert(format!("{}.operations.strategy", base));
    paths.insert(format!("{}.operations.operation", base));
    paths.insert(format!("{}.operations.commands", base));
    paths.insert(format!("{}.operations.commands.0", base));
    paths.insert(format!("{}.operations.commands.0.name", base));
    paths.insert(format!("{}.operations.commands.0.weight", base));
    paths.insert(format!("{}.operations.commands.0.params", base));
    paths.insert(format!("{}.operations.commands.0.params.count", base));
    paths.insert(format!("{}.operations.commands.0.params.num_replicas", base));
    paths.insert(format!("{}.operations.commands.0.params.timeout_ms", base));

    // Value size configuration (Redis)
    paths.insert(format!("{}.value_size", base));
    paths.insert(format!("{}.value_size.strategy", base));
    paths.insert(format!("{}.value_size.size", base));
    paths.insert(format!("{}.value_size.min", base));
    paths.insert(format!("{}.value_size.max", base));
    paths.insert(format!("{}.value_size.mean", base));
    paths.insert(format!("{}.value_size.std_dev", base));

    // Data import configuration (Redis)
    paths.insert(format!("{}.data_import", base));
    paths.insert(format!("{}.data_import.file", base));
    paths.insert(format!("{}.data_import.verification", base));
    paths.insert(format!("{}.data_import.verification.mode", base));
    paths.insert(format!("{}.data_import.verification.sample_rate", base));

    // HTTP configuration
    paths.insert(format!("{}.method", base));
    paths.insert(format!("{}.path", base));
    paths.insert(format!("{}.host", base));
    paths.insert(format!("{}.body_size", base));

    // Memcached configuration
    paths.insert(format!("{}.operation", base));

    // Echo configuration
    paths.insert(format!("{}.message_size", base));
}

/// Recursively extract all valid paths from a JSON schema value
fn extract_paths_from_schema_value(
    schema: &Schema,
    prefix: &str,
    paths: &mut BTreeSet<String>,
    defs: &Option<serde_json::Map<String, Value>>,
) {
    // Schema is now a wrapper around serde_json::Value
    // We need to work with it as JSON
    let Value::Object(schema_obj) = schema.as_value() else {
        return;
    };

    // Add the current path if it's not empty
    if !prefix.is_empty() {
        paths.insert(prefix.to_string());
    }

    // Handle $ref references first - resolve and recurse
    if let Some(Value::String(reference)) = schema_obj.get("$ref") {
        // Early return if no defs available
        let Some(defs_map) = defs else {
            return;
        };

        // $ref format is typically "#/$defs/TypeName"
        let Some(type_name) = reference.strip_prefix("#/$defs/") else {
            return;
        };

        let Some(ref_schema_value) = defs_map.get(type_name) else {
            return;
        };

        let Ok(ref_schema) = serde_json::from_value::<Schema>(ref_schema_value.clone()) else {
            return;
        };

        extract_paths_from_schema_value(&ref_schema, prefix, paths, defs);
        return;
    }

    // Handle object properties
    if let Some(Value::Object(properties)) = schema_obj.get("properties") {
        for (prop_name, prop_schema_value) in properties {
            let new_prefix = if prefix.is_empty() {
                prop_name.clone()
            } else {
                format!("{}.{}", prefix, prop_name)
            };

            // Add this path
            paths.insert(new_prefix.clone());

            // Recurse into the property if it's a valid schema
            if let Ok(prop_schema) = serde_json::from_value::<Schema>(prop_schema_value.clone()) {
                extract_paths_from_schema_value(&prop_schema, &new_prefix, paths, defs);
            }
        }
    }

    // Handle array items - add a .0 indexing example
    if let Some(Value::Object(items)) = schema_obj.get("items") {
        let array_prefix = format!("{}.0", prefix);
        paths.insert(array_prefix.clone());

        if let Ok(item_schema) = serde_json::from_value::<Schema>(Value::Object(items.clone())) {
            extract_paths_from_schema_value(&item_schema, &array_prefix, paths, defs);
        }
    }

    // Handle oneOf (for enums with different variants)
    if let Some(Value::Array(one_of)) = schema_obj.get("oneOf") {
        for variant_value in one_of {
            if let Ok(variant_schema) = serde_json::from_value::<Schema>(variant_value.clone()) {
                extract_paths_from_schema_value(&variant_schema, prefix, paths, defs);
            }
        }
    }

    // Handle allOf (for composed schemas)
    if let Some(Value::Array(all_of)) = schema_obj.get("allOf") {
        for schema_value in all_of {
            if let Ok(composed_schema) = serde_json::from_value::<Schema>(schema_value.clone()) {
                extract_paths_from_schema_value(&composed_schema, prefix, paths, defs);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_config_paths() {
        let paths = get_config_paths();

        // Should contain top-level sections
        assert!(paths.contains(&"experiment".to_string()));
        assert!(paths.contains(&"target".to_string()));
        assert!(paths.contains(&"output".to_string()));

        // Should contain nested paths
        assert!(paths.contains(&"experiment.name".to_string()));
        assert!(paths.contains(&"experiment.duration".to_string()));
        assert!(paths.contains(&"experiment.seed".to_string()));
        assert!(paths.contains(&"target.transport".to_string()));

        // Should contain array indexing for traffic_groups
        assert!(paths.contains(&"traffic_groups".to_string()));
        assert!(paths.contains(&"traffic_groups.0".to_string()));

        // Should contain protocol_config paths (manually added for completions)
        assert!(
            paths.contains(&"traffic_groups.0.protocol_config".to_string()),
            "Missing protocol_config path"
        );
        assert!(
            paths.contains(&"traffic_groups.0.protocol_config.keys".to_string()),
            "Missing protocol_config.keys path"
        );
        assert!(
            paths.contains(&"traffic_groups.0.protocol_config.keys.strategy".to_string()),
            "Missing protocol_config.keys.strategy path"
        );
        assert!(
            paths.contains(&"traffic_groups.0.protocol_config.keys.n".to_string()),
            "Missing protocol_config.keys.n path"
        );
        assert!(
            paths.contains(&"traffic_groups.0.protocol_config.operations".to_string()),
            "Missing protocol_config.operations path"
        );
        assert!(
            paths.contains(&"traffic_groups.0.protocol_config.operations.strategy".to_string()),
            "Missing protocol_config.operations.strategy path"
        );

        // Ensure we have a reasonable number of paths (should be > 60 with protocol_config)
        assert!(paths.len() > 60, "Expected > 60 paths, got {}", paths.len());
    }
}
