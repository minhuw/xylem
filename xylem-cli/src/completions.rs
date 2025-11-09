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
/// This extracts paths automatically from the ProfileConfig JSON Schema.
pub fn get_config_paths() -> Vec<String> {
    let schema = schema_for!(ProfileConfig);
    let mut paths = BTreeSet::new();

    // schema_for! now returns a Schema directly (which wraps serde_json::Value)
    extract_paths_from_schema_value(&schema, "", &mut paths);

    paths.into_iter().collect()
}

/// Recursively extract all valid paths from a JSON schema value
fn extract_paths_from_schema_value(schema: &Schema, prefix: &str, paths: &mut BTreeSet<String>) {
    // Schema is now a wrapper around serde_json::Value
    // We need to work with it as JSON
    let Value::Object(schema_obj) = schema.as_value() else {
        return;
    };

    // Add the current path if it's not empty
    if !prefix.is_empty() {
        paths.insert(prefix.to_string());
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
                extract_paths_from_schema_value(&prop_schema, &new_prefix, paths);
            }
        }
    }

    // Handle array items - add a .0 indexing example
    if let Some(Value::Object(items)) = schema_obj.get("items") {
        let array_prefix = format!("{}.0", prefix);
        paths.insert(array_prefix.clone());

        if let Ok(item_schema) = serde_json::from_value::<Schema>(Value::Object(items.clone())) {
            extract_paths_from_schema_value(&item_schema, &array_prefix, paths);
        }
    }

    // Handle $ref references
    if let Some(Value::String(_reference)) = schema_obj.get("$ref") {
        // For now, we'll just note that references exist but won't follow them
        // since schemars 1.x handles references differently
        // The schema should already be resolved by schema_for!
    }

    // Handle oneOf (for enums with different variants)
    if let Some(Value::Array(one_of)) = schema_obj.get("oneOf") {
        for variant_value in one_of {
            if let Ok(variant_schema) = serde_json::from_value::<Schema>(variant_value.clone()) {
                extract_paths_from_schema_value(&variant_schema, prefix, paths);
            }
        }
    }

    // Handle allOf (for composed schemas)
    if let Some(Value::Array(all_of)) = schema_obj.get("allOf") {
        for schema_value in all_of {
            if let Ok(composed_schema) = serde_json::from_value::<Schema>(schema_value.clone()) {
                extract_paths_from_schema_value(&composed_schema, prefix, paths);
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
        assert!(paths.contains(&"workload".to_string()));
        assert!(paths.contains(&"output".to_string()));

        // Should contain nested paths
        assert!(paths.contains(&"experiment.name".to_string()));
        assert!(paths.contains(&"experiment.duration".to_string()));
        assert!(paths.contains(&"experiment.seed".to_string()));
        assert!(paths.contains(&"target.address".to_string()));
        assert!(paths.contains(&"target.protocol".to_string()));

        // Should contain array indexing for traffic_groups
        assert!(paths.contains(&"traffic_groups".to_string()));
        assert!(paths.contains(&"traffic_groups.0".to_string()));

        // Print all paths for manual inspection
        println!("\nExtracted {} config paths:", paths.len());
        for (i, path) in paths.iter().enumerate() {
            println!("  {:2}. {}", i + 1, path);
        }

        // Ensure we have a reasonable number of paths (should be > 30)
        assert!(paths.len() > 30, "Expected > 30 paths, got {}", paths.len());
    }
}
