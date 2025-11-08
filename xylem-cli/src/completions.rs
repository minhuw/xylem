//! Shell completion support for --set overrides
//!
//! This module provides intelligent autocompletion for the --set flag by
//! extracting paths from the JSON Schema of ProfileConfig.

use schemars::schema::{RootSchema, Schema, SchemaObject};
use schemars::schema_for;
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

    # Complete subcommands
    if [[ $cword -eq 1 ]]; then
        COMPREPLY=( $(compgen -W "run completions schema help" -- "$cur") )
        return 0
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
        '(-l --log-level)'{{'{{'}}-l,--log-level{{'}}'}}'[Log level]:level:(trace debug info warn error)' \
        '1: :->command' \
        '*:: :->args'

    case $state in
        command)
            local -a subcommands
            subcommands=(
                'run:Run a latency measurement experiment'
                'completions:Generate shell completions'
                'schema:Generate JSON Schema'
                'help:Print help'
            )
            _describe 'command' subcommands
            ;;
        args)
            case $line[1] in
                run)
                    _arguments \
                        '(-P --profile)'{{'{{'}}-P,--profile{{'}}'}}'[Profile path]:file:_files -g "*.toml"' \
                        '*--set[Override config]:key=value:_{bin_name}_complete_set_keys'
                    ;;
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

    extract_paths_from_schema(&schema, "", &mut paths);

    paths.into_iter().collect()
}

/// Recursively extract all valid paths from a JSON schema
fn extract_paths_from_schema(root_schema: &RootSchema, prefix: &str, paths: &mut BTreeSet<String>) {
    // Start from the root schema object
    extract_paths_from_object(&root_schema.schema, prefix, paths, &root_schema.definitions);
}

/// Extract paths from a schema object
fn extract_paths_from_object(
    schema: &SchemaObject,
    prefix: &str,
    paths: &mut BTreeSet<String>,
    definitions: &std::collections::BTreeMap<String, Schema>,
) {
    // Add the current path if it's not empty
    if !prefix.is_empty() {
        paths.insert(prefix.to_string());
    }

    // Handle object properties
    if let Some(obj) = &schema.object {
        for (prop_name, prop_schema) in &obj.properties {
            let new_prefix = if prefix.is_empty() {
                prop_name.clone()
            } else {
                format!("{}.{}", prefix, prop_name)
            };

            // Add this path
            paths.insert(new_prefix.clone());

            // Recurse into the property
            extract_paths_from_schema_value(prop_schema, &new_prefix, paths, definitions);
        }
    }

    // Handle oneOf (for enums with different variants)
    if let Some(subschemas) = &schema.subschemas {
        if let Some(schemas) = &subschemas.one_of {
            for variant_schema in schemas {
                extract_paths_from_schema_value(variant_schema, prefix, paths, definitions);
            }
        }

        // Handle allOf (for composed schemas)
        if let Some(schemas) = &subschemas.all_of {
            for schema in schemas {
                extract_paths_from_schema_value(schema, prefix, paths, definitions);
            }
        }
    }
}

/// Extract paths from a schema value (which might be a reference or inline)
fn extract_paths_from_schema_value(
    schema: &Schema,
    prefix: &str,
    paths: &mut BTreeSet<String>,
    definitions: &std::collections::BTreeMap<String, Schema>,
) {
    match schema {
        Schema::Object(obj) => {
            // Check if it's a reference
            if let Some(reference) = &obj.reference {
                // Extract the definition name from the reference
                if let Some(def_name) = reference.strip_prefix("#/definitions/") {
                    if let Some(def_schema) = definitions.get(def_name) {
                        extract_paths_from_schema_value(def_schema, prefix, paths, definitions);
                    }
                }
            } else {
                // It's an inline object
                extract_paths_from_object(obj, prefix, paths, definitions);
            }
        }
        Schema::Bool(_) => {
            // Boolean schemas don't add paths
        }
    }

    // Handle arrays - add a .0 indexing example
    handle_array_schema(schema, prefix, paths, definitions);
}

/// Handle array schemas to add indexing examples
fn handle_array_schema(
    schema: &Schema,
    prefix: &str,
    paths: &mut BTreeSet<String>,
    definitions: &std::collections::BTreeMap<String, Schema>,
) {
    let Schema::Object(obj) = schema else {
        return;
    };

    let Some(array) = &obj.array else {
        return;
    };

    let Some(items) = &array.items else {
        return;
    };

    match items {
        schemars::schema::SingleOrVec::Single(item_schema) => {
            let array_prefix = format!("{}.0", prefix);
            paths.insert(array_prefix.clone());
            extract_paths_from_schema_value(item_schema, &array_prefix, paths, definitions);
        }
        schemars::schema::SingleOrVec::Vec(item_schemas) => {
            for (idx, item_schema) in item_schemas.iter().enumerate() {
                let array_prefix = format!("{}.{}", prefix, idx);
                paths.insert(array_prefix.clone());
                extract_paths_from_schema_value(item_schema, &array_prefix, paths, definitions);
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
