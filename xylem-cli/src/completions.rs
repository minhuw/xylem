//! Shell completion support for --set overrides
//!
//! This module provides intelligent autocompletion for the --set flag by
//! extracting paths from the JSON Schema of ProfileConfig. The bash and zsh
//! scripts take their flag, subcommand, and shell lists from the clap command
//! of the binary.

use clap::{Arg, ArgAction, Command};
use schemars::schema_for;
use schemars::Schema;
use serde_json::Value;
use std::collections::BTreeSet;

use crate::config::ProfileConfig;

/// Generate enhanced bash completion script with --set support
///
/// The flags, subcommands, and shell names are taken from `cmd`, the clap
/// command of the binary.
pub fn generate_bash_completion(cmd: &mut Command, bin_name: &str) -> String {
    // Building adds clap's --help and --version flags and help subcommand
    cmd.build();

    let mut flags = Vec::new();
    for arg in visible_flags(cmd) {
        flags.extend(flag_names(arg));
    }

    let mut subcommands = Vec::new();
    for subcommand in visible_subcommands(cmd) {
        subcommands.push(subcommand.get_name());
    }

    let flag_words = flags.join(" ");
    let subcommand_words = subcommands.join(" ");
    let subcommand_pattern = subcommands.join("|");
    let shell_words = shell_names(cmd).join(" ");

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
        COMPREPLY=( $(compgen -W "{flag_words}" -- "$cur") )
        return 0
    fi

    # Check if we already have a subcommand
    local has_subcommand=0
    for ((i=1; i<cword; i++)); do
        case "${{words[i]}}" in
            {subcommand_pattern})
                has_subcommand=1
                break
                ;;
        esac
    done

    # Complete subcommands only if no flags and no subcommand yet
    if [[ $has_subcommand -eq 0 && "$cur" != -* ]]; then
        # If we're at position 1 or haven't seen a profile flag yet, suggest subcommands
        if [[ $cword -eq 1 ]]; then
            COMPREPLY=( $(compgen -W "{subcommand_words}" -- "$cur") )
            return 0
        fi
    fi

    # Complete shell names for 'completions' subcommand
    if [[ "${{words[1]}}" == "completions" ]]; then
        COMPREPLY=( $(compgen -W "{shell_words}" -- "$cur") )
        return 0
    fi
}}

complete -F _{bin_name} {bin_name}
"#,
        bin_name = bin_name
    )
}

/// Generate enhanced zsh completion script with --set support
///
/// The flags, subcommands, and shell names are taken from `cmd`, the clap
/// command of the binary.
pub fn generate_zsh_completion(cmd: &mut Command, bin_name: &str) -> String {
    // Building adds clap's --help and --version flags and help subcommand
    cmd.build();

    let mut flag_lines = Vec::new();
    for arg in visible_flags(cmd) {
        flag_lines.push(format!("        {} \\", zsh_flag_spec(arg, bin_name)));
    }

    let mut subcommand_lines = Vec::new();
    for subcommand in visible_subcommands(cmd) {
        let about = match subcommand.get_about() {
            Some(about) => about.to_string(),
            None => String::new(),
        };
        subcommand_lines.push(format!(
            "                '{}:{}'",
            subcommand.get_name(),
            zsh_escape_help(&about)
        ));
    }

    let flag_lines = flag_lines.join("\n");
    let subcommand_lines = subcommand_lines.join("\n");
    let shell_words = shell_names(cmd).join(" ");

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
{flag_lines}
        '1: :->command' \
        '*:: :->args'

    case $state in
        command)
            local -a subcommands
            subcommands=(
{subcommand_lines}
            )
            _describe 'command' subcommands
            ;;
        args)
            case $line[1] in
                completions)
                    _arguments '1:shell:({shell_words})'
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

/// Flags of the command that shell completions offer (not positional, not hidden)
fn visible_flags(cmd: &Command) -> Vec<&Arg> {
    let mut flags = Vec::new();
    for arg in cmd.get_arguments() {
        if !arg.is_positional() && !arg.is_hide_set() {
            flags.push(arg);
        }
    }
    flags
}

/// Subcommands that shell completions offer (complete-paths is hidden)
fn visible_subcommands(cmd: &Command) -> Vec<&Command> {
    let mut subcommands = Vec::new();
    for subcommand in cmd.get_subcommands() {
        if !subcommand.is_hide_set() {
            subcommands.push(subcommand);
        }
    }
    subcommands
}

/// Short and long spellings of a flag, e.g. `-P` and `--profile`
fn flag_names(arg: &Arg) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(short) = arg.get_short() {
        names.push(format!("-{}", short));
    }
    if let Some(long) = arg.get_long() {
        names.push(format!("--{}", long));
    }
    names
}

/// Shell names accepted by the `completions` subcommand
fn shell_names(cmd: &Command) -> Vec<String> {
    let mut names = Vec::new();
    let Some(completions) = cmd.find_subcommand("completions") else {
        return names;
    };
    for arg in completions.get_positionals() {
        for value in arg.get_possible_values() {
            if !value.is_hide_set() {
                names.push(value.get_name().to_string());
            }
        }
    }
    names
}

/// Zsh `_arguments` spec for one flag, e.g.
/// `'(-P --profile)'{-P,--profile}'[Path to TOML profile configuration file]:file:_files -g "*.toml"'`
fn zsh_flag_spec(arg: &Arg, bin_name: &str) -> String {
    let names = flag_names(arg);

    // Repeatable flags may be given many times; other flags exclude all their spellings
    let prefix = if matches!(arg.get_action(), ArgAction::Append | ArgAction::Count) {
        "*".to_string()
    } else {
        format!("({})", names.join(" "))
    };

    // Brace expansion turns {-P,--profile} into one spec per spelling
    let spellings = if names.len() == 1 {
        names[0].clone()
    } else {
        format!("{{{}}}", names.join(","))
    };

    let help = match arg.get_help() {
        Some(help) => help.to_string(),
        None => String::new(),
    };

    format!(
        "'{}'{}'[{}]{}'",
        prefix,
        spellings,
        zsh_escape_help(&help),
        zsh_value_spec(arg, bin_name)
    )
}

/// Zsh `:message:action` for the value of a flag, empty if the flag takes no value
///
/// clap only knows these values as strings, so the profile file filter, the
/// --set key completion, and the log levels are chosen here by argument id.
fn zsh_value_spec(arg: &Arg, bin_name: &str) -> String {
    if !arg.get_action().takes_values() {
        return String::new();
    }
    match arg.get_id().as_str() {
        "profile" => ":file:_files -g \"*.toml\"".to_string(),
        "set" => format!(":key=value:_{}_complete_set_keys", bin_name),
        "log_level" => ":level:(trace debug info warn error)".to_string(),
        // A single-space action completes nothing
        _ => ":value: ".to_string(),
    }
}

/// Escape clap help text for a single-quoted zsh string inside `[...]` or `_describe`
fn zsh_escape_help(help: &str) -> String {
    help.replace('\\', "\\\\")
        .replace('\'', "'\\''")
        .replace('[', "\\[")
        .replace(']', "\\]")
        .replace(':', "\\:")
        .replace('$', "\\$")
        .replace('`', "\\`")
        .replace('\n', " ")
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

    // HTTP configuration
    paths.insert(format!("{}.method", base));
    paths.insert(format!("{}.path", base));
    paths.insert(format!("{}.host", base));
    paths.insert(format!("{}.body_size", base));

    // Memcached configuration
    paths.insert(format!("{}.operation", base));
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

    // Handle anyOf (schemars emits it for Option<T> fields such as request_dump)
    if let Some(Value::Array(any_of)) = schema_obj.get("anyOf") {
        for variant_value in any_of {
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
        assert!(paths.contains(&"output".to_string()));

        // Should contain nested paths
        assert!(paths.contains(&"experiment.name".to_string()));
        assert!(paths.contains(&"experiment.duration".to_string()));
        assert!(paths.contains(&"experiment.seed".to_string()));

        // Should contain array indexing for traffic_groups
        assert!(paths.contains(&"traffic_groups".to_string()));
        assert!(paths.contains(&"traffic_groups.0".to_string()));
        assert!(paths.contains(&"traffic_groups.0.request_timeout".to_string()));

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

    #[test]
    fn config_paths_include_anyof_children() {
        let paths = get_config_paths();

        // request_dump is Option<DumpConfig>, which the schema wraps in anyOf
        assert!(paths.contains(&"request_dump".to_string()));
        assert!(
            paths.contains(&"request_dump.directory".to_string()),
            "Missing request_dump.directory path"
        );
    }
}
