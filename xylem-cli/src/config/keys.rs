//! Unknown and removed profile keys, and the paths of `--set` overrides
//!
//! serde rejects unknown keys too, but stops at the first one and, inside a
//! protocol_config, cannot say where it is. This module walks a profile
//! along its JSON schema instead and reports every unknown key with its path
//! and the keys valid there. serde stays the backstop for anything it skips.

use super::{PathSegment, ProfileConfig, ProtocolKind, PROTOCOLS};
use anyhow::{bail, Result};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt;
use xylem_protocols::RedisCommandParams;

/// A key profiles no longer accept, or a value they no longer accept at a key
struct Removed {
    /// Dotted path of the key, with `[]` for any array index
    path: &'static str,
    /// The removed value, or None if the key itself was removed
    value: Option<&'static str>,
    /// What to do instead. Under a traffic group it follows the group's
    /// index and name.
    message: &'static str,
}

/// Removed keys and values. Their message replaces the error an unknown key
/// or an invalid value would get.
const REMOVED: &[Removed] = &[
    Removed {
        path: "target",
        value: None,
        message: "The top-level [target] section was removed; set target and transport \
                  in each [[traffic_groups]] entry",
    },
    Removed {
        path: "workload",
        value: None,
        message: "The top-level [workload] section was removed; set keys, operations and \
                  value_size in each traffic group's protocol_config",
    },
    Removed {
        path: "stats.streaming",
        value: None,
        message: "stats.streaming was removed with Parquet stats streaming; delete the \
                  [stats.streaming] section",
    },
    Removed {
        path: "stats.sampling_policy.type",
        value: Some("adaptive"),
        message: "The adaptive sampling policy was removed because it never adapted its \
                  rate; give stats.sampling_policy type \"limited\" with a fixed rate, or \
                  leave it unset",
    },
    Removed {
        path: "traffic_groups[].sampling_rate",
        value: None,
        message: "sampling_rate was replaced by sampling_policy; to record a fraction of \
                  latencies, set sampling_policy = { type = \"limited\", rate = ... }",
    },
    Removed {
        path: "traffic_groups[].sampling_policy.type",
        value: Some("adaptive"),
        message: "the adaptive sampling policy was removed because it never adapted its \
                  rate; use type \"limited\" with a fixed rate, or the default \
                  \"hdr-histogram\"",
    },
    Removed {
        path: "traffic_groups[].protocol",
        value: Some("redis-cluster"),
        message: "Redis Cluster support was removed because it never routed requests by \
                  hash slot; benchmark each node with its own redis traffic group",
    },
    Removed {
        path: "traffic_groups[].protocol_config.data_import",
        value: None,
        message: "data_import is not supported for redis protocol. \
                  Remove protocol_config.data_import.",
    },
];

/// The message of a removed key, or of a removed value at a key
fn removed(path: &Path, value: Option<&str>) -> Option<&'static str> {
    let pattern = path.pattern();
    REMOVED
        .iter()
        .find(|removed| removed.path == pattern && removed.value == value)
        .map(|removed| removed.message)
}

/// Why traffic group protocol `protocol` is gone, if it was removed
pub(super) fn removed_protocol(protocol: &str) -> Option<&'static str> {
    removed(&Path::default().key("traffic_groups").index(0).key("protocol"), Some(protocol))
}

/// Every unknown and removed key in `profile`, one message each
pub(super) fn check_profile(profile: &Value) -> Vec<String> {
    let schema = Schema::of::<ProfileConfig>();
    let mut problems = Vec::new();
    schema.check(&schema.0, profile, &Path::default(), &mut problems);

    let groups = profile.get("traffic_groups").and_then(Value::as_array);
    let groups = groups.map(Vec::as_slice).unwrap_or_default();
    let name = |index: usize| groups.get(index)?.get("name")?.as_str();
    let mut messages: Vec<String> = problems.iter().map(|problem| problem.render(name)).collect();
    for (index, group) in groups.iter().enumerate() {
        let protocol = group.get("protocol").and_then(Value::as_str).and_then(protocol_kind);
        // A group without a known protocol fails validation instead
        if let (Some(protocol), Some(pc)) = (protocol, group.get("protocol_config")) {
            let name = name(index).unwrap_or_default();
            messages.extend(check_protocol_config(protocol, pc, index, name));
        }
    }
    messages
}

/// Every unknown and removed key in the protocol_config `pc` of traffic
/// group `index` named `name`, one message each
pub(super) fn check_protocol_config(
    protocol: ProtocolKind,
    pc: &Value,
    index: usize,
    name: &str,
) -> Vec<String> {
    let path = Path::default().key("traffic_groups").index(index).key("protocol_config");
    let schema = protocol_schema(protocol);
    let mut problems = Vec::new();
    schema.check(&schema.0, pc, &path, &mut problems);
    if protocol == ProtocolKind::Redis {
        check_redis_params(pc, &path, &mut problems);
    }
    problems.iter().map(|problem| problem.render(|_| Some(name))).collect()
}

/// Redis command params are untagged shapes, which the schema check skips:
/// only the command tells which keys belong to them
fn check_redis_params(pc: &Value, path: &Path, problems: &mut Vec<Problem>) {
    let Some(commands) = pc.pointer("/operations/commands").and_then(Value::as_array) else {
        return;
    };
    let path = path.key("operations").key("commands");
    for (index, command) in commands.iter().enumerate() {
        let name = command.get("name").and_then(Value::as_str);
        let Some(Value::Object(params)) = command.get("params") else {
            continue;
        };
        // An unknown command fails with its own error
        let Some(valid) = name.and_then(RedisCommandParams::keys) else {
            continue;
        };
        let params_path = path.index(index).key("params");
        for key in params.keys().filter(|key| !valid.contains(&key.as_str())) {
            problems.push(Problem::unknown(&params_path, key, valid.iter().copied()));
        }
    }
}

/// Check the path of a `--set` override against the profile's schema: each
/// key must exist where it is, and each index must be within its array.
/// `profile` is the profile the override applies to.
pub(super) fn check_set_path(profile: &Value, segments: &[PathSegment]) -> Result<()> {
    let schema = Schema::of::<ProfileConfig>();
    let groups = profile.get("traffic_groups").and_then(Value::as_array);
    let name = |index: usize| groups?.get(index)?.get("name")?.as_str();
    // A protocol_config has the schema of its group's protocol
    if let [PathSegment::Key(groups_key), PathSegment::Index(index), PathSegment::Key(key), rest @ ..] =
        segments
    {
        if groups_key == "traffic_groups" && key == "protocol_config" {
            schema.check_path(&schema.0, Some(profile), Path::default(), &segments[..2], name)?;
            let group = &profile["traffic_groups"][*index];
            let protocol = group.get("protocol").and_then(Value::as_str).and_then(protocol_kind);
            // Validation reports an unknown protocol
            let Some(protocol) = protocol else {
                return Ok(());
            };
            let schema = protocol_schema(protocol);
            let path = Path::default().key(groups_key).index(*index).key(key);
            return schema.check_path(&schema.0, group.get("protocol_config"), path, rest, name);
        }
    }
    schema.check_path(&schema.0, Some(profile), Path::default(), segments, name)
}

fn protocol_kind(name: &str) -> Option<ProtocolKind> {
    PROTOCOLS.iter().find(|(protocol, _)| *protocol == name).map(|&(_, kind)| kind)
}

/// The schema of the protocol_config of `protocol`, the type
/// `parse_protocol_config` parses it as
fn protocol_schema(protocol: ProtocolKind) -> Schema {
    match protocol {
        ProtocolKind::Redis => Schema::of::<xylem_protocols::RedisConfig>(),
        ProtocolKind::MemcachedBinary | ProtocolKind::MemcachedAscii => {
            Schema::of::<xylem_protocols::MemcachedConfig>()
        }
        ProtocolKind::Http => Schema::of::<xylem_protocols::HttpConfig>(),
        ProtocolKind::Http2 => Schema::of::<xylem_protocols::Http2Config>(),
        ProtocolKind::Grpc => Schema::of::<xylem_protocols::GrpcConfig>(),
        ProtocolKind::Masstree => Schema::of::<xylem_protocols::MasstreeConfig>(),
        ProtocolKind::Dns => Schema::of::<xylem_protocols::DnsConfig>(),
        ProtocolKind::NvmeTcp => Schema::of::<xylem_protocols::NvmeTcpConfig>(),
        ProtocolKind::ClickHouse => Schema::of::<xylem_protocols::ClickHouseConfig>(),
        ProtocolKind::XylemEcho => Schema::of::<xylem_protocols::XylemEchoConfig>(),
    }
}

/// The location of a key in a profile
#[derive(Debug, Clone, Default)]
struct Path(Vec<Step>);

#[derive(Debug, Clone)]
enum Step {
    Key(String),
    Index(usize),
}

impl Path {
    fn key(&self, key: &str) -> Path {
        let mut path = self.clone();
        path.0.push(Step::Key(key.to_string()));
        path
    }

    fn index(&self, index: usize) -> Path {
        let mut path = self.clone();
        path.0.push(Step::Index(index));
        path
    }

    /// The traffic group the path is in
    fn group(&self) -> Option<usize> {
        match &self.0[..] {
            [Step::Key(key), Step::Index(index), ..] if key == "traffic_groups" => Some(*index),
            _ => None,
        }
    }

    /// The path with `[]` for every index, as `REMOVED` names keys
    fn pattern(&self) -> String {
        self.write(false)
    }

    fn write(&self, indexes: bool) -> String {
        let mut text = String::new();
        for step in &self.0 {
            match step {
                Step::Key(key) if text.is_empty() => text.push_str(key),
                Step::Key(key) => {
                    text.push('.');
                    text.push_str(key);
                }
                Step::Index(index) if indexes => text.push_str(&format!("[{index}]")),
                Step::Index(_) => text.push_str("[]"),
            }
        }
        text
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.write(true))
    }
}

/// A key that does not belong where it is
enum Problem {
    Unknown {
        parent: Path,
        key: String,
        valid: Vec<String>,
    },
    Removed {
        path: Path,
        message: &'static str,
    },
}

impl Problem {
    /// Key `key` of table `parent` is not one of `valid`
    fn unknown<'a>(parent: &Path, key: &str, valid: impl IntoIterator<Item = &'a str>) -> Self {
        let path = parent.key(key);
        match removed(&path, None) {
            Some(message) => Problem::Removed { path, message },
            None => Problem::Unknown {
                parent: parent.clone(),
                key: key.to_string(),
                valid: valid.into_iter().map(str::to_string).collect(),
            },
        }
    }

    /// The message, naming traffic groups by `group_name`
    fn render<'a>(&self, group_name: impl Fn(usize) -> Option<&'a str>) -> String {
        match self {
            Problem::Unknown { parent, key, valid } => {
                let place = if parent.0.is_empty() {
                    "at the top level".to_string()
                } else {
                    format!("in {parent}")
                };
                let hint = match closest(key, valid) {
                    Some(candidate) => format!(" (did you mean `{candidate}`?)"),
                    None => String::new(),
                };
                if valid.is_empty() {
                    return format!("unknown field `{key}` {place}{hint}, which takes no fields");
                }
                let valid: Vec<String> = valid.iter().map(|key| format!("`{key}`")).collect();
                format!("unknown field `{key}` {place}{hint}; expected one of {}", valid.join(", "))
            }
            Problem::Removed { path, message } => match path.group() {
                Some(index) => format!(
                    "Traffic group {} '{}': {}",
                    index,
                    group_name(index).unwrap_or_default(),
                    message
                ),
                None => message.to_string(),
            },
        }
    }
}

/// The valid key closest to the unknown `key`, if one is close enough to be
/// a typo of it
fn closest<'a>(key: &str, valid: &'a [String]) -> Option<&'a str> {
    valid
        .iter()
        .map(|candidate| (edit_distance(key, candidate), candidate.as_str()))
        .filter(|&(distance, _)| distance <= 1 + key.chars().count() / 4)
        .min_by_key(|&(distance, _)| distance)
        .map(|(_, candidate)| candidate)
}

/// Levenshtein distance between `a` and `b`
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, a_char) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, b_char) in b.iter().enumerate() {
            let above = row[j + 1];
            row[j + 1] = if a_char == *b_char {
                diagonal
            } else {
                1 + diagonal.min(above).min(row[j])
            };
            diagonal = above;
        }
    }
    row[b.len()]
}

/// The keys a table may have where a schema applies
enum Keys<'a> {
    /// Not a table
    None,
    /// Any keys: a free-form value, or an untagged enum checked elsewhere
    Any,
    /// Exactly these keys, each with its schema
    Fields(BTreeMap<&'a str, &'a Value>),
    /// Any keys, each value with this schema
    Map(&'a Value),
    /// A tagged enum whose tag, this key, names no variant
    UnknownTag(&'a str),
}

/// A JSON schema as schemars generates it, definitions included
struct Schema(Value);

impl Schema {
    fn of<T: schemars::JsonSchema>() -> Self {
        Schema(schemars::schema_for!(T).to_value())
    }

    /// `node` with its references followed
    fn resolve<'a>(&'a self, mut node: &'a Value) -> &'a Value {
        while let Some(reference) = node.get("$ref").and_then(Value::as_str) {
            let target = match reference.strip_prefix("#/$defs/") {
                Some(name) => self.0.get("$defs").and_then(|defs| defs.get(name)),
                None if reference == "#" => Some(&self.0),
                None => None,
            };
            match target {
                Some(target) => node = target,
                None => break,
            }
        }
        node
    }

    /// The keys a table may have where `node` applies. With `table`, a
    /// tagged enum gives the keys of the variant its tag names; without it,
    /// the keys of all variants.
    fn keys<'a>(&'a self, node: &'a Value, table: Option<&Map<String, Value>>) -> Keys<'a> {
        let node = self.resolve(node);
        let Some(object) = node.as_object() else {
            // The schema `true` admits any value
            return if node.as_bool() == Some(true) {
                Keys::Any
            } else {
                Keys::None
            };
        };
        let variants = object.get("oneOf").or_else(|| object.get("anyOf"));
        if let Some(variants) = variants.and_then(Value::as_array) {
            return self.variant_keys(variants, table);
        }
        let fields = || -> BTreeMap<&'a str, &'a Value> {
            let properties = object.get("properties").and_then(Value::as_object);
            properties
                .into_iter()
                .flatten()
                .map(|(key, value)| (key.as_str(), value))
                .collect()
        };
        match object.get("additionalProperties") {
            Some(Value::Bool(false)) => Keys::Fields(fields()),
            Some(values @ Value::Object(_)) if !object.contains_key("properties") => {
                Keys::Map(values)
            }
            // An open table, or a value of any type
            _ if object.contains_key("properties") || !object.contains_key("type") => Keys::Any,
            _ if object.get("type").and_then(Value::as_str) == Some("object") => Keys::Any,
            _ => Keys::None,
        }
    }

    /// The keys of a table that may take any of `variants`
    fn variant_keys<'a>(
        &'a self,
        variants: &'a [Value],
        table: Option<&Map<String, Value>>,
    ) -> Keys<'a> {
        let mut tables = Vec::new();
        for variant in variants {
            match self.keys(variant, table) {
                Keys::None => {}
                Keys::Fields(fields) => tables.push(fields),
                keys => return keys,
            }
        }
        if tables.len() <= 1 {
            return tables.pop().map_or(Keys::None, Keys::Fields);
        }
        let constant = |schema: &'a Value| self.resolve(schema).get("const");
        // In a tagged enum every variant has the tag, a constant
        let tag = tables[0].keys().copied().find(|&key| {
            tables
                .iter()
                .all(|fields| fields.get(key).is_some_and(|&schema| constant(schema).is_some()))
        });
        let all_keys = |tables: Vec<BTreeMap<&'a str, &'a Value>>| {
            Keys::Fields(tables.into_iter().flatten().collect())
        };
        let Some(tag) = tag else {
            // An untagged enum: only the table's context tells its variant
            return if table.is_some() {
                Keys::Any
            } else {
                all_keys(tables)
            };
        };
        let Some(value) = table.and_then(|table| table.get(tag)) else {
            return all_keys(tables);
        };
        match tables.iter().position(|fields| constant(fields[tag]) == Some(value)) {
            Some(index) => Keys::Fields(tables.swap_remove(index)),
            None => Keys::UnknownTag(tag),
        }
    }

    /// The schema of the items where `node` applies, if it is an array
    fn items<'a>(&'a self, node: &'a Value) -> Option<&'a Value> {
        let node = self.resolve(node);
        if let Some(items) = node.get("items") {
            return Some(items);
        }
        let variants = node.get("anyOf").or_else(|| node.get("oneOf"))?.as_array()?;
        variants.iter().find_map(|variant| self.items(variant))
    }

    /// Add the unknown and removed keys of `value` at `path` to `problems`
    fn check(&self, node: &Value, value: &Value, path: &Path, problems: &mut Vec<Problem>) {
        match value {
            Value::Object(table) => match self.keys(node, Some(table)) {
                Keys::Fields(fields) => {
                    for (key, child) in table {
                        match fields.get(key.as_str()) {
                            Some(schema) => self.check(schema, child, &path.key(key), problems),
                            None => {
                                problems.push(Problem::unknown(path, key, fields.keys().copied()))
                            }
                        }
                    }
                }
                Keys::Map(schema) => {
                    for (key, child) in table {
                        self.check(schema, child, &path.key(key), problems);
                    }
                }
                // serde reports an unknown variant, unless it was removed
                Keys::UnknownTag(tag) => {
                    let tag_path = path.key(tag);
                    if let Some(message) = removed(&tag_path, table[tag].as_str()) {
                        problems.push(Problem::Removed { path: tag_path, message });
                    }
                }
                Keys::Any | Keys::None => {}
            },
            Value::Array(items) => {
                if let Some(schema) = self.items(node) {
                    for (index, item) in items.iter().enumerate() {
                        self.check(schema, item, &path.index(index), problems);
                    }
                }
            }
            _ => {}
        }
    }

    /// Check `segments`, which continue `path`, from `node`. `value` is the
    /// profile's value at `path`, if it has one.
    fn check_path<'s, 'a>(
        &'s self,
        mut node: &'s Value,
        mut value: Option<&Value>,
        mut path: Path,
        segments: &[PathSegment],
        group_name: impl Fn(usize) -> Option<&'a str>,
    ) -> Result<()> {
        for segment in segments {
            match segment {
                PathSegment::Key(key) => {
                    match self.keys(node, None) {
                        Keys::Fields(fields) => match fields.get(key.as_str()) {
                            Some(schema) => node = schema,
                            None => {
                                let problem = Problem::unknown(&path, key, fields.keys().copied());
                                bail!(problem.render(group_name));
                            }
                        },
                        Keys::Map(schema) => node = schema,
                        Keys::Any => return Ok(()),
                        Keys::None | Keys::UnknownTag(_) => bail!("{path} is not a table"),
                    }
                    value = value.and_then(|value| value.get(key));
                    path = path.key(key);
                }
                PathSegment::Index(index) => {
                    let Some(items) = self.items(node) else {
                        bail!("{path} is not an array");
                    };
                    match value {
                        Some(Value::Array(array)) if *index < array.len() => {}
                        Some(Value::Array(array)) => bail!(
                            "index {} is out of bounds for {} (length: {})",
                            index,
                            path,
                            array.len()
                        ),
                        _ => bail!("index {index} is out of bounds: {path} is not set"),
                    }
                    node = items;
                    value = value.and_then(|value| value.get(index));
                    path = path.index(*index);
                }
                PathSegment::Append => {
                    if self.items(node).is_none() {
                        bail!("{path} is not an array, so `+` cannot append to it");
                    }
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}
