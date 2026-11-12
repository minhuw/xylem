//! Keep the book's TOML examples loadable as the CLI's profile format evolves,
//! and its pages linked.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use toml::{Table, Value};
use xylem_cli::config::{ntcp, NtcpBackend, ProfileConfig};
use xylem_cli::multi_protocol::MultiProtocol;

/// Load `profile` as `xylem -P` does: unknown keys, parsing, validation.
/// A profile for an ntcp backend this build lacks is only parsed, and
/// returns None.
fn load(profile: &str, dir: &Path) -> anyhow::Result<Option<ProfileConfig>> {
    let file = dir.join("profile.toml");
    std::fs::write(&file, profile)?;
    let mut config = ProfileConfig::from_file(&file)?;
    let built = match config.ntcp.as_ref().map(|ntcp| &ntcp.backend) {
        Some(NtcpBackend::Dpdk) => ntcp::SUPPORTED_DPDK,
        Some(NtcpBackend::AfXdp) => ntcp::SUPPORTED_AF_XDP,
        _ => ntcp::SUPPORTED || config.traffic_groups.iter().all(|g| g.transport != "ntcp"),
    };
    if !built {
        return Ok(None);
    }
    config.validate()?;
    Ok(Some(config))
}

/// The protocol of a fragment's traffic group that names none: that of its
/// protocol page, or Redis, which the configuration pages use
fn page_protocol(path: &Path) -> &'static str {
    match path.file_stem().and_then(|stem| stem.to_str()) {
        Some("dns") => "dns",
        Some("masstree") => "masstree",
        Some("memcached") => "memcached-ascii",
        _ => "redis",
    }
}

/// Complete the fragment `table` into a profile: missing sections and
/// traffic group keys get minimal values (a UDP group the echo protocol),
/// each group a threshold names exists, and an excerpt of weighted
/// `operations.commands` gets its `strategy`
fn complete(mut table: Table, protocol: &str) -> Table {
    let defaults: Table = toml::from_str(
        "experiment = { name = \"book\", duration = \"1s\" }\noutput = { file = \"book\" }",
    )
    .unwrap();
    for (key, value) in defaults {
        table.entry(key).or_insert(value);
    }
    let groups = match table.remove("traffic_groups") {
        // `[traffic_groups.protocol_config]` alone: one group's tables
        Some(Value::Table(group)) => vec![Value::Table(group)],
        Some(Value::Array(groups)) => groups,
        Some(other) => panic!("traffic_groups is {other}"),
        None => vec![Value::Table(Table::new())],
    };
    let mut groups = groups;
    let thresholds = table.get("thresholds").and_then(Value::as_table);
    for key in thresholds.into_iter().flat_map(Table::keys) {
        let Some((_, name)) = key.split_once("{group=") else {
            continue;
        };
        let name = name.trim_end_matches('}');
        if !groups
            .iter()
            .any(|group| group.get("name").and_then(Value::as_str) == Some(name))
        {
            groups.push(Value::Table(Table::from_iter([("name".into(), name.into())])));
        }
    }
    let groups = groups.into_iter().enumerate().map(|(index, group)| {
        let Value::Table(mut group) = group else {
            panic!("traffic group {group}")
        };
        group.entry("name").or_insert_with(|| format!("group{index}").into());
        let udp = group.get("transport").and_then(Value::as_str) == Some("udp");
        let protocol = if udp { "xylem-echo" } else { protocol };
        group.entry("protocol").or_insert_with(|| protocol.into());
        group.entry("target").or_insert_with(|| "127.0.0.1:1".into());
        group.entry("threads").or_insert_with(|| Value::Array(vec![0.into()]));
        group.entry("connections_per_thread").or_insert_with(|| 1.into());
        group.entry("max_pending_per_connection").or_insert_with(|| 1.into());
        let closed_loop = || Table::from_iter([("type".into(), "closed-loop".into())]).into();
        group.entry("traffic_policy").or_insert_with(closed_loop);
        let config = group.entry("protocol_config").or_insert_with(|| Table::new().into());
        if let Some(Value::Table(operations)) = config.get_mut("operations") {
            if operations.contains_key("commands") {
                operations.entry("strategy").or_insert_with(|| "weighted".into());
            }
        }
        Value::Table(group)
    });
    table.insert("traffic_groups".into(), Value::Array(groups.collect()));
    table
}

/// Check every TOML block of the page at `path`. A block with all three
/// required sections is a complete profile and must load as it is; any other
/// block is a fragment, completed into a profile around it. Returns the
/// numbers of complete profiles and fragments, and adds each failure to `errors`.
fn check_markdown(path: &Path, dir: &Path, errors: &mut Vec<String>) -> (usize, usize) {
    let markdown = std::fs::read_to_string(path).unwrap();
    let (mut profiles, mut fragments) = (0, 0);
    for (index, block) in markdown.split("```toml\n").skip(1).enumerate() {
        let block = block.split("```").next().unwrap();
        let at = format!("{} TOML block {}", path.display(), index + 1);
        assert!(!block.contains("[target]"), "{at} uses the obsolete target section");
        let complete_profile = ["[experiment]", "[[traffic_groups]]", "[output]"]
            .iter()
            .all(|section| block.contains(section));
        if complete_profile {
            match load(block, dir) {
                Ok(Some(config)) => check_workloads(&config, Path::new(&at)),
                Ok(None) => {}
                Err(e) => errors.push(format!("{at}: {e:#}")),
            }
            profiles += 1;
        } else {
            fragments += 1;
            let table: Table = match toml::from_str(block) {
                Ok(table) => table,
                Err(e) => {
                    errors.push(format!("{at}: {e}"));
                    continue;
                }
            };
            let profile = toml::to_string(&complete(table, page_protocol(path))).unwrap();
            if let Err(e) = load(&profile, dir) {
                errors.push(format!("{at}: {e:#}\n{profile}"));
            }
        }
    }
    (profiles, fragments)
}

fn markdown_files(path: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            markdown_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "md") {
            files.push(path);
        }
    }
}

#[test]
fn book_profiles_are_valid() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../book/src");
    // The variable the password_env examples name; nextest runs each test
    // in a process of its own
    std::env::set_var("REDIS_PASSWORD", "book");
    let dir = tempfile::tempdir().unwrap();
    let mut files = Vec::new();
    markdown_files(&root, &mut files);
    let (mut profiles, mut fragments, mut errors) = (0, 0, Vec::new());
    for path in &files {
        let (page_profiles, page_fragments) = check_markdown(path, dir.path(), &mut errors);
        profiles += page_profiles;
        fragments += page_fragments;
    }
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
    // Catch a change to the block syntax that would skip every block
    assert!(profiles >= 15, "Expected at least 15 complete profiles, found {profiles}");
    assert!(fragments >= 70, "Expected at least 70 fragments, found {fragments}");
}

/// The lines of `markdown` outside code blocks
fn prose(markdown: &str) -> Vec<&str> {
    let mut in_code = false;
    let mut lines = Vec::new();
    for line in markdown.lines() {
        if line.trim_start().starts_with("```") {
            in_code = !in_code;
        } else if !in_code {
            lines.push(line);
        }
    }
    lines
}

/// The anchors of the headings of `markdown`, as mdBook makes them
fn anchors(markdown: &str) -> Vec<String> {
    let mut anchors = Vec::new();
    let mut seen: HashMap<String, usize> = HashMap::new();
    for line in prose(markdown) {
        let Some(heading) = line.strip_prefix('#') else {
            continue;
        };
        let id: String = heading
            .trim_start_matches('#')
            .trim()
            .chars()
            .filter_map(|ch| match ch {
                ch if ch.is_alphanumeric() || ch == '_' || ch == '-' => {
                    Some(ch.to_ascii_lowercase())
                }
                ch if ch.is_whitespace() => Some('-'),
                _ => None,
            })
            .collect();
        let count = seen.entry(id.clone()).or_default();
        anchors.push(if *count == 0 {
            id.clone()
        } else {
            format!("{id}-{count}")
        });
        *count += 1;
    }
    anchors
}

/// The targets of the inline links of `markdown`
fn links(markdown: &str) -> Vec<&str> {
    let mut links = Vec::new();
    for line in prose(markdown) {
        for (_, rest) in line.match_indices("](").map(|(at, _)| line.split_at(at + 2)) {
            if let Some(end) = rest.find(')') {
                links.push(&rest[..end]);
            }
        }
    }
    links
}

#[test]
fn book_links_resolve_and_summary_lists_every_page() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../book/src");
    let root = root.canonicalize().unwrap();
    let mut files = Vec::new();
    markdown_files(&root, &mut files);
    let mut errors = Vec::new();
    for path in &files {
        let markdown = std::fs::read_to_string(path).unwrap();
        for link in links(&markdown) {
            if link.starts_with("http://") || link.starts_with("https://") {
                continue;
            }
            let (file, anchor) = link.split_once('#').unwrap_or((link, ""));
            let target = if file.is_empty() {
                path.clone()
            } else {
                match path.parent().unwrap().join(file).canonicalize() {
                    Ok(target) if target.starts_with(&root) => target,
                    _ => {
                        errors.push(format!("{}: no page {link}", path.display()));
                        continue;
                    }
                }
            };
            if !anchor.is_empty() {
                let page = std::fs::read_to_string(&target).unwrap();
                if !anchors(&page).iter().any(|id| id == anchor) {
                    errors.push(format!("{}: no heading for {link}", path.display()));
                }
            }
        }
    }
    // mdBook builds only the pages SUMMARY.md lists
    let summary = std::fs::read_to_string(root.join("SUMMARY.md")).unwrap();
    let listed: Vec<PathBuf> = links(&summary)
        .iter()
        .map(|link| root.join(link).canonicalize().unwrap())
        .collect();
    for path in files.iter().filter(|path| !path.ends_with("SUMMARY.md")) {
        if !listed.contains(path) {
            errors.push(format!("{} is not in SUMMARY.md", path.display()));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// Every TOML file under `profiles/`, including subdirectories.
fn bundled_profiles() -> Vec<PathBuf> {
    fn visit(path: &Path, profiles: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, profiles);
            } else if path.extension().is_some_and(|ext| ext == "toml") {
                profiles.push(path);
            }
        }
    }
    let mut profiles = Vec::new();
    visit(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../profiles"), &mut profiles);
    profiles
}

#[test]
fn bundled_profiles_are_valid() {
    let profiles = bundled_profiles();
    for path in &profiles {
        let mut config =
            ProfileConfig::from_file(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        config.validate().unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        check_workloads(&config, path);
    }
    assert_eq!(profiles.len(), 15, "Update coverage when adding or removing bundled profiles");
}

#[test]
fn bundled_profiles_have_no_target_section() {
    for path in bundled_profiles() {
        let content = std::fs::read_to_string(&path).unwrap();
        let profile: toml::Table =
            toml::from_str(&content).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        // ProfileConfig has no top-level target, so serde would silently drop it.
        assert!(
            !profile.contains_key("target"),
            "{} has an ignored [target] section; set target in each traffic group",
            path.display()
        );
    }
}

/// Build each group's protocol the way a worker does, from the validated configs.
fn check_workloads(config: &ProfileConfig, path: &Path) {
    assert_eq!(config.protocol_configs.len(), config.traffic_groups.len(), "{}", path.display());
    for (group, protocol_config) in config.traffic_groups.iter().zip(&config.protocol_configs) {
        MultiProtocol::from_config(
            protocol_config,
            &group.target,
            &group.transport,
            group.max_pending_per_connection,
            config.experiment.seed,
        )
        .unwrap_or_else(|e| panic!("{} group {}: {e}", path.display(), group.name));
    }
}
