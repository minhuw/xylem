//! Keep complete book examples loadable as the CLI's profile format evolves.
use std::path::{Path, PathBuf};
use xylem_cli::config::ProfileConfig;

fn check_markdown(path: &Path) -> usize {
    let markdown = std::fs::read_to_string(path).unwrap();
    let mut checked = 0;
    for block in markdown.split("```toml\n").skip(1) {
        let profile = block.split("```").next().unwrap();
        // Reference fragments deliberately show individual sections. A block
        // with all three top-level requirements is a complete runnable profile.
        if !["[experiment]", "[[traffic_groups]]", "[output]"]
            .iter()
            .all(|section| profile.contains(section))
        {
            continue;
        }
        let config: ProfileConfig =
            toml::from_str(profile).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        config.validate().unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        assert!(!profile.contains("[target]"), "{} uses obsolete target section", path.display());
        checked += 1;
    }
    checked
}

fn check_directory(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                check_directory(&path)
            } else if path.extension().is_some_and(|ext| ext == "md") {
                check_markdown(&path)
            } else {
                0
            }
        })
        .sum()
}

#[test]
fn complete_book_profiles_are_valid() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../book/src");
    let count = check_directory(&root);
    assert!(count >= 6, "Expected at least six complete profiles, found {count}");
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
        let config =
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

fn check_workloads(config: &ProfileConfig, path: &Path) {
    for group in &config.traffic_groups {
        let value = group.protocol_config.clone().unwrap_or_else(|| serde_json::json!({}));
        let result = match group.protocol.as_str() {
            "redis" => xylem_protocols::RedisProtocol::from_config(
                &serde_json::from_value(value).unwrap(),
                config.experiment.seed,
            )
            .map(|_| ()),
            "redis-cluster" => xylem_protocols::RedisClusterProtocol::from_config(
                &serde_json::from_value(value).unwrap(),
                config.experiment.seed,
            )
            .map(|_| ()),
            "memcached-ascii" => xylem_protocols::memcached::MemcachedAsciiProtocol::from_config(
                &serde_json::from_value(value).unwrap(),
                config.experiment.seed,
            )
            .map(|_| ()),
            "memcached-binary" => xylem_protocols::memcached::MemcachedBinaryProtocol::from_config(
                &serde_json::from_value(value).unwrap(),
                config.experiment.seed,
            )
            .map(|_| ()),
            "http" => {
                xylem_protocols::HttpProtocol::from_config(&serde_json::from_value(value).unwrap())
                    .map(|_| ())
            }
            "xylem-echo" => Ok(()),
            other => panic!("Add constructor validation for {other}"),
        };
        result.unwrap_or_else(|e| panic!("{} group {}: {e}", path.display(), group.name));
    }
}
