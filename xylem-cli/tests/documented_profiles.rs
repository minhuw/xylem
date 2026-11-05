//! Keep complete book examples loadable as the CLI's profile format evolves.
use std::path::Path;
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
