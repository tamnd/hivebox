//! `cargo xtask bump <version>`: move the whole workspace to a new version in one commit.
//!
//! Every crate shares one version and the internal dependencies are pinned exactly, so a release
//! touches the workspace version, one pin per crate and the changelog heading. Doing that by hand
//! is thirty edits where missing one gives a build that resolves a crate at the wrong version.

use std::path::Path;

/// The Python SDK's files that carry the version, and the line in each that starts with it.
const PYTHON: [(&str, &str); 2] = [
    ("sdk/python/pyproject.toml", "version = \""),
    ("sdk/python/hivebox/__init__.py", "__version__ = \""),
];

/// Rewrites `Cargo.toml`, `CHANGELOG.md` and the Python SDK's version under `root` for `version`.
pub(crate) fn run(root: &Path, version: &str) -> Result<(), String> {
    if !is_version(version) {
        return Err(format!("{version} is not a version, expected something like 0.1.2"));
    }
    let manifest_path = root.join("Cargo.toml");
    let manifest = read(&manifest_path)?;
    let old = current(&manifest).ok_or("no version in [workspace.package]")?;
    if old == version {
        return Err(format!("the workspace is already at {version}"));
    }
    // The changelog is checked first, so a release with nothing in it changes neither file.
    let changelog_path = root.join("CHANGELOG.md");
    let changelog = changelog_for(&read(&changelog_path)?, version)?;
    write(&manifest_path, &manifest_for(&manifest, &old, version))?;
    write(&changelog_path, &changelog)?;
    for (file, prefix) in PYTHON {
        let path = root.join(file);
        write(&path, &line_for(&read(&path)?, prefix, &old, version))?;
    }

    println!("{old} -> {version}");
    println!("now run `cargo update --workspace` so the lockfile agrees, then commit");
    Ok(())
}

fn is_version(v: &str) -> bool {
    let parts: Vec<&str> = v.split('.').collect();
    parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

/// The version in `[workspace.package]`, which is the first `version = ` line in the file.
fn current(manifest: &str) -> Option<String> {
    manifest
        .lines()
        .find_map(|l| l.strip_prefix("version = \""))
        .and_then(|rest| rest.split('"').next())
        .map(str::to_string)
}

fn manifest_for(manifest: &str, old: &str, new: &str) -> String {
    let mut out = String::with_capacity(manifest.len());
    let mut replaced_package = false;
    for line in manifest.split_inclusive('\n') {
        if !replaced_package && line.starts_with("version = \"") {
            out.push_str(&line.replacen(old, new, 1));
            replaced_package = true;
        } else if line.contains("path = \"crates/") {
            out.push_str(&line.replacen(&format!("\"={old}\""), &format!("\"={new}\""), 1));
        } else {
            out.push_str(line);
        }
    }
    out
}

/// Moves the first line that starts with `prefix` from `old` to `new`.
fn line_for(text: &str, prefix: &str, old: &str, new: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut done = false;
    for line in text.split_inclusive('\n') {
        if !done && line.starts_with(prefix) {
            out.push_str(&line.replacen(old, new, 1));
            done = true;
        } else {
            out.push_str(line);
        }
    }
    out
}

/// Turns the `## Unreleased` section into `## <version>` and opens a new empty one above it.
fn changelog_for(changelog: &str, version: &str) -> Result<String, String> {
    const HEADING: &str = "## Unreleased\n";
    let at = changelog.find(HEADING).ok_or("CHANGELOG.md has no `## Unreleased` section")?;
    let rest = &changelog[at + HEADING.len()..];
    let body_end = rest.find("\n## ").map_or(rest.len(), |i| i + 1);
    if rest[..body_end].trim().is_empty() {
        return Err("the Unreleased section is empty, so there is nothing to release".into());
    }
    Ok(format!("{}{HEADING}\n## {version}\n{rest}", &changelog[..at]))
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("could not read {}: {e}", path.display()))
}

fn write(path: &Path, text: &str) -> Result<(), String> {
    std::fs::write(path, text).map_err(|e| format!("could not write {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = "[workspace.package]\nversion = \"0.0.0\"\n\n[workspace.dependencies]\n\
        hive-types = { version = \"=0.0.0\", path = \"crates/hive-types\" }\n\
        tokio = { version = \"0.0.0\" }\n";

    #[test]
    fn moves_the_package_and_every_pin_and_nothing_else() {
        let out = manifest_for(MANIFEST, "0.0.0", "0.0.1");
        assert!(out.contains("version = \"0.0.1\"\n"));
        assert!(out.contains("\"=0.0.1\", path = \"crates/hive-types\""));
        assert!(out.contains("tokio = { version = \"0.0.0\" }"));
        assert_eq!(current(&out).as_deref(), Some("0.0.1"));
    }

    #[test]
    fn the_python_version_moves_and_nothing_else() {
        let py = "[project]\nname = \"hivebox\"\nversion = \"0.0.1\"\ndependencies = [\"grpcio>=0.0.1\"]\n";
        let out = line_for(py, PYTHON[0].1, "0.0.1", "0.0.2");
        assert!(out.contains("version = \"0.0.2\"\n") && out.contains("grpcio>=0.0.1"));
        let init = "__version__ = \"0.0.1\"\n";
        assert_eq!(line_for(init, PYTHON[1].1, "0.0.1", "0.0.2"), "__version__ = \"0.0.2\"\n");
    }

    #[test]
    fn the_changelog_gets_a_new_unreleased_section() {
        let log = "# Changelog\n\n## Unreleased\n\nA thing.\n\n## 0.0.1\n\nOld.\n";
        let out = changelog_for(log, "0.0.2").unwrap();
        assert_eq!(
            out,
            "# Changelog\n\n## Unreleased\n\n## 0.0.2\n\nA thing.\n\n## 0.0.1\n\nOld.\n"
        );
        assert!(changelog_for("# Changelog\n\n## Unreleased\n\n## 0.0.1\n", "0.0.2").is_err());
    }

    #[test]
    fn versions_are_three_numbers() {
        assert!(is_version("0.1.12"));
        assert!(!is_version("v0.1.0"));
        assert!(!is_version("0.1"));
        assert!(!is_version("0.1.0-rc1"));
    }
}
