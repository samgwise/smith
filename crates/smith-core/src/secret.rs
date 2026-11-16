//! Secrets at rest: private files written atomically (S-15).
//!
//! Token and state files are created with owner-only permissions (mode 0600
//! on Unix) and land via temp-file-then-rename in the target directory, so a
//! crash never leaves a half-written secret behind and the file is never
//! briefly world-readable under a default umask.

use std::io::Write as _;
use std::path::Path;

use anyhow::Context as _;

/// Write `bytes` to `path` atomically and privately.
///
/// The file is created in the target directory with mode 0600 (Unix), synced,
/// and renamed over `path` — the rename also tightens the mode of a
/// previously wider file. On Windows the profile directory already scopes
/// access to the user; no ACL work is attempted here.
pub fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let dir = path
        .parent()
        .context("secret path has no parent directory")?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("secret")
        .to_string();
    let tmp = dir.join(format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));

    let result = (|| -> anyhow::Result<()> {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            opts.mode(0o600);
        }
        let mut file = opts
            .open(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("writing {}", tmp.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", tmp.display()))?;
        drop(file);
        std::fs::rename(&tmp, path)
            .with_context(|| format!("renaming {} into place", tmp.display()))?;
        Ok(())
    })();

    if result.is_err() {
        // Never leave a half-written secret (or temp file) behind.
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The written content round-trips and no temp sibling survives.
    #[test]
    fn write_round_trips_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret.bin");
        write_private(&path, b"tok-123").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"tok-123");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
    }

    /// A second write replaces the content (the rename replaces an existing
    /// file) and still leaves no temp behind.
    #[test]
    fn rewrite_replaces_the_file_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        write_private(&path, b"{\"v\":1}").unwrap();
        write_private(&path, b"{\"v\":2}").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"v\":2}");
    }

    /// S-15 (Unix): the file is owner-only — no group or world bits, and an
    /// existing wider-mode file is tightened by the rename.
    #[cfg(unix)]
    #[test]
    fn private_files_have_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");

        write_private(&path, b"tok").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "secret must be owner-only: {mode:o}");

        // An existing 0644 file is tightened by the next private write.
        std::fs::write(&path, b"old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_private(&path, b"new").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "wide mode must be tightened: {mode:o}");
    }
}
