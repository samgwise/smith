//! Project instruction files: `AGENTS.md` (fallback `CLAUDE.md`) in the
//! workspace root, injected into the system prompt.
//!
//! Loaded once per session on purpose: the system prompt must stay
//! byte-stable across calls so provider prompt caches keep hitting.

use std::path::Path;

/// An instruction file larger than this is truncated with a visible marker
/// (S-22): the prompt must not grow without limit from a huge file.
pub const MAX_INSTRUCTION_BYTES: usize = 256 * 1024;

/// Load project instructions from the workspace root, if present.
/// `AGENTS.md` wins over `CLAUDE.md`; files over [`MAX_INSTRUCTION_BYTES`]
/// are truncated with a visible marker (S-22).
pub fn load_project_instructions(workspace: &Path) -> Option<String> {
    for name in ["AGENTS.md", "CLAUDE.md"] {
        if let Ok(raw) = std::fs::read_to_string(workspace.join(name)) {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.len() > MAX_INSTRUCTION_BYTES {
                let mut cut = trimmed
                    .char_indices()
                    .map(|(i, _)| i)
                    .find(|i| *i >= MAX_INSTRUCTION_BYTES)
                    .unwrap_or(trimmed.len());
                while !trimmed.is_char_boundary(cut) {
                    cut += 1;
                }
                return Some(format!(
                    "{}\n[smith: truncated — {} exceeds {} KiB]",
                    &trimmed[..cut],
                    name,
                    MAX_INSTRUCTION_BYTES / 1024
                ));
            }
            return Some(trimmed.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agents_md_wins_over_claude_md() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("AGENTS.md"), "agents rules").unwrap();
        std::fs::write(tmp.path().join("CLAUDE.md"), "claude rules").unwrap();
        assert_eq!(
            load_project_instructions(tmp.path()).as_deref(),
            Some("agents rules")
        );
    }

    #[test]
    fn claude_md_is_the_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("CLAUDE.md"), "claude rules").unwrap();
        assert_eq!(
            load_project_instructions(tmp.path()).as_deref(),
            Some("claude rules")
        );
    }

    #[test]
    fn none_when_absent_or_empty() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(load_project_instructions(tmp.path()).is_none());
        std::fs::write(tmp.path().join("AGENTS.md"), "   \n").unwrap();
        assert!(load_project_instructions(tmp.path()).is_none());
    }

    /// S-22: an instruction file over the cap is truncated with a visible
    /// marker instead of growing the prompt without limit.
    #[test]
    fn oversized_instructions_are_truncated_with_a_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let body = "x".repeat(MAX_INSTRUCTION_BYTES + 4096);
        std::fs::write(tmp.path().join("AGENTS.md"), &body).unwrap();
        let loaded = load_project_instructions(tmp.path()).unwrap();
        assert!(
            loaded.len() < MAX_INSTRUCTION_BYTES + 4096,
            "must be truncated"
        );
        assert!(
            loaded.contains("[smith: truncated — AGENTS.md exceeds 256 KiB]"),
            "{loaded}"
        );
        assert!(loaded.starts_with("xxx"), "the head of the file is kept");
    }
}
