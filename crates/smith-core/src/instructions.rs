//! Project instruction files: `AGENTS.md` (fallback `CLAUDE.md`) in the
//! workspace root, injected into the system prompt.
//!
//! Loaded once per session on purpose: the system prompt must stay
//! byte-stable across calls so provider prompt caches keep hitting.

use std::path::Path;

/// Load project instructions from the workspace root, if present.
/// `AGENTS.md` wins over `CLAUDE.md`.
pub fn load_project_instructions(workspace: &Path) -> Option<String> {
    for name in ["AGENTS.md", "CLAUDE.md"] {
        if let Ok(raw) = std::fs::read_to_string(workspace.join(name)) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
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
}
