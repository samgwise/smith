//! Egress allowlist filtering for smith sandbox containers.
//!
//! The proxy binary in this crate runs inside the podman machine as a sidecar
//! and permits only destinations on the allowlist. HTTPS is filtered from the
//! `CONNECT` request line — traffic stays end-to-end encrypted (no MITM).

/// Destinations reachable by default: package registries and VCS hosts the
/// agent commonly needs. Replaceable via `sandbox.egress_allowlist`.
pub const DEFAULT_ALLOWLIST: &[&str] = &[
    "crates.io",
    "index.crates.io",
    "static.crates.io",
    "registry.npmjs.org",
    "pypi.org",
    "files.pythonhosted.org",
    "deb.debian.org",
    "archive.ubuntu.com",
    "security.ubuntu.com",
    "github.com",
    "objects.githubusercontent.com",
];

/// Path of the bundled Containerfile (source builds only; a released binary
/// would embed the source tree path, which is acceptable while smith is run
/// from source).
pub fn containerfile_path() -> std::io::Result<std::path::PathBuf> {
    Ok(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("container/Containerfile"))
}

/// Build context directory for the Containerfile (this crate's source).
pub fn container_context() -> std::io::Result<std::path::PathBuf> {
    Ok(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

/// Parsed allowlist: exact hostnames plus `*.suffix` wildcard entries.
#[derive(Clone, Debug, Default)]
pub struct Allowlist {
    entries: Vec<String>,
}

impl Allowlist {
    /// Parse a comma-separated allowlist value (as passed to the sidecar).
    pub fn parse(value: &str) -> Self {
        let entries = value
            .split(',')
            .map(|e| normalise_host(e.trim()))
            .filter(|e| !e.is_empty())
            .collect();
        Self { entries }
    }

    pub fn defaults() -> Self {
        Self::parse(&DEFAULT_ALLOWLIST.join(","))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether `host` (bare hostname, no port) is permitted.
    pub fn allows(&self, host: &str) -> bool {
        let host = normalise_host(host);
        if host.is_empty() {
            return false;
        }
        self.entries.iter().any(|entry| {
            if let Some(suffix) = entry.strip_prefix("*.") {
                host == suffix || host.ends_with(&format!(".{suffix}"))
            } else {
                host == *entry
            }
        })
    }
}

/// Lowercase, trim trailing dots, strip a `:port` suffix if present.
fn normalise_host(host: &str) -> String {
    let host = host.trim().trim_end_matches('.').to_lowercase();
    // IPv6 literals contain colons; only strip a port after an IPv4/name.
    match host.rfind(':') {
        Some(idx) if host[..idx].contains('.') => host[..idx].to_string(),
        _ => host,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_and_wildcard_matching() {
        let al = Allowlist::parse("crates.io, *.github.com");
        assert!(al.allows("crates.io"));
        assert!(al.allows("CRATES.io.")); // case + trailing dot
        assert!(!al.allows("evil-crates.io")); // not a subdomain
        assert!(al.allows("github.com"));
        assert!(al.allows("api.github.com"));
        assert!(!al.allows("github.com.evil.test")); // suffix must anchor
        assert!(!al.allows("example.com"));
    }

    #[test]
    fn host_header_ports_and_normalisation() {
        let al = Allowlist::parse("pypi.org");
        assert!(al.allows("pypi.org:443"));
        assert!(al.allows("  PYPI.org "));
        assert!(!al.allows(""));
        assert!(!al.allows("pypi.org.evil.test"));
    }

    #[test]
    fn defaults_cover_common_registries() {
        let al = Allowlist::defaults();
        assert!(al.allows("index.crates.io"));
        assert!(al.allows("registry.npmjs.org"));
        assert!(al.allows("objects.githubusercontent.com"));
        assert!(!al.allows("example.com"));
    }
}
