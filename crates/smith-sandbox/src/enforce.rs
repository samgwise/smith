//! Egress enforcement: nftables ruleset for the `smith-egress` network.
//!
//! The ruleset is scoped to the egress bridge interface only — the machine's
//! own networking is untouched (chain policy stays `accept`; every rule
//! matches `iifname <bridge>`).
//!
//! Forward-hook semantics: packets between two containers on the same bridge
//! are L2-bridged and may bypass the L3 forward hook entirely; when they do
//! hit it, the intra-bridge accept covers them. Egress from a container is
//! accepted only when the source is the proxy's pinned IP; everything else
//! (including IPv6 and IP-literal dials) drops.

use anyhow::Context as _;
use base64::Engine as _;

/// Fixed addressing for the egress network.
pub const EGRESS_SUBNET: &str = "10.89.100.0/24";
pub const EGRESS_GATEWAY: &str = "10.89.100.1";
pub const EGRESS_PROXY_IP: &str = "10.89.100.10";
/// nft table name (also used for the delete command on cleanup).
pub const EGRESS_TABLE: &str = "smith-egress";

/// Render the complete nft ruleset for `nft -f`.
///
/// The payload is an idempotent reload (S-28): the bare `table` line
/// declares (or merges into) the table, `delete table` removes it, and the
/// final block recreates it with exactly these rules — all in one `nft -f`
/// transaction, so stale rules can never accumulate across re-applies.
/// The `input` chain (S-08) closes the locally-destined gap: from the bridge
/// only DNS to the gateway is accepted into the machine; every other port
/// drops. The residual DNS channel to the gateway stays (documented).
pub fn ruleset(bridge: &str, proxy_ip: &str) -> String {
    format!(
        "table inet {EGRESS_TABLE}\n\
         delete table inet {EGRESS_TABLE}\n\
         table inet {EGRESS_TABLE} {{\n\
         \x20 chain forward {{\n\
         \x20   type filter hook forward priority 0; policy accept;\n\
         \x20   iifname \"{bridge}\" oifname \"{bridge}\" accept\n\
         \x20   iifname \"{bridge}\" ip saddr {proxy_ip} accept\n\
         \x20   iifname \"{bridge}\" drop\n\
         \x20 }}\n\
         \x20 chain input {{\n\
         \x20   type filter hook input priority 0; policy accept;\n\
         \x20   iifname \"{bridge}\" ip saddr {EGRESS_GATEWAY} udp dport 53 accept\n\
         \x20   iifname \"{bridge}\" ip saddr {EGRESS_GATEWAY} tcp dport 53 accept\n\
         \x20   iifname \"{bridge}\" drop\n\
         \x20 }}\n\
         }}\n"
    )
}

/// Command that removes the table (best-effort; error ignored at call site).
pub fn delete_command() -> String {
    format!("nft delete table inet {EGRESS_TABLE}")
}

/// Encode a ruleset for transport through `podman machine ssh` as a single
/// argument (avoids quoting and newline issues entirely).
pub fn encode_payload(ruleset: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(ruleset)
}

/// Parse a podman connection URI (`ssh://user@host:port/socket`) into
/// `(user, host, port)` for the SshHost enforcement transport.
pub fn ssh_target_from_uri(uri: &str) -> anyhow::Result<(String, String, u16)> {
    let rest = uri
        .strip_prefix("ssh://")
        .with_context(|| format!("unsupported connection URI (need ssh): {uri}"))?;
    let (user, hostport) = rest
        .split_once('@')
        .with_context(|| format!("connection URI has no user: {uri}"))?;
    let hostport = hostport.split('/').next().unwrap_or(hostport);
    let (host, port) = hostport
        .rsplit_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().unwrap_or(22)))
        .unwrap_or_else(|| (hostport.to_string(), 22));
    anyhow::ensure!(!host.is_empty(), "connection URI has no host: {uri}");
    Ok((user.to_string(), host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ruleset_is_exact_and_scoped() {
        let rs = ruleset("podman1", "10.89.100.10");
        assert!(rs.contains("table inet smith-egress {"));
        assert!(rs.contains("type filter hook forward priority 0; policy accept;"));
        assert!(rs.contains("iifname \"podman1\" oifname \"podman1\" accept"));
        assert!(rs.contains("iifname \"podman1\" ip saddr 10.89.100.10 accept"));
        assert!(rs.contains("iifname \"podman1\" drop\n  }"));
        // The ruleset mentions no other interface.
        assert!(!rs.contains("eth0"));
    }

    /// S-08: the input chain closes the locally-destined gap — from the
    /// bridge only DNS (udp/tcp 53) to the gateway enters the machine.
    #[test]
    fn ruleset_contains_a_dns_only_input_chain() {
        let rs = ruleset("podman1", "10.89.100.10");
        assert!(rs.contains("type filter hook input priority 0; policy accept;"));
        assert!(
            rs.contains("iifname \"podman1\" ip saddr 10.89.100.1 udp dport 53 accept"),
            "DNS over UDP must be accepted"
        );
        assert!(
            rs.contains("iifname \"podman1\" ip saddr 10.89.100.1 tcp dport 53 accept"),
            "DNS over TCP must be accepted"
        );
        let input_pos = rs.find("hook input").expect("input chain");
        let input_section = &rs[input_pos..];
        assert!(input_section.contains("iifname \"podman1\" drop"));
    }

    /// S-28: the payload is an idempotent reload — declare, delete, then
    /// recreate the table inside one `nft -f` transaction, so re-applying
    /// never appends stale rules.
    #[test]
    fn ruleset_reloads_idempotently() {
        let rs = ruleset("podman1", "10.89.100.10");
        let lines: Vec<&str> = rs.lines().collect();
        assert_eq!(lines[0], "table inet smith-egress", "declare first");
        assert_eq!(lines[1], "delete table inet smith-egress", "then delete");
        assert_eq!(lines[2], "table inet smith-egress {", "then recreate");
        // Exactly one full table definition follows the prelude.
        assert_eq!(rs.matches("table inet smith-egress").count(), 3);
        assert!(delete_command().ends_with("delete table inet smith-egress"));
    }

    #[test]
    fn parses_ssh_connection_uris() {
        assert_eq!(
            ssh_target_from_uri("ssh://user@127.0.0.1:59921/run/user/1000/podman/podman.sock")
                .unwrap(),
            ("user".into(), "127.0.0.1".into(), 59921)
        );
        assert_eq!(
            ssh_target_from_uri("ssh://sam@gpu-box.local/run/podman/podman.sock").unwrap(),
            ("sam".into(), "gpu-box.local".into(), 22)
        );
        assert!(ssh_target_from_uri("unix:///run/podman/podman.sock").is_err());
        assert!(ssh_target_from_uri("ssh://host-only/socket").is_err());
    }

    #[test]
    fn payload_survives_base64_round_trip() {
        let rs = ruleset("podman1", "10.89.100.10");
        let encoded = encode_payload(&rs);
        assert!(!encoded.contains(['\n', '\r', ' ', '"']));
        let decoded = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(&encoded)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, rs);
    }
}
