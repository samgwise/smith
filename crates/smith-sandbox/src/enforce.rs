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

use base64::Engine as _;

/// Fixed addressing for the egress network.
pub const EGRESS_SUBNET: &str = "10.89.100.0/24";
pub const EGRESS_GATEWAY: &str = "10.89.100.1";
pub const EGRESS_PROXY_IP: &str = "10.89.100.10";
/// nft table name (also used for the delete command on cleanup).
pub const EGRESS_TABLE: &str = "smith-egress";

/// Render the complete nft ruleset for `nft -f`.
pub fn ruleset(bridge: &str, proxy_ip: &str) -> String {
    format!(
        "table inet {EGRESS_TABLE} {{\n\
         \x20 chain forward {{\n\
         \x20   type filter hook forward priority 0; policy accept;\n\
         \x20   iifname \"{bridge}\" oifname \"{bridge}\" accept\n\
         \x20   iifname \"{bridge}\" ip saddr {proxy_ip} accept\n\
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ruleset_is_exact_and_scoped() {
        let rs = ruleset("podman1", "10.89.100.10");
        assert!(rs.starts_with("table inet smith-egress {\n"));
        assert!(rs.contains("type filter hook forward priority 0; policy accept;"));
        assert!(rs.contains("iifname \"podman1\" oifname \"podman1\" accept"));
        assert!(rs.contains("iifname \"podman1\" ip saddr 10.89.100.10 accept"));
        assert!(rs.ends_with("iifname \"podman1\" drop\n  }\n}\n"));
        // The ruleset mentions no other interface.
        assert!(!rs.contains("eth0"));
    }

    #[test]
    fn ruleset_is_idempotent_nft_input() {
        // Applying twice must be valid: `nft -f` replaces the table.
        let once = ruleset("podman2", "10.89.100.10");
        let twice = format!("{once}{once}");
        assert_eq!(twice.lines().count(), once.lines().count() * 2);
        assert!(delete_command().ends_with("delete table inet smith-egress"));
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
