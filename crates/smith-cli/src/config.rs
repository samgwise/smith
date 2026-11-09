//! Configuration lives in smith-server (the daemon reads the same file);
//! this module re-exports it for the CLI.

pub use smith_server::config::{Config, VerifyConfig, ensure_example_config, parse_network_mode};
