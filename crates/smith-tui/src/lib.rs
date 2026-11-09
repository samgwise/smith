//! Smith TUI frontend: ratatui-based interface over the headless core.
//!
//! The UI is front-end only: sessions come through the [`link::SessionLink`]
//! seam — [`link::LocalLink`] (in-process harness) or [`link::RemoteLink`]
//! (thin client for `smith attach`).

pub mod app;
pub mod gate;
pub mod link;
pub mod ui;

pub use app::{App, AppParams};
pub use gate::{TuiGate, approval_channel};
pub use link::{LinkEvent, LocalLink, ProviderFactory, RemoteLink, SessionLink};
