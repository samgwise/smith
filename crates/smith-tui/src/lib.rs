//! Smith TUI frontend: ratatui-based interface over the headless core.

pub mod app;
pub mod gate;
pub mod ui;

pub use app::{App, AppParams};
pub use gate::{TuiGate, approval_channel};
