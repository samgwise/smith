//! Smith protocol: the frozen wire contract between thin clients (the TUI,
//! CI issuers) and the `smith serve` controller daemon.
//!
//! - [`events`] — envelope and message types (the M13 compatibility freeze).
//! - [`http`] — hand-rolled HTTP/1.1 plumbing shared by both ends.
//! - [`client`] — the raw client + SSE envelope stream.

pub mod client;
pub mod events;
pub mod http;

pub use client::{EventStream, ServeClient, decision_word, read_token_file, token_file};
pub use events::{
    ApprovalPost, ClearancePost, CreateReply, CreateSession, DEFAULT_SERVE_PORT, DiffReply,
    Envelope, PROTOCOL_VERSION, ProviderPost, ResumePost, ResumeReply, SessionStatus,
    SessionSummary, StartTurn, WireEvent, render_diff,
};
