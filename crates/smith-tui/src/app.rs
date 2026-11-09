//! TUI application state and the main event loop.
//!
//! The App is front-end only: every session operation crosses the
//! [`SessionLink`] seam (`crate::link`) — in-process for `smith`, the serve
//! protocol for `smith attach`. The two modes render identically.

use std::time::{Duration, Instant};

use anyhow::Context;
use ratatui::text::Line;
use smith_core::{ApprovalDecision, ApprovalRequest, CoreEvent, Usage};
use tokio::sync::mpsc;

use crate::link::{LinkEvent, SessionLink};
use crate::ui;

/// Key events from the reader thread.
#[derive(Debug)]
pub enum InputEvent {
    Key(crossterm::event::KeyEvent),
}

pub struct AppParams {
    /// The session this window is attached to.
    pub link: Box<dyn SessionLink>,
    /// Whether prompts echo back as UserMessage events (remote mode).
    pub user_echo: bool,
    pub workspace_label: String,
    pub container_label: String,
    pub session_label: String,
    pub provider_name: String,
    pub model_name: String,
    /// Snapshot of MCP server status taken at attach time.
    pub mcp_status: Vec<String>,
}

/// What a modal answers: an agent tool approval or an egress clearance.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ModalKind {
    Tool,
    Clearance(u64),
}

struct PendingModal {
    call_id: String,
    request: ApprovalRequest,
    kind: ModalKind,
}

pub struct App {
    link: Box<dyn SessionLink>,
    user_echo: bool,
    busy: bool,

    pending_approval: Option<PendingModal>,

    /// Rendered transcript lines (already wrapped at draw time via `ui`).
    transcript: Vec<Line<'static>>,
    /// Assistant text still streaming in.
    streaming: String,
    tools: Vec<(String, String, bool)>, // name, detail, finished_ok
    input: String,
    usage: Usage,
    /// `None` = follow the tail; `Some(offset)` = user scrolled up.
    scroll: Option<u16>,
    workspace_label: String,
    container_label: String,
    session_label: String,
    provider_name: String,
    model_name: String,
    mcp_status: Vec<String>,
    /// Last Ctrl+C while busy (quit-with-second-press flow).
    last_ctrl_c: Option<Instant>,
    /// Egress clearance requests waiting for the modal to free up.
    egress_queue: Vec<(u64, String)>,
    status_note: String,
    should_quit: bool,
}

impl App {
    pub fn new(params: AppParams) -> Self {
        Self {
            link: params.link,
            user_echo: params.user_echo,
            busy: false,
            pending_approval: None,
            transcript: vec![],
            streaming: String::new(),
            tools: vec![],
            input: String::new(),
            usage: Usage::default(),
            scroll: None,
            workspace_label: params.workspace_label,
            container_label: params.container_label,
            session_label: params.session_label,
            provider_name: params.provider_name,
            model_name: params.model_name,
            mcp_status: params.mcp_status,
            last_ctrl_c: None,
            egress_queue: vec![],
            status_note: String::new(),
            should_quit: false,
        }
    }

    /// Run until the user quits. Terminal setup/teardown is handled by `ui`.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<InputEvent>();

        // Blocking keyboard reader on its own thread. On Windows crossterm
        // delivers both Press and Release events; only Press is acted on.
        std::thread::spawn(move || {
            use crossterm::event::{Event, KeyEventKind, read};
            loop {
                match read() {
                    Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                        if input_tx.send(InputEvent::Key(key)).is_err() {
                            return;
                        }
                    }
                    Ok(_) => {} // resize/mouse/key-release ignored for v1
                    Err(_) => return,
                }
            }
        });

        let mut terminal = ui::setup().context("failed to enter terminal UI mode")?;

        loop {
            ui::draw(&mut terminal, &self)?;

            tokio::select! {
                event = self.link.next_event() => {
                    match event {
                        Some(event) => self.handle_link(event),
                        None => {
                            // Link closed: nothing left to show.
                            self.status_note = "session link closed".into();
                        }
                    }
                }
                key = input_rx.recv() => {
                    match key {
                        Some(k) => self.handle_key(k).await,
                        None => break,
                    }
                }
            }

            if self.should_quit {
                break;
            }
        }

        ui::restore(&mut terminal)?;
        Ok(())
    }

    // ------------------------------------------------------ link events --

    fn handle_link(&mut self, event: LinkEvent) {
        match event {
            LinkEvent::Core(ev) => self.handle_core(ev),
            LinkEvent::Status(status) => {
                self.busy = status.busy;
                self.provider_name = status.provider;
                self.model_name = status.model;
                if let Some(container) = status.container {
                    self.container_label = container;
                }
                if self.session_label.is_empty() {
                    self.session_label = status.id.chars().take(8).collect();
                }
                self.mcp_status = status.mcp_status;
            }
            LinkEvent::Clearance { id, host } => {
                self.egress_queue.push((id, host));
                self.promote_next_egress();
            }
            LinkEvent::ApprovalDecided { decision, .. } => {
                // Another viewer decided; close our modal if one is open.
                // (If we decided ourselves the modal is already gone.)
                if let Some(modal) = self.pending_approval.take()
                    && modal.kind == ModalKind::Tool
                {
                    self.status_note = match decision {
                        ApprovalDecision::Deny => "approved elsewhere: denied".into(),
                        _ => "approved elsewhere".into(),
                    };
                    self.promote_next_egress();
                }
            }
        }
    }

    fn handle_core(&mut self, ev: CoreEvent) {
        match ev {
            CoreEvent::AssistantDelta { text } => {
                self.streaming.push_str(&text);
            }
            CoreEvent::AssistantMessage { text } => {
                if !self.streaming.is_empty() {
                    self.streaming.clear();
                }
                self.push_assistant(&text);
            }
            CoreEvent::ToolStarted { name, detail, .. } => {
                self.tools.push((name, detail, false));
                self.trim_tools();
            }
            CoreEvent::ToolFinished { ok, summary, .. } => {
                if let Some(entry) = self.tools.iter_mut().find(|(_, _, done)| !*done) {
                    entry.2 = ok;
                }
                if !ok {
                    self.push_line(format!("  ✗ {summary}"));
                }
            }
            CoreEvent::Usage(u) => self.usage = u,
            CoreEvent::Compacted { messages, summary } => {
                self.push_line(format!("[compacted {messages} messages: {summary}]"));
            }
            CoreEvent::Notice { text } => {
                self.push_line(text.clone());
                self.transcript.push(Line::from(""));
            }
            CoreEvent::UserMessage { text } => {
                // Remote mode echoes the prompt; local mode renders its own.
                if self.user_echo {
                    self.push_user(&text);
                }
            }
            CoreEvent::ApprovalRequested { call_id, detail } => {
                // Remote mode: approvals arrive as events. (Local mode also
                // sees them — the gate bridge synthesises them — and both
                // reply through the link.)
                if self.pending_approval.is_none() {
                    self.pending_approval = Some(PendingModal {
                        call_id,
                        request: ApprovalRequest {
                            tool: "shell".into(),
                            detail,
                        },
                        kind: ModalKind::Tool,
                    });
                    self.status_note = "approval required".into();
                }
            }
            CoreEvent::TurnCompleted => {
                self.busy = false;
                self.status_note.clear();
            }
            CoreEvent::TurnCancelled => {
                self.busy = false;
                self.status_note = "turn cancelled".into();
            }
            CoreEvent::Error { message } => {
                self.busy = false;
                self.status_note = message;
            }
        }
    }

    async fn handle_key(&mut self, ev: InputEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};

        let InputEvent::Key(key) = ev;

        // Modal has first claim on the keyboard.
        if self.pending_approval_key(&key).await {
            return;
        }

        match (key.code, key.modifiers) {
            // Ctrl+C: cancel a running turn, quit when idle. A second press
            // within 2s of the first cancels and quits together.
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                if self.busy {
                    self.link.cancel().await;
                    if self
                        .last_ctrl_c
                        .is_some_and(|t| t.elapsed() < Duration::from_secs(2))
                    {
                        self.should_quit = true;
                    } else {
                        self.last_ctrl_c = Some(Instant::now());
                        self.status_note = "cancelling — press Ctrl+C again to quit".into();
                    }
                } else {
                    self.should_quit = true;
                }
            }
            (KeyCode::Esc, _) => {
                if !self.input.is_empty() {
                    self.input.clear();
                } else if !self.busy {
                    self.should_quit = true;
                }
            }
            (KeyCode::Enter, _) => self.submit().await,
            (KeyCode::Backspace, _) => {
                self.input.pop();
            }
            (KeyCode::Char(c), m) => {
                if m.is_empty() || m == KeyModifiers::SHIFT {
                    self.input.push(c);
                }
            }
            (KeyCode::PageUp, _) => self.scroll_up(10),
            (KeyCode::PageDown, _) => self.scroll_down(10),
            _ => {}
        }
    }

    async fn submit(&mut self) {
        let input = self.input.trim().to_string();
        if input.is_empty() {
            return;
        }
        if let Some(rest) = input.strip_prefix('/') {
            self.input.clear();
            self.handle_slash(rest).await;
            return;
        }
        if self.busy {
            return;
        }
        self.input.clear();
        // Remote sessions echo the prompt back as an event; local renders it
        // here (the agent never emits UserMessage in local mode).
        if !self.user_echo {
            self.push_user(&input);
        }

        self.busy = true;
        self.status_note = "thinking...".into();
        if let Err(e) = self.link.send_prompt(input).await {
            self.busy = false;
            self.status_note.clear();
            self.push_line(format!("[error] {e:#}"));
            self.transcript.push(Line::from(""));
        }
    }

    // ---------------------------------------------------- slash commands --

    async fn handle_slash(&mut self, rest: &str) {
        match parse_slash(rest) {
            Err(message) => self.push_line(format!("[error] {message}")),
            Ok(None) => {}
            Ok(Some(Slash::Help)) => {
                for line in [
                    "/model <name>      switch model mid-session",
                    "/provider <name>   switch provider (ollama|openai)",
                    "/diff              show git diff of the workspace",
                    "/sessions          list sessions",
                    "/mcp               list MCP servers and status",
                    "/resume <id>       continue a saved session",
                    "/help              this list",
                    "/quit              exit smith",
                ] {
                    self.push_line(line.to_string());
                }
                self.transcript.push(Line::from(""));
            }
            Ok(Some(Slash::Quit)) => self.should_quit = true,
            Ok(Some(Slash::Model(name))) => {
                let provider = self.provider_name.clone();
                match self.link.set_provider(&provider, &name).await {
                    Ok(line) => {
                        self.model_name = name.clone();
                        self.push_line(line);
                    }
                    Err(e) => self.push_line(format!("[error] {e:#}")),
                }
            }
            Ok(Some(Slash::Provider(name))) => {
                let model = self.model_name.clone();
                match self.link.set_provider(&name, &model).await {
                    Ok(line) => {
                        self.provider_name = name.clone();
                        self.push_line(line);
                    }
                    Err(e) => self.push_line(format!("[error] {e:#}")),
                }
            }
            Ok(Some(Slash::Diff)) => {
                let text = match self.link.diff().await {
                    Ok(text) => text,
                    Err(e) => format!("git diff failed: {e:#}"),
                };
                self.push_line(text);
                self.transcript.push(Line::from(""));
            }
            Ok(Some(Slash::Sessions)) => match self.link.sessions().await {
                Ok(sessions) if sessions.is_empty() => {
                    self.push_line("[no sessions]".into());
                }
                Ok(sessions) => {
                    for s in sessions.iter().take(10) {
                        let busy_marker = if s.busy { " [busy]" } else { "" };
                        match &s.path {
                            Some(path) => self.push_line(format!(
                                "{}  {}:{}  {}{busy_marker} ({})",
                                &s.id[..8.min(s.id.len())],
                                s.provider,
                                s.model,
                                s.workspace,
                                path
                            )),
                            None => self.push_line(format!(
                                "{}  {}:{}  {}{busy_marker}",
                                &s.id[..8.min(s.id.len())],
                                s.provider,
                                s.model,
                                s.workspace
                            )),
                        }
                    }
                    self.transcript.push(Line::from(""));
                }
                Err(e) => self.push_line(format!("[error] {e:#}")),
            },
            Ok(Some(Slash::Mcp)) => {
                let status = self.mcp_status.clone();
                if status.is_empty() {
                    self.push_line("[no MCP servers configured]".into());
                } else {
                    for line in status {
                        self.push_line(line);
                    }
                }
                self.transcript.push(Line::from(""));
            }
            Ok(Some(Slash::Resume(prefix))) => {
                if self.busy {
                    self.push_line("[error] cannot resume while a turn is running".into());
                    return;
                }
                match self.link.resume(&prefix).await {
                    Ok(line) => {
                        self.session_label = prefix;
                        self.push_line(line);
                    }
                    Err(e) => self.push_line(format!("[error] {e:#}")),
                }
            }
        }
    }

    /// Show the next queued egress clearance modal if none is open.
    fn promote_next_egress(&mut self) {
        if self.pending_approval.is_some() {
            return; // modal busy; the queue drains when it frees
        }
        let Some((id, host)) = (!self.egress_queue.is_empty()).then(|| self.egress_queue.remove(0))
        else {
            return;
        };
        self.pending_approval = Some(PendingModal {
            call_id: format!("clearance-{id}"),
            request: ApprovalRequest {
                tool: "egress".into(),
                detail: host.clone(),
            },
            kind: ModalKind::Clearance(id),
        });
        self.status_note = format!("egress clearance: {host}");
    }

    // ----------------------------------------------------------- scrolling --

    fn scroll_up(&mut self, n: u16) {
        let max = self.transcript.len().min(u16::MAX as usize) as u16;
        let current = self.scroll.unwrap_or(max);
        self.scroll = Some(current.saturating_sub(n));
    }

    fn scroll_down(&mut self, n: u16) {
        if let Some(offset) = self.scroll {
            let next = offset.saturating_add(n);
            self.scroll = if self.at_bottom(next) {
                None
            } else {
                Some(next)
            };
        } // else: already following the tail
    }

    fn at_bottom(&self, offset: u16) -> bool {
        // The UI recomputes the exact tail; treat near-max as "following".
        offset >= self.transcript.len().saturating_sub(1) as u16
    }

    // ----------------------------------------------------------- transcript --

    fn push_user(&mut self, text: &str) {
        for line in wrap(format!("❯ {text}")) {
            self.transcript.push(
                Line::from(line)
                    .style(ratatui::style::Style::new().fg(ratatui::style::Color::Cyan)),
            );
        }
        self.transcript.push(Line::from(""));
    }

    fn push_assistant(&mut self, text: &str) {
        for line in wrap(text.to_string()) {
            self.transcript.push(Line::from(line));
        }
        self.transcript.push(Line::from(""));
    }

    fn push_line(&mut self, text: String) {
        self.transcript.push(Line::from(text));
    }

    fn trim_tools(&mut self) {
        while self.tools.len() > 100 {
            self.tools.remove(0);
        }
    }

    // Accessors used by `ui`.
    pub fn transcript(&self) -> &[Line<'static>] {
        &self.transcript
    }

    pub fn streaming(&self) -> &str {
        &self.streaming
    }

    pub fn tools(&self) -> &[(String, String, bool)] {
        &self.tools
    }

    pub fn input(&self) -> &str {
        &self.input
    }

    pub fn busy(&self) -> bool {
        self.busy
    }

    pub fn usage(&self) -> Usage {
        self.usage
    }

    pub fn scroll(&self) -> Option<u16> {
        self.scroll
    }

    /// The modal's request (what the renderer shows), if one is open.
    pub fn pending_approval(&self) -> Option<&ApprovalRequest> {
        self.pending_approval.as_ref().map(|m| &m.request)
    }

    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    pub fn container_label(&self) -> &str {
        &self.container_label
    }

    pub fn session_label(&self) -> &str {
        &self.session_label
    }

    pub fn workspace_label(&self) -> &str {
        &self.workspace_label
    }

    pub fn status_note(&self) -> &str {
        &self.status_note
    }
}

/// Handle a modal key: returns the decision if the modal should close.
fn modal_decision(key: &crossterm::event::KeyEvent) -> Option<ApprovalDecision> {
    use crossterm::event::{KeyCode, KeyModifiers};
    if !key.modifiers.is_empty() && key.modifiers != KeyModifiers::SHIFT {
        return None;
    }
    match key.code {
        KeyCode::Char('y') => Some(ApprovalDecision::AllowOnce),
        KeyCode::Char('a') => Some(ApprovalDecision::AllowAlways),
        KeyCode::Char('n') | KeyCode::Esc => Some(ApprovalDecision::Deny),
        _ => None,
    }
}

impl App {
    /// If the modal is open and the key answers it, reply through the link.
    /// Returns whether the key was consumed by the modal.
    async fn pending_approval_key(&mut self, key: &crossterm::event::KeyEvent) -> bool {
        let Some(modal) = self.pending_approval.take() else {
            return false;
        };
        match modal_decision(key) {
            Some(decision) => {
                match &modal.kind {
                    ModalKind::Tool => {
                        self.link.respond_approval(&modal.call_id, decision).await;
                    }
                    ModalKind::Clearance(id) => {
                        self.link.respond_clearance(*id, decision).await;
                    }
                }
                // The modal freed: surface the next queued egress request.
                self.promote_next_egress();
                true
            }
            None => {
                // Not answered; put it back.
                self.pending_approval = Some(modal);
                false
            }
        }
    }
}

/// Naive greedy word-wrap for transcript lines.
fn wrap(text: String) -> Vec<String> {
    const WIDTH: usize = 96;
    let mut out = vec![];
    for para in text.lines() {
        if para.is_empty() {
            out.push(String::new());
            continue;
        }
        let mut current = String::new();
        for word in para.split_whitespace() {
            if !current.is_empty() && current.len() + 1 + word.len() > WIDTH {
                out.push(std::mem::take(&mut current));
            }
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(word);
        }
        out.push(current);
    }
    out
}

/// A parsed slash command from the input box.
#[derive(Debug, PartialEq, Eq)]
pub enum Slash {
    Model(String),
    Provider(String),
    Diff,
    Sessions,
    Resume(String),
    Mcp,
    Help,
    Quit,
}

/// Parse the text after `/`. Returns `Ok(None)` for empty input.
pub fn parse_slash(rest: &str) -> Result<Option<Slash>, String> {
    let mut parts = rest.split_whitespace();
    let name = parts.next().unwrap_or("");
    let arg = parts.next();
    let tail = arg.map(|a| a.to_string());
    match name {
        "" => Ok(None),
        "model" => tail
            .map(|t| Ok(Some(Slash::Model(t))))
            .unwrap_or(Err("/model requires a model name".into())),
        "provider" => match tail.as_deref() {
            Some("ollama") | Some("openai") => Ok(Some(Slash::Provider(tail.unwrap()))),
            _ => Err("/provider expects `ollama` or `openai`".into()),
        },
        "diff" => Ok(Some(Slash::Diff)),
        "sessions" => Ok(Some(Slash::Sessions)),
        "resume" => tail
            .map(|t| Ok(Some(Slash::Resume(t))))
            .unwrap_or(Err("/resume requires a session id (see /sessions)".into())),
        "help" => Ok(Some(Slash::Help)),
        "quit" => Ok(Some(Slash::Quit)),
        other => Err(format!("unknown command `/{other}` — try /help")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_commands() {
        assert_eq!(
            parse_slash("model qwen3:0.6b").unwrap(),
            Some(Slash::Model("qwen3:0.6b".into()))
        );
        assert_eq!(
            parse_slash("provider openai").unwrap(),
            Some(Slash::Provider("openai".into()))
        );
        assert_eq!(parse_slash("diff").unwrap(), Some(Slash::Diff));
        assert_eq!(parse_slash("sessions").unwrap(), Some(Slash::Sessions));
        assert_eq!(
            parse_slash("resume abc123").unwrap(),
            Some(Slash::Resume("abc123".into()))
        );
        assert_eq!(parse_slash("help").unwrap(), Some(Slash::Help));
        assert_eq!(parse_slash("quit").unwrap(), Some(Slash::Quit));
        assert_eq!(parse_slash("").unwrap(), None);
    }

    #[test]
    fn rejects_malformed_commands() {
        assert!(parse_slash("model").is_err());
        assert!(parse_slash("provider shazam").is_err());
        assert!(parse_slash("resume").is_err());
        assert!(parse_slash("frobnicate").is_err());
    }
}
