//! TUI application state and the main event loop.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use ratatui::text::Line;
use smith_core::{
    Agent, ApprovalDecision, ApprovalRequest, CoreEvent, LlmProvider, Message, Usage,
    list_sessions, load_history, workspace_identity,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::gate::ApprovalRx;
use crate::ui;

/// Key events from the reader thread.
#[derive(Debug)]
pub enum InputEvent {
    Key(crossterm::event::KeyEvent),
}

enum Turn {
    /// A turn is running.
    Running {
        handle: JoinHandle<Vec<Message>>,
        cancel: CancellationToken,
    },
    Idle,
}

/// Builds a provider for `/model` and `/provider` switches.
pub type ProviderFactory =
    Arc<dyn Fn(&str, &str) -> anyhow::Result<Arc<dyn LlmProvider>> + Send + Sync>;

pub struct AppParams {
    pub agent: Arc<Agent>,
    pub history: Vec<Message>,
    pub gate_rx: ApprovalRx,
    /// Canonical workspace path (for `/resume` identity checks).
    pub workspace: PathBuf,
    pub workspace_label: String,
    pub container_label: String,
    pub session_label: String,
    pub provider_name: String,
    pub model_name: String,
    pub build_provider: ProviderFactory,
    /// Snapshot of MCP server status taken at session start.
    pub mcp_status: Vec<String>,
}

pub struct App {
    agent: Arc<Agent>,
    history: Vec<Message>,
    gate_rx: ApprovalRx,
    event_tx: mpsc::UnboundedSender<CoreEvent>,
    event_rx: mpsc::UnboundedReceiver<CoreEvent>,

    busy: bool,
    turn: Turn,
    pending_approval: Option<(ApprovalRequest, mpsc::UnboundedSender<ApprovalDecision>)>,

    /// Rendered transcript lines (already wrapped at draw time via `ui`).
    transcript: Vec<Line<'static>>,
    /// Assistant text still streaming in.
    streaming: String,
    tools: Vec<(String, String, bool)>, // name, detail, finished_ok
    input: String,
    usage: Usage,
    /// `None` = follow the tail; `Some(offset)` = user scrolled up.
    scroll: Option<u16>,
    workspace: PathBuf,
    workspace_label: String,
    container_label: String,
    session_label: String,
    provider_name: String,
    model_name: String,
    build_provider: ProviderFactory,
    mcp_status: Vec<String>,
    /// Last Ctrl+C while busy (quit-with-second-press flow).
    last_ctrl_c: Option<Instant>,
    status_note: String,
    should_quit: bool,
}

impl App {
    pub fn new(params: AppParams) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Self {
            agent: params.agent,
            history: params.history,
            gate_rx: params.gate_rx,
            event_tx,
            event_rx,
            busy: false,
            turn: Turn::Idle,
            pending_approval: None,
            transcript: vec![],
            streaming: String::new(),
            tools: vec![],
            input: String::new(),
            usage: Usage::default(),
            scroll: None,
            workspace: params.workspace,
            workspace_label: params.workspace_label,
            container_label: params.container_label,
            session_label: params.session_label,
            provider_name: params.provider_name,
            model_name: params.model_name,
            build_provider: params.build_provider,
            mcp_status: params.mcp_status,
            last_ctrl_c: None,
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

            // Drain any queued events without blocking the draw loop.
            tokio::select! {
                ev = self.event_rx.recv() => {
                    match ev {
                        Some(ev) => self.handle_core(ev),
                        None => break,
                    }
                }
                some = self.gate_rx.recv() => {
                    if let Some((request, reply)) = some {
                        self.pending_approval = Some((request, reply));
                    }
                }
                key = input_rx.recv() => {
                    match key {
                        Some(k) => self.handle_key(k).await,
                        None => break,
                    }
                }
            }

            // Reap a finished turn and take history back.
            if let Turn::Running { handle, .. } = &mut self.turn
                && handle.is_finished()
            {
                let Turn::Running { handle, .. } = std::mem::replace(&mut self.turn, Turn::Idle)
                else {
                    unreachable!()
                };
                match handle.await {
                    Ok(history) => self.history = history,
                    Err(e) => {
                        self.status_note = format!("turn failed: {e}");
                    }
                }
                self.busy = false;
            }

            if self.should_quit {
                break;
            }
        }

        ui::restore(&mut terminal)?;
        Ok(())
    }

    // ------------------------------------------------------ event handling --

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
            CoreEvent::TurnCompleted => {
                self.status_note.clear();
            }
            CoreEvent::TurnCancelled => {
                self.status_note = "turn cancelled".into();
            }
            CoreEvent::Error { message } => {
                self.status_note = message;
            }
            CoreEvent::ApprovalRequested { .. } => {}
        }
    }

    async fn handle_key(&mut self, ev: InputEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};

        let InputEvent::Key(key) = ev;

        // Modal has first claim on the keyboard.
        if self.pending_approval_key(&key) {
            return;
        }

        match (key.code, key.modifiers) {
            // Ctrl+C: cancel a running turn, quit when idle. A second press
            // within 2s of the first cancels and quits together.
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                if self.busy {
                    if let Turn::Running { cancel, .. } = &self.turn {
                        cancel.cancel();
                    }
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
        self.push_user(&input);

        let agent = Arc::clone(&self.agent);
        let mut history = std::mem::take(&mut self.history);
        let tx = self.event_tx.clone();
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        self.busy = true;
        self.status_note = "thinking...".into();
        let handle = tokio::spawn(async move {
            let result = agent
                .run_turn(&mut history, &input, tx, cancel_for_task)
                .await;
            match result {
                Ok(()) => history,
                Err(e) => {
                    // Surface the failure; keep whatever history we have.
                    let _ = e;
                    history
                }
            }
        });
        self.turn = Turn::Running { handle, cancel };
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
                    "/sessions          list saved sessions",
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
                match (self.build_provider)(&self.provider_name, &name) {
                    Ok(provider) => {
                        self.agent.set_provider(provider);
                        self.model_name = name.clone();
                        self.push_line(format!("[model switched to {name}]"));
                    }
                    Err(e) => self.push_line(format!("[error] {e:#}")),
                }
            }
            Ok(Some(Slash::Provider(name))) => {
                match (self.build_provider)(&name, &self.model_name) {
                    Ok(provider) => {
                        self.agent.set_provider(provider);
                        self.provider_name = name.clone();
                        self.push_line(format!("[provider switched to {name}]"));
                    }
                    Err(e) => self.push_line(format!("[error] {e:#}")),
                }
            }
            Ok(Some(Slash::Diff)) => {
                let agent = Arc::clone(&self.agent);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    let exec = agent.executor();
                    let text = match exec
                        .exec_shell("git --no-pager diff", Duration::from_secs(30))
                        .await
                    {
                        Ok(res) if res.stdout.trim().is_empty() && res.stderr.trim().is_empty() => {
                            "git diff: (no changes)".to_string()
                        }
                        Ok(res) => {
                            let out = format!("{}{}", res.stdout, res.stderr);
                            let chars: Vec<char> = out.chars().collect();
                            let start = chars.len().saturating_sub(4000);
                            format!("git diff:\n{}", chars[start..].iter().collect::<String>())
                        }
                        Err(e) => {
                            format!("git diff failed: {e:#} (git may be missing in the base image)")
                        }
                    };
                    let _ = tx.send(CoreEvent::Notice { text });
                });
            }
            Ok(Some(Slash::Sessions)) => match list_sessions() {
                Ok(sessions) if sessions.is_empty() => self.push_line("[no saved sessions]".into()),
                Ok(sessions) => {
                    for (path, meta) in sessions.iter().take(10) {
                        self.push_line(format!(
                            "{}  {}:{}  {} ({})",
                            &meta.id[..8.min(meta.id.len())],
                            meta.provider,
                            meta.model,
                            meta.workspace_name,
                            path.display()
                        ));
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
                let loaded = self.load_session_for_resume(&prefix).await;
                match loaded {
                    Ok((id_prefix, count)) => {
                        self.push_line(format!("[resumed session {id_prefix}: {count} messages]"));
                        self.session_label = id_prefix;
                    }
                    Err(e) => self.push_line(format!("[error] {e:#}")),
                }
            }
        }
    }

    /// Load a session's history into the app (used by /resume).
    async fn load_session_for_resume(&mut self, prefix: &str) -> anyhow::Result<(String, usize)> {
        let sessions = list_sessions()?;
        let matches: Vec<_> = sessions
            .iter()
            .filter(|(_, meta)| meta.id.starts_with(prefix))
            .collect();
        match matches.len() {
            0 => anyhow::bail!("no session matches `{prefix}` (see /sessions)"),
            n if n > 1 => anyhow::bail!("{n} sessions match `{prefix}`; be more specific"),
            _ => {}
        }
        let (path, meta) = matches[0].clone();

        let current = workspace_identity(&self.workspace);
        if meta.workspace_id != current {
            anyhow::bail!(
                "session {} belongs to workspace `{}`, not this one",
                &meta.id[..8.min(meta.id.len())],
                meta.workspace_name
            );
        }

        let (_, mut history) = load_history(&path)?;
        if !history
            .first()
            .is_some_and(|m| m.role == smith_core::Role::System)
        {
            history.insert(0, Message::system(self.agent.system_prompt().await));
        }
        let count = history.len();
        self.history = history;
        Ok((meta.id[..8.min(meta.id.len())].to_string(), count))
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

    pub fn pending_approval(
        &self,
    ) -> Option<&(ApprovalRequest, mpsc::UnboundedSender<ApprovalDecision>)> {
        self.pending_approval.as_ref()
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
    /// If the modal is open and the key answers it, reply with the decision.
    /// Returns whether the key was consumed by the modal.
    fn pending_approval_key(&mut self, key: &crossterm::event::KeyEvent) -> bool {
        let Some((request, reply)) = self.pending_approval.take() else {
            return false;
        };
        match modal_decision(key) {
            Some(decision) => {
                let _ = reply.send(decision);
                true
            }
            None => {
                // Not answered; put it back.
                self.pending_approval = Some((request, reply));
                false
            }
        }
    }
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
        "mcp" => Ok(Some(Slash::Mcp)),
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
