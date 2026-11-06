//! TUI application state and the main event loop.

use std::sync::Arc;

use anyhow::Context;
use ratatui::text::Line;
use smith_core::{Agent, ApprovalDecision, ApprovalRequest, CoreEvent, Message, Usage};
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

pub struct AppParams {
    pub agent: Arc<Agent>,
    pub history: Vec<Message>,
    pub gate_rx: ApprovalRx,
    pub model_label: String,
    pub workspace_label: String,
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
    model_label: String,
    workspace_label: String,
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
            model_label: params.model_label,
            workspace_label: params.workspace_label,
            status_note: String::new(),
            should_quit: false,
        }
    }

    /// Run until the user quits. Terminal setup/teardown is handled by `ui`.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<InputEvent>();

        // Blocking keyboard reader on its own thread.
        std::thread::spawn(move || {
            use crossterm::event::{Event, read};
            loop {
                match read() {
                    Ok(Event::Key(key)) => {
                        if input_tx.send(InputEvent::Key(key)).is_err() {
                            return;
                        }
                    }
                    Ok(_) => {} // resize/mouse ignored for v1
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
                        Some(k) => self.handle_key(k),
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

    fn handle_key(&mut self, ev: InputEvent) {
        use crossterm::event::{KeyCode, KeyModifiers};

        let InputEvent::Key(key) = ev;

        // Modal has first claim on the keyboard.
        if self.pending_approval_key(&key) {
            return;
        }

        match (key.code, key.modifiers) {
            // Ctrl+C: cancel a running turn, quit when idle.
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                if self.busy {
                    if let Turn::Running { cancel, .. } = &self.turn {
                        cancel.cancel();
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
            (KeyCode::Enter, _) => self.submit(),
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

    fn submit(&mut self) {
        let input = self.input.trim().to_string();
        if input.is_empty() || self.busy {
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

    pub fn model_label(&self) -> &str {
        &self.model_label
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
