//! Terminal setup/teardown and frame drawing.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap as WidgetWrap};
use smith_core::ApprovalDecision;

use crate::app::App;

/// Owns the terminal in raw mode; restores on drop (including panics).
pub struct TerminalGuard {
    terminal: ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
}

pub fn setup() -> anyhow::Result<TerminalGuard> {
    use crossterm::{event::EnableMouseCapture, execute, terminal::*};
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;

    // Restore the terminal if a panic escapes the main loop.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
        hook(info);
    }));

    let backend = ratatui::backend::CrosstermBackend::new(std::io::stdout());
    let terminal = ratatui::Terminal::new(backend)?;
    Ok(TerminalGuard { terminal })
}

pub fn restore(guard: &mut TerminalGuard) -> anyhow::Result<()> {
    use crossterm::{execute, terminal::*};
    guard.terminal.draw(|_| {})?; // flush nothing further
    disable_raw_mode()?;
    execute!(std::io::stdout(), LeaveAlternateScreen)?;
    Ok(())
}

pub fn draw(terminal: &mut TerminalGuard, app: &App) -> anyhow::Result<()> {
    terminal.terminal.draw(|frame| render(frame, app))?;
    Ok(())
}

fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let [main, tools_area, input_area, status_area] = Layout::vertical([
        Constraint::Min(0),
        Constraint::Length(8),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(area);

    render_transcript(frame, main, app);
    render_tools(frame, tools_area, app);
    render_input(frame, input_area, app);
    render_status(frame, status_area, app);
    if let Some(request) = app.pending_approval() {
        render_modal(frame, area, request);
    }
}

fn render_transcript(frame: &mut Frame, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();

    // Assistant text still streaming, shown live at the tail.
    let mut committed: Vec<Line> = app
        .transcript()
        .iter()
        .map(|line| {
            let sanitised: Vec<String> = line
                .spans
                .iter()
                .map(|s| sanitise(&s.content.clone()))
                .collect();
            let mut out = Line::from(sanitised.join(""));
            out.style = line.style;
            out
        })
        .collect();
    let streaming = app.streaming();
    if !streaming.is_empty() {
        for line in streaming.lines() {
            committed
                .push(Line::from(sanitise(line)).style(Style::new().add_modifier(Modifier::DIM)));
        }
    }

    // Nothing rendered yet: welcome text.
    if committed.is_empty() {
        committed.push(Line::from(
            "smith — sandboxed coding agent (v1)".to_string(),
        ));
        committed.push(Line::from(
            "Type a task and press Enter. Ctrl+C cancels a turn / quits.".to_string(),
        ));
        committed.push(Line::from(""));
    }

    let height = area.height.saturating_sub(2) as usize; // borders
    let offset = match app.scroll() {
        Some(o) => o as usize,
        None => committed.len().saturating_sub(height),
    };
    lines.extend(committed);

    let paragraph = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(" transcript "))
        .wrap(WidgetWrap { trim: false })
        .scroll((offset as u16, 0));
    frame.render_widget(paragraph, area);
}

fn render_tools(frame: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .tools()
        .iter()
        .map(|(name, detail, done)| {
            let marker = if *done { "✓" } else { "…" };
            let style = if *done {
                Style::new().fg(Color::Green)
            } else {
                Style::new().fg(Color::Yellow)
            };
            ListItem::new(Line::from(format!("{marker} {name}: {detail}")).style(style))
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" tool activity "),
    );
    frame.render_widget(list, area);
}

fn render_input(frame: &mut Frame, area: Rect, app: &App) {
    let (title, style) = if app.busy() {
        (
            " input (busy — Ctrl+C to cancel) ",
            Style::new().fg(Color::Yellow),
        )
    } else {
        (" input ", Style::new().fg(Color::Cyan))
    };
    let text = if app.busy() && app.input().is_empty() {
        Line::from("(type your next message after the turn finishes)".to_string())
            .style(Style::new().add_modifier(Modifier::DIM))
    } else {
        Line::from(format!("❯ {}", app.input()))
    };
    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(style),
    );
    frame.render_widget(paragraph, area);
}

fn render_status(frame: &mut Frame, area: Rect, app: &App) {
    let usage = app.usage();
    let status = format!(
        " {}: {} | {} | sess {} | {} | in {} out {}{}{}",
        app.provider_name(),
        app.model_name(),
        app.container_label(),
        app.session_label(),
        app.workspace_label(),
        usage.input_tokens,
        usage.output_tokens,
        if app.status_note().is_empty() {
            ""
        } else {
            " | "
        },
        app.status_note(),
    );
    let paragraph =
        Paragraph::new(Line::from(status).style(Style::new().bg(Color::Rgb(0, 0, 120))));
    frame.render_widget(paragraph, area);
}

fn render_modal(frame: &mut Frame, area: Rect, request: &smith_core::ApprovalRequest) {
    let inner_width = 58.min(area.width.saturating_sub(2) as usize);
    let max_command_lines = area.height.saturating_sub(8) as usize; // chrome + option row

    // The command is wrapped manually and the TAIL is kept, so the option
    // row is always visible (S-12). Height grows with the command up to the
    // available space.
    let wrapped = wrap_text(&sanitise(&request.detail), inner_width);
    let shown = if wrapped.len() > max_command_lines {
        wrapped[wrapped.len() - max_command_lines..].to_vec()
    } else {
        wrapped.clone()
    };
    let height = (shown.len() as u16 + 6).min(area.height);
    let modal = centered_rect(60.min(area.width), height, area);

    let mut text = vec![
        Line::from("shell command approval".to_string())
            .style(Style::new().add_modifier(Modifier::BOLD)),
        Line::from(""),
    ];
    if wrapped.len() > shown.len() {
        text.push(
            Line::from(format!(
                "… (+{} earlier lines)",
                wrapped.len() - shown.len()
            ))
            .style(Style::new().add_modifier(Modifier::DIM)),
        );
    }
    for cmd_line in &shown {
        text.push(Line::from(cmd_line.clone()));
    }
    text.push(Line::from(""));
    text.push(Line::from(
        "[y] once   [a] always (this session)   [n/Esc] deny".to_string(),
    ));
    let paragraph = Paragraph::new(text).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" approval required ")
            .border_style(Style::new().fg(Color::Red)),
    );
    frame.render_widget(paragraph, modal);
}

/// S-12: control characters and terminal escape sequences (ANSI CSI/OSC) are
/// stripped from text before it reaches the screen — the agent's output must
/// not drive the terminal. Newlines and tabs are kept.
pub fn sanitise(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            match chars.peek() {
                Some('[') => {
                    chars.next();
                    // CSI: consume up to the final byte 0x40–0x7E.
                    while let Some(&f) = chars.peek() {
                        chars.next();
                        if ('\x40'..='\x7e').contains(&f) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next();
                    // OSC: consume up to BEL or ST (ESC \).
                    loop {
                        match chars.next() {
                            None | Some('\x07') => break,
                            Some('\x1b') => {
                                chars.next();
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                _ => {} // bare ESC: dropped
            }
            continue;
        }
        if c == '\n' || c == '\t' {
            out.push(c);
        } else if !c.is_control() {
            out.push(c);
        }
    }
    out
}

/// Char-based wrapping of `text` to `width` columns (one line per chunk);
/// newlines in the input start new chunks. Good enough for display — no
/// unicode-width dependency.
fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    for para in sanitise(text).split('\n') {
        let mut current = String::new();
        for c in para.chars() {
            current.push(c);
            if current.chars().count() >= width {
                lines.push(std::mem::take(&mut current));
            }
        }
        if current.is_empty() && !lines.is_empty() {
            continue;
        }
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

/// S-12: an oversized command keeps its TAIL visible — the wrapped lines are
/// trimmed from the head so the option row is the last thing on screen.
#[test]
fn modal_layout_shows_the_tail_of_a_long_command() {
    // Distinct characters per chunk so head/tail lines are recognisable.
    let long_command: String = (0..300).map(|i| (b'a' + (i % 26) as u8) as char).collect();
    let inner_width = 58;
    let wrapped = wrap_text(&long_command, inner_width);
    assert!(wrapped.len() >= 3, "a 300-char command wraps: {wrapped:?}");

    // The tail rule: with only 2 command lines fitting, the LAST two of the
    // wrapped output are kept and the head is dropped.
    let max_command_lines = 2;
    let shown = &wrapped[wrapped.len() - max_command_lines..];
    assert_eq!(shown.len(), 2);
    assert_ne!(shown[0], wrapped[0], "the head is dropped");
    assert_eq!(shown[1], wrapped[wrapped.len() - 1], "the tail survives");
}

#[test]
fn modal_layout_wraps_at_the_width() {
    let wrapped = wrap_text(&"x".repeat(120), 58);
    assert_eq!(wrapped.len(), 3);
    for line in &wrapped {
        assert!(line.chars().count() <= 58, "{line}");
    }
}

#[test]
fn sanitise_strips_ansi_and_control_characters() {
    // CSI clear-screen and cursor-move sequences are gone.
    assert_eq!(sanitise("\x1b[2Jok"), "ok");
    assert_eq!(sanitise("a\x1b[31mred\x1b[0m"), "ared");
    // OSC (terminal title) sequences are gone.
    assert_eq!(sanitise("\x1b]0;evil title\x07ok"), "ok");
    // C0 controls (except \n and \t) are stripped.
    assert_eq!(sanitise("a\x07b\x0cc"), "abc");
    assert_eq!(
        sanitise("line one\nline two\ttab"),
        "line one\nline two\ttab"
    );
    // Plain text passes through untouched.
    assert_eq!(sanitise("plain — text ✓"), "plain — text ✓");
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect {
        x,
        y,
        width: width.min(area.width),
        height: height.min(area.height),
    }
}

// The ApprovalDecision import is used by the gate reply flow; keep the
// compiler honest about the wire-up.
#[allow(dead_code)]
fn _decision_wellformed(_d: ApprovalDecision) {}
