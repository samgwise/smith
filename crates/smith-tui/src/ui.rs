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
    if let Some((request, _)) = app.pending_approval() {
        render_modal(frame, area, request);
    }
}

fn render_transcript(frame: &mut Frame, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();

    // Assistant text still streaming, shown live at the tail.
    let mut committed: Vec<Line> = app.transcript().to_vec();
    let streaming = app.streaming();
    if !streaming.is_empty() {
        for line in streaming.lines() {
            committed
                .push(Line::from(line.to_string()).style(Style::new().add_modifier(Modifier::DIM)));
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
        " {} | {} | in {} out {}{}{}",
        app.model_label(),
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
    let width = 60.min(area.width);
    let height = 7;
    let modal = centered_rect(width, height, area);

    let text = vec![
        Line::from("shell command approval".to_string())
            .style(Style::new().add_modifier(Modifier::BOLD)),
        Line::from(""),
        Line::from(request.detail.clone()),
        Line::from(""),
        Line::from("[y] once   [a] always (this session)   [n/Esc] deny".to_string()),
    ];
    let paragraph = Paragraph::new(text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" approval required ")
                .border_style(Style::new().fg(Color::Red)),
        )
        .wrap(WidgetWrap { trim: false });
    frame.render_widget(paragraph, modal);
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
