use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    widgets::{Block, BorderType, Borders, List, ListItem, ListState, Paragraph, Wrap},
    text::{Line, Span},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);

    render_subjects_panel(f, chunks[0], app);
    render_detail_panel(f, chunks[1], app);
}

fn render_subjects_panel(f: &mut Frame, area: Rect, app: &App) {
    let title = format!(" Subjects ({}) ", app.schema_subjects.len());
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Theme::block_active());

    if app.schema_subjects_loading {
        let p = Paragraph::new(Line::from(Span::styled("  Loading subjects…", Theme::dim())))
            .block(block);
        f.render_widget(p, area);
        return;
    }

    if app.schema_subjects.is_empty() {
        let lines = vec![
            Line::from(Span::styled("  No subjects found", Theme::dim())),
            Line::from(""),
            Line::from(Span::styled("  Press [r] to refresh", Theme::dim())),
            Line::from(Span::styled("  Schema Registry must be", Theme::dim())),
            Line::from(Span::styled("  configured for this cluster.", Theme::dim())),
        ];
        let p = Paragraph::new(lines).block(block);
        f.render_widget(p, area);
        return;
    }

    let items: Vec<ListItem> = app.schema_subjects.iter().map(|s| {
        ListItem::new(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::raw(s.clone()),
        ]))
    }).collect();

    let mut state = ListState::default();
    state.select(Some(app.schema_subjects_cursor));

    let list = List::new(items)
        .block(block)
        .highlight_style(Theme::list_selected())
        .highlight_symbol("▶ ");
    f.render_stateful_widget(list, area, &mut state);
}

fn render_detail_panel(f: &mut Frame, area: Rect, app: &App) {
    let subject_name = app.schema_subjects
        .get(app.schema_subjects_cursor)
        .cloned()
        .unwrap_or_default();
    let title = if subject_name.is_empty() {
        " Schema Detail ".to_string()
    } else {
        format!(" {} ", subject_name)
    };

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Theme::block_inactive());

    if app.schema_detail_loading {
        let p = Paragraph::new(Span::styled("  Loading schema…", Theme::dim())).block(block);
        f.render_widget(p, area);
        return;
    }

    match &app.schema_detail {
        None => {
            let lines = vec![
                Line::from(""),
                Line::from(Span::styled("  Select a subject to view its schema.", Theme::dim())),
                Line::from(""),
                Line::from(Span::styled("  Keys:", Theme::key_hint())),
                Line::from(Span::styled("    Enter  View latest schema", Theme::dim())),
                Line::from(Span::styled("    r      Refresh subjects", Theme::dim())),
                Line::from(Span::styled("    Esc    Go back", Theme::dim())),
            ];
            let p = Paragraph::new(lines).block(block);
            f.render_widget(p, area);
        }
        Some(detail) => {
            let mut lines = vec![
                Line::from(vec![
                    Span::styled("Subject: ", Theme::key_hint()),
                    Span::raw(&detail.subject),
                ]),
                Line::from(vec![
                    Span::styled("Version: ", Theme::key_hint()),
                    Span::raw(detail.version.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("Schema ID: ", Theme::key_hint()),
                    Span::raw(detail.id.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("Type: ", Theme::key_hint()),
                    Span::raw(&detail.schema_type),
                ]),
                Line::from(""),
                Line::from(Span::styled("─── Schema ───", Theme::dim())),
                Line::from(""),
            ];
            // Pretty-print the schema JSON
            let schema_pretty = serde_json::from_str::<serde_json::Value>(&detail.schema)
                .ok()
                .and_then(|v| serde_json::to_string_pretty(&v).ok())
                .unwrap_or_else(|| detail.schema.clone());
            for line in schema_pretty.lines() {
                lines.push(Line::from(Span::raw(line.to_string())));
            }
            let p = Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: false })
                .scroll((app.scroll_offset as u16, 0));
            f.render_widget(p, area);
        }
    }
}

pub fn render_detail(f: &mut Frame, area: Rect, app: &App) {
    render(f, area, app);
}

