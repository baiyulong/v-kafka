use crate::app::App;
use crate::ui::theme::Theme;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    // ── Left: group list ──────────────────────────────────────────────────────
    let groups = &app.consumer_groups;
    let items: Vec<ListItem> = if groups.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            if app.consumer_groups_loading {
                "  Loading…"
            } else {
                "  No groups found"
            },
            Theme::dim(),
        )))]
    } else {
        groups
            .iter()
            .map(|g| {
                let state_style = match g.state.as_str() {
                    "Stable" => Theme::success(),
                    "Empty" => Theme::dim(),
                    _ => Theme::warning(),
                };
                ListItem::new(Line::from(vec![
                    Span::styled(format!("  {:<35}", g.group_id), Theme::normal()),
                    Span::styled(format!("{:<10}", g.state), state_style),
                    Span::styled(format!("{}m", g.members), Theme::dim()),
                ]))
            })
            .collect()
    };

    let list = List::new(items)
        .block(
            Block::default()
                .title(" Consumer Groups  [r]efresh ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    state.select(if groups.is_empty() {
        None
    } else {
        Some(app.list_cursor.min(groups.len().saturating_sub(1)))
    });
    f.render_stateful_widget(list, chunks[0], &mut state);

    // ── Right: selected group detail ──────────────────────────────────────────
    let selected = groups.get(app.list_cursor);
    let title = selected
        .map(|g| format!(" {} ─ {} ", g.group_id, g.state))
        .unwrap_or_else(|| " Group Detail ".to_string());

    let mut lines: Vec<Line> = Vec::new();
    if let Some(g) = selected {
        lines.push(Line::from(vec![
            Span::styled("  Protocol: ", Theme::key()),
            Span::styled(g.protocol.clone(), Theme::normal()),
            Span::styled("   Members: ", Theme::key()),
            Span::styled(g.members.to_string(), Theme::normal()),
        ]));
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            format!(
                "  {:<35} {:>12}  {:>12}  {:>8}",
                "TOPIC/PARTITION", "COMMITTED", "HIGH", "LAG"
            ),
            Theme::dim(),
        )]));
        lines.push(Line::from(Span::styled(
            "  ".to_string() + &"─".repeat(74),
            Theme::dim(),
        )));
        lines.push(Line::from(Span::styled(
            "  Press [Enter] to load offsets",
            Theme::dim(),
        )));
    } else {
        lines.push(Line::from(Span::styled(
            "  Select a group and press [Enter]",
            Theme::dim(),
        )));
    }

    let detail = Paragraph::new(lines).block(
        Block::default()
            .title(title.as_str())
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block()),
    );
    f.render_widget(detail, chunks[1]);
}

pub fn render_detail(f: &mut Frame, area: Rect, app: &App) {
    let group_id = app.selected_group.as_deref().unwrap_or("?");
    let offsets = &app.consumer_group_offsets;

    let header = Line::from(vec![Span::styled(
        format!(
            "  {:<40} {:>12}  {:>12}  {:>8}",
            "TOPIC/PARTITION", "COMMITTED", "HIGH", "LAG"
        ),
        ratatui::style::Style::default()
            .add_modifier(Modifier::BOLD)
            .fg(ratatui::style::Color::Cyan),
    )]);

    let mut lines: Vec<Line> = vec![
        header,
        Line::from(Span::styled(
            "  ".to_string() + &"─".repeat(80),
            Theme::dim(),
        )),
    ];

    if offsets.is_empty() {
        lines.push(Line::from(Span::styled(
            if app.group_offsets_loading {
                "  Loading…"
            } else {
                "  No committed offsets"
            },
            Theme::dim(),
        )));
    } else {
        let total_lag: i64 = offsets.iter().map(|o| o.lag()).sum();
        for o in offsets {
            let tp = format!("{}/{}", o.topic, o.partition);
            let committed = if o.committed_offset < 0 {
                "-".to_string()
            } else {
                o.committed_offset.to_string()
            };
            let lag_style = if o.lag() == 0 {
                Theme::success()
            } else if o.lag() > 1000 {
                Theme::error()
            } else {
                Theme::warning()
            };
            lines.push(Line::from(vec![
                Span::styled(format!("  {:<40}", tp), Theme::normal()),
                Span::styled(format!("{:>12}  ", committed), Theme::dim()),
                Span::styled(format!("{:>12}  ", o.high_watermark), Theme::dim()),
                Span::styled(format!("{:>8}", o.lag()), lag_style),
            ]));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("  Total lag: ", Theme::key()),
            Span::styled(
                total_lag.to_string(),
                if total_lag == 0 {
                    Theme::success()
                } else {
                    Theme::warning()
                },
            ),
        ]));
    }

    let title = format!(
        " Group: {} ─ [R]eset earliest  [L]atest  [Esc] back ",
        group_id
    );
    let para = Paragraph::new(lines).block(
        Block::default()
            .title(title.as_str())
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    );
    f.render_widget(para, area);
}
