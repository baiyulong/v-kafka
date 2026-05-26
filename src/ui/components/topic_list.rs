use crate::app::App;
use crate::ui::theme::Theme;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    render_list(f, chunks[0], app);
    render_detail(f, chunks[1], app);
}

fn render_list(f: &mut Frame, area: Rect, app: &App) {
    let topics = app.filtered_topics();
    let total = app.metadata.topics.len();

    let items: Vec<ListItem> = topics
        .iter()
        .map(|t| {
            let part_str = format!("{}", t.partition_count());
            let repl_str = format!("{}", t.replication_factor());
            let line = Line::from(vec![
                Span::styled(if t.is_internal { "  𝑖 " } else { "  ▤ " }, Theme::dim()),
                Span::styled(
                    t.name.clone(),
                    if t.is_internal {
                        Theme::dim()
                    } else {
                        Theme::normal()
                    },
                ),
                Span::raw("  "),
                Span::styled(format!("P:{}", part_str), Theme::key()),
                Span::raw(" "),
                Span::styled(format!("R:{}", repl_str), Theme::dim()),
            ]);
            ListItem::new(line)
        })
        .collect();

    let filter_hint = if !app.filter.is_empty() {
        format!(" filter:'{}' ({}/{}) ", app.filter, items.len(), total)
    } else {
        format!(" {} topics ", total)
    };

    let loading_indicator = if app.loading { " ⟳" } else { "" };

    let list = List::new(if items.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            if app.loading {
                "  Loading…"
            } else {
                "  No topics found"
            },
            Theme::dim(),
        )))]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!(
                " Topics{}{} [n]ew [d]del [/]filter [b]rokers [g]roups ",
                filter_hint, loading_indicator
            ))
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    )
    .highlight_style(Theme::selected())
    .highlight_symbol("▶ ");

    let mut state = ListState::default();
    if !topics.is_empty() {
        state.select(Some(app.list_cursor.min(topics.len().saturating_sub(1))));
    }
    f.render_stateful_widget(list, area, &mut state);
}

fn render_detail(f: &mut Frame, area: Rect, app: &App) {
    let topics = app.filtered_topics();
    let topic = topics.get(app.list_cursor.min(topics.len().saturating_sub(1)));

    let content = match topic {
        None => vec![
            Line::from(""),
            Line::from(Span::styled("  No topic selected", Theme::dim())),
        ],
        Some(t) => {
            let mut lines = vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Name            ", Theme::key()),
                    Span::raw(t.name.as_str()),
                ]),
                Line::from(vec![
                    Span::styled("  Partitions      ", Theme::key()),
                    Span::raw(t.partition_count().to_string()),
                ]),
                Line::from(vec![
                    Span::styled("  Replication     ", Theme::key()),
                    Span::raw(t.replication_factor().to_string()),
                ]),
                Line::from(vec![
                    Span::styled("  Internal        ", Theme::key()),
                    Span::styled(
                        if t.is_internal { "yes" } else { "no" },
                        if t.is_internal {
                            Theme::dim()
                        } else {
                            Theme::normal()
                        },
                    ),
                ]),
                Line::from(""),
                Line::from(Span::styled("  Partitions:", Theme::key())),
            ];

            for p in t.partitions.iter().take(10) {
                let isr_str = p
                    .isr
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let err = p.error.as_deref().unwrap_or("");
                lines.push(Line::from(vec![
                    Span::styled(format!("    {:>3} ", p.id), Theme::dim()),
                    Span::styled(format!("leader:{:<3} ", p.leader), Theme::normal()),
                    Span::styled(format!("isr:[{}]", isr_str), Theme::success()),
                    if !err.is_empty() {
                        Span::styled(format!(" ⚠{}", err), Theme::error())
                    } else {
                        Span::raw("")
                    },
                ]));
            }
            if t.partitions.len() > 10 {
                lines.push(Line::from(Span::styled(
                    format!("    … {} more", t.partitions.len() - 10),
                    Theme::dim(),
                )));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "  [Enter] view partitions",
                Theme::dim(),
            )));
            lines
        }
    };

    let para = Paragraph::new(content).block(
        Block::default()
            .title(" Topic Detail ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block()),
    );
    f.render_widget(para, area);
}
