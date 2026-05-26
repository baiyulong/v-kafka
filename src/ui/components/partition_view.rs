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
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    render_list(f, chunks[0], app);
    render_detail(f, chunks[1], app);
}

fn render_list(f: &mut Frame, area: Rect, app: &App) {
    let topic_name = app.selected_topic.as_deref().unwrap_or("—");
    let partitions = app
        .selected_topic_meta()
        .map(|t| t.partitions.as_slice())
        .unwrap_or(&[]);

    let items: Vec<ListItem> = partitions
        .iter()
        .map(|p| {
            // Match watermark for this partition
            let wm = app.watermarks.iter().find(|(id, _, _)| *id == p.id);
            let (low, high) = wm.map(|(_, l, h)| (*l, *h)).unwrap_or((-1, -1));
            let msgs = if high >= low && low >= 0 {
                high - low
            } else {
                0
            };

            let err_indicator = if p.error.is_some() { "⚠ " } else { "" };
            let line = Line::from(vec![
                Span::styled(format!("  {:>3}  ", p.id), Theme::key()),
                Span::styled(format!("L:{:<4} ", p.leader), Theme::normal()),
                Span::styled(err_indicator, Theme::error()),
                if high >= 0 {
                    Span::styled(format!("{} msgs", msgs), Theme::success())
                } else {
                    Span::styled("—", Theme::dim())
                },
            ]);
            ListItem::new(line)
        })
        .collect();

    let list = List::new(if items.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  No partitions",
            Theme::dim(),
        )))]
    } else {
        items
    })
    .block(
        Block::default()
            .title(format!(
                " {} — Partitions  [r]efresh [Enter]browse ",
                topic_name
            ))
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    )
    .highlight_style(Theme::selected())
    .highlight_symbol("▶ ");

    let mut state = ListState::default();
    if !partitions.is_empty() {
        state.select(Some(
            app.list_cursor.min(partitions.len().saturating_sub(1)),
        ));
    }
    f.render_stateful_widget(list, area, &mut state);
}

fn render_detail(f: &mut Frame, area: Rect, app: &App) {
    let partitions = app
        .selected_topic_meta()
        .map(|t| t.partitions.as_slice())
        .unwrap_or(&[]);

    let partition = partitions.get(app.list_cursor.min(partitions.len().saturating_sub(1)));

    let content = match partition {
        None => vec![
            Line::from(""),
            Line::from(Span::styled("  No partition selected", Theme::dim())),
        ],
        Some(p) => {
            let wm = app.watermarks.iter().find(|(id, _, _)| *id == p.id);
            let (low, high) = wm.map(|(_, l, h)| (*l, *h)).unwrap_or((-1, -1));
            let msgs = if high >= low && low >= 0 {
                high - low
            } else {
                0
            };

            let replicas_str = p
                .replicas
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let isr_str = p
                .isr
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ");

            vec![
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Partition ID    ", Theme::key()),
                    Span::raw(p.id.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("  Leader          ", Theme::key()),
                    Span::styled(p.leader.to_string(), Theme::success()),
                ]),
                Line::from(vec![
                    Span::styled("  Replicas        ", Theme::key()),
                    Span::raw(replicas_str),
                ]),
                Line::from(vec![
                    Span::styled("  In-Sync Replicas", Theme::key()),
                    Span::styled(isr_str, Theme::success()),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::styled("  Earliest Offset ", Theme::key()),
                    Span::styled(
                        if low >= 0 {
                            low.to_string()
                        } else {
                            "—".into()
                        },
                        Theme::normal(),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("  Latest Offset   ", Theme::key()),
                    Span::styled(
                        if high >= 0 {
                            high.to_string()
                        } else {
                            "—".into()
                        },
                        Theme::normal(),
                    ),
                ]),
                Line::from(vec![
                    Span::styled("  Message Count   ", Theme::key()),
                    Span::styled(
                        if high >= 0 {
                            msgs.to_string()
                        } else {
                            "—".into()
                        },
                        Theme::success(),
                    ),
                ]),
                if let Some(err) = &p.error {
                    Line::from(vec![
                        Span::styled("  Error           ", Theme::key()),
                        Span::styled(err.as_str(), Theme::error()),
                    ])
                } else {
                    Line::from("")
                },
                Line::from(""),
                Line::from(Span::styled(
                    "  [Enter] browse messages  [r] refresh offsets",
                    Theme::dim(),
                )),
            ]
        }
    };

    let para = Paragraph::new(content).block(
        Block::default()
            .title(" Partition Detail ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block()),
    );
    f.render_widget(para, area);
}
