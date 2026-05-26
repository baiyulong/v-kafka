use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState, Paragraph},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    render_broker_list(f, chunks[0], app);
    render_cluster_summary(f, chunks[1], app);
}

fn render_broker_list(f: &mut Frame, area: Rect, app: &App) {
    let brokers = &app.metadata.brokers;

    let items: Vec<ListItem> = brokers.iter().map(|b| {
        let line = Line::from(vec![
            Span::styled(format!("  {:>3}  ", b.id), Theme::key()),
            Span::raw(format!("{}:{}", b.host, b.port)),
        ]);
        ListItem::new(line)
    }).collect();

    let list = List::new(if items.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  No broker info  [r] to refresh",
            Theme::dim(),
        )))]
    } else {
        items
    })
    .block(
        Block::default()
            .title(" Brokers  [r]efresh ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    )
    .highlight_style(Theme::selected())
    .highlight_symbol("▶ ");

    let mut state = ListState::default();
    if !brokers.is_empty() {
        state.select(Some(app.list_cursor.min(brokers.len().saturating_sub(1))));
    }
    f.render_stateful_widget(list, area, &mut state);
}

fn render_cluster_summary(f: &mut Frame, area: Rect, app: &App) {
    let cluster_name = app.active_cluster.as_ref()
        .map(|c| c.cluster.name.as_str())
        .unwrap_or("—");
    let bootstrap = app.active_cluster.as_ref()
        .map(|c| c.cluster.bootstrap_servers.as_str())
        .unwrap_or("—");

    let brokers = &app.metadata.brokers;
    let topics = &app.metadata.topics;
    let internal = topics.iter().filter(|t| t.is_internal).count();
    let user_topics = topics.len() - internal;

    let selected_broker = brokers.get(
        app.list_cursor.min(brokers.len().saturating_sub(1))
    );

    let mut lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  Cluster Name    ", Theme::key()),
            Span::raw(cluster_name),
        ]),
        Line::from(vec![
            Span::styled("  Bootstrap       ", Theme::key()),
            Span::raw(bootstrap),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Brokers         ", Theme::key()),
            Span::styled(brokers.len().to_string(), Theme::success()),
        ]),
        Line::from(vec![
            Span::styled("  Topics          ", Theme::key()),
            Span::styled(user_topics.to_string(), Theme::success()),
        ]),
        Line::from(vec![
            Span::styled("  Internal Topics ", Theme::key()),
            Span::styled(internal.to_string(), Theme::dim()),
        ]),
    ];

    if let Some(b) = selected_broker {
        lines.extend([
            Line::from(""),
            Line::from(Span::styled("  Selected Broker:", Theme::key())),
            Line::from(vec![
                Span::styled("  ID              ", Theme::key()),
                Span::raw(b.id.to_string()),
            ]),
            Line::from(vec![
                Span::styled("  Host            ", Theme::key()),
                Span::raw(b.host.as_str()),
            ]),
            Line::from(vec![
                Span::styled("  Port            ", Theme::key()),
                Span::raw(b.port.to_string()),
            ]),
        ]);
    }

    let para = Paragraph::new(lines).block(
        Block::default()
            .title(" Cluster Summary ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block()),
    );
    f.render_widget(para, area);
}
