use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState, Paragraph},
    Frame,
};
use crate::app::App;
use crate::config::cluster::AuthMechanism;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    render_list(f, chunks[0], app);
    render_detail(f, chunks[1], app);
}

fn render_list(f: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .profile_manager
        .profiles
        .iter()
        .enumerate()
        .map(|(i, cluster)| {
            let auth_badge = auth_badge(&cluster.auth);
            let is_active = app.active_cluster.as_ref()
                .map(|a| a.cluster.name == cluster.name)
                .unwrap_or(false);
            let name_style = if is_active { Theme::success() } else { Theme::normal() };
            let prefix = if is_active { "● " } else { "  " };
            let line = Line::from(vec![
                Span::styled(prefix, if is_active { Theme::success() } else { Theme::dim() }),
                Span::styled(&cluster.name, name_style),
                Span::raw("  "),
                Span::styled(auth_badge, Theme::dim()),
            ]);
            ListItem::new(line)
        })
        .collect();

    let empty = if items.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  No clusters. Press 'n' to add one.",
            Theme::dim(),
        )))]
    } else {
        vec![]
    };

    let display = if items.is_empty() { empty } else { items };

    let list = List::new(display)
        .block(
            Block::default()
                .title(" Clusters  [n]ew  [e]dit  [d]elete  [Enter]connect ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    if !app.profile_manager.profiles.is_empty() {
        state.select(Some(app.list_cursor));
    }
    f.render_stateful_widget(list, area, &mut state);
}

fn render_detail(f: &mut Frame, area: Rect, app: &App) {
    let content = if let Some(cluster) = app.profile_manager.profiles.get(app.list_cursor) {
        vec![
            Line::from(""),
            Line::from(vec![
                Span::styled("  Name            ", Theme::key()),
                Span::raw(&cluster.name),
            ]),
            Line::from(vec![
                Span::styled("  Bootstrap       ", Theme::key()),
                Span::raw(&cluster.bootstrap_servers),
            ]),
            Line::from(vec![
                Span::styled("  Auth            ", Theme::key()),
                Span::raw(auth_badge(&cluster.auth)),
            ]),
            Line::from(""),
            if cluster.schema_registry.is_some() {
                Line::from(vec![
                    Span::styled("  Schema Registry ", Theme::key()),
                    Span::styled(
                        cluster.schema_registry.as_ref().unwrap().url.as_str(),
                        Theme::success(),
                    ),
                ])
            } else {
                Line::from(vec![
                    Span::styled("  Schema Registry ", Theme::key()),
                    Span::styled("not configured", Theme::dim()),
                ])
            },
            Line::from(""),
            Line::from(Span::styled(
                "  Press Enter to connect",
                Theme::dim(),
            )),
        ]
    } else {
        vec![
            Line::from(""),
            Line::from(Span::styled("  Select a cluster to view details", Theme::dim())),
        ]
    };

    let para = Paragraph::new(content).block(
        Block::default()
            .title(" Connection Details ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block()),
    );
    f.render_widget(para, area);
}

fn auth_badge(auth: &AuthMechanism) -> &'static str {
    match auth {
        AuthMechanism::Plaintext => "PLAINTEXT",
        AuthMechanism::Ssl => "SSL/TLS",
        AuthMechanism::SaslPlain => "SASL/PLAIN",
        AuthMechanism::SaslScramSha256 => "SCRAM-256",
        AuthMechanism::SaslScramSha512 => "SCRAM-512",
        AuthMechanism::Kerberos => "GSSAPI",
    }
}
