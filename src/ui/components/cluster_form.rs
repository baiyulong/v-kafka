use crate::app::{App, ClusterFormField, InputMode};
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
        .constraints([Constraint::Percentage(38), Constraint::Percentage(62)])
        .split(area);

    render_field_list(f, chunks[0], app);
    render_input_area(f, chunks[1], app);
}

fn render_field_list(f: &mut Frame, area: Rect, app: &App) {
    let form = &app.cluster_form;
    let fields = form.fields();
    let focused_idx = form.focused_field_index.min(fields.len() - 1);

    let items: Vec<ListItem> = fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let value = form.field_value(field);
            let is_focused = i == focused_idx;
            let label_style = if is_focused {
                Theme::key()
            } else {
                Theme::dim()
            };
            let value_style = if value.is_empty() {
                Theme::dim()
            } else {
                Theme::normal()
            };

            let line = if matches!(field, ClusterFormField::Submit) {
                let style = if is_focused {
                    Theme::success().add_modifier(Modifier::BOLD)
                } else {
                    Theme::dim()
                };
                Line::from(Span::styled(format!("  {}", field.label()), style))
            } else {
                Line::from(vec![
                    Span::styled(format!("  {:<22}", field.label()), label_style),
                    Span::styled(
                        if value.is_empty() {
                            "—".into()
                        } else {
                            value
                        },
                        value_style,
                    ),
                ])
            };
            ListItem::new(line)
        })
        .collect();

    let title = if app.cluster_form_edit_index.is_some() {
        " Edit Cluster  [Tab]next  [Shift+Tab]prev  [Enter]edit  [Esc]cancel "
    } else {
        " New Cluster   [Tab]next  [Shift+Tab]prev  [Enter]edit  [Esc]cancel "
    };

    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    state.select(Some(focused_idx));
    f.render_stateful_widget(list, area, &mut state);
}

fn render_input_area(f: &mut Frame, area: Rect, app: &App) {
    let form = &app.cluster_form;
    let fields = form.fields();
    let focused_field = fields
        .get(form.focused_field_index.min(fields.len() - 1))
        .cloned()
        .unwrap_or(ClusterFormField::Name);

    match &focused_field {
        ClusterFormField::AuthMechanism => render_auth_selector(f, area, app),
        ClusterFormField::VerifyHostname => render_boolean_selector(f, area, app),
        ClusterFormField::Submit => render_submit_panel(f, area, app),
        _ => render_text_input(f, area, app, &focused_field),
    }
}

fn render_text_input(f: &mut Frame, area: Rect, app: &App, field: &ClusterFormField) {
    let form = &app.cluster_form;
    let value = form.field_value(field);
    let is_editing = app.input_mode == InputMode::Editing;

    let display = if is_editing {
        format!("{}_", value)
    } else {
        if value.is_empty() {
            "  Press Enter to edit…".to_string()
        } else {
            format!("  {}", value)
        }
    };

    let border_style = if is_editing {
        Theme::block_active()
    } else {
        Theme::block()
    };

    let hint = match field {
        ClusterFormField::BootstrapServers => "  e.g. broker1:9092,broker2:9092",
        ClusterFormField::SchemaRegistryUrl => {
            "  e.g. http://schema-registry:8081  (leave blank to skip)"
        }
        ClusterFormField::KerberosServiceName => "  default: kafka",
        _ => "",
    };

    let content = vec![
        Line::from(""),
        Line::from(Span::styled(format!("  {}", display), Theme::normal())),
        Line::from(""),
        Line::from(Span::styled(hint, Theme::dim())),
        Line::from(""),
        Line::from(Span::styled(
            if is_editing {
                "  [Enter] confirm  [Esc] cancel"
            } else {
                "  [Enter] edit"
            },
            Theme::dim(),
        )),
    ];

    let para = Paragraph::new(content).block(
        Block::default()
            .title(format!(" {} ", field.label()))
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(border_style),
    );
    f.render_widget(para, area);
}

fn render_auth_selector(f: &mut Frame, area: Rect, app: &App) {
    let form = &app.cluster_form;
    let items: Vec<ListItem> = [
        "PLAINTEXT   — no encryption or authentication",
        "SSL/TLS     — encrypted, certificate-based",
        "SASL/PLAIN  — username/password (plaintext SASL)",
        "SCRAM-256   — SASL/SCRAM-SHA-256 + SSL",
        "SCRAM-512   — SASL/SCRAM-SHA-512 + SSL",
        "Kerberos    — GSSAPI/Kerberos",
    ]
    .iter()
    .map(|s| ListItem::new(Line::from(Span::raw(format!("  {}", s)))))
    .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .title(" Auth Mechanism  [↑↓] select  [Enter] confirm ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    state.select(Some(form.auth_index));
    f.render_stateful_widget(list, area, &mut state);
}

fn render_boolean_selector(f: &mut Frame, area: Rect, app: &App) {
    let form = &app.cluster_form;
    let items: Vec<ListItem> = vec![
        ListItem::new(Line::from(Span::raw(
            "  yes — verify server hostname (recommended)",
        ))),
        ListItem::new(Line::from(Span::raw("  no  — skip hostname verification"))),
    ];

    let list = List::new(items)
        .block(
            Block::default()
                .title(" Verify Hostname  [↑↓] select  [Enter] confirm ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    state.select(Some(if form.verify_hostname { 0 } else { 1 }));
    f.render_stateful_widget(list, area, &mut state);
}

fn render_submit_panel(f: &mut Frame, area: Rect, app: &App) {
    let form = &app.cluster_form;
    let valid = !form.name.trim().is_empty() && !form.bootstrap_servers.trim().is_empty();

    let content = vec![
        Line::from(""),
        Line::from(Span::styled(
            if valid {
                "  ✓ Configuration looks good"
            } else {
                "  ✗ Name and Bootstrap Servers are required"
            },
            if valid {
                Theme::success()
            } else {
                Theme::error()
            },
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Name:       ", Theme::key()),
            Span::raw(if form.name.is_empty() {
                "(empty)"
            } else {
                &form.name
            }),
        ]),
        Line::from(vec![
            Span::styled("  Brokers:    ", Theme::key()),
            Span::raw(if form.bootstrap_servers.is_empty() {
                "(empty)"
            } else {
                &form.bootstrap_servers
            }),
        ]),
        Line::from(vec![
            Span::styled("  Auth:       ", Theme::key()),
            Span::raw(form.auth_label()),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            if valid {
                "  Press Enter to save and test connection"
            } else {
                "  Fill in required fields first"
            },
            if valid { Theme::dim() } else { Theme::error() },
        )),
    ];

    let para = Paragraph::new(content).block(
        Block::default()
            .title(" Ready to Save ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(if valid {
                Theme::success()
            } else {
                Theme::error()
            }),
    );
    f.render_widget(para, area);
}
