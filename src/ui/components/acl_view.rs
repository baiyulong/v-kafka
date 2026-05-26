use ratatui::{
    layout::{Constraint, Rect},
    style::Modifier,
    widgets::{Block, BorderType, Borders, Cell, Row, Table},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let header = Row::new(vec![
        Cell::from("RESOURCE").style(ratatui::style::Style::default().add_modifier(Modifier::BOLD).fg(ratatui::style::Color::Cyan)),
        Cell::from("NAME").style(ratatui::style::Style::default().add_modifier(Modifier::BOLD).fg(ratatui::style::Color::Cyan)),
        Cell::from("PRINCIPAL").style(ratatui::style::Style::default().add_modifier(Modifier::BOLD).fg(ratatui::style::Color::Cyan)),
        Cell::from("OPERATION").style(ratatui::style::Style::default().add_modifier(Modifier::BOLD).fg(ratatui::style::Color::Cyan)),
        Cell::from("PERMISSION").style(ratatui::style::Style::default().add_modifier(Modifier::BOLD).fg(ratatui::style::Color::Cyan)),
        Cell::from("HOST").style(ratatui::style::Style::default().add_modifier(Modifier::BOLD).fg(ratatui::style::Color::Cyan)),
    ]);

    let widths = [
        Constraint::Length(15),
        Constraint::Length(25),
        Constraint::Length(25),
        Constraint::Length(16),
        Constraint::Length(12),
        Constraint::Min(8),
    ];

    let rows: Vec<Row> = if app.acl_list.is_empty() {
        let msg = if app.acl_loading { "Loading ACLs…" } else { "No ACLs found (press [r] to refresh)" };
        vec![Row::new(vec![Cell::from(msg).style(Theme::dim())])]
    } else {
        app.acl_list.iter().enumerate().map(|(i, acl)| {
            let perm_style = match acl.permission.as_str() {
                "Allow" => Theme::success(),
                "Deny"  => Theme::error(),
                _       => Theme::normal(),
            };
            let style = if i == app.list_cursor { Theme::selected() }
                        else if i % 2 == 0 { Theme::normal() }
                        else { ratatui::style::Style::default().fg(ratatui::style::Color::Gray) };
            Row::new(vec![
                Cell::from(acl.resource_type.clone()),
                Cell::from(acl.name.clone()),
                Cell::from(acl.principal.clone()),
                Cell::from(acl.operation.clone()),
                Cell::from(acl.permission.clone()).style(perm_style),
                Cell::from(acl.host.clone()),
            ]).style(style)
        }).collect()
    };

    let title = format!(
        " ACL Management ─ {} entries  [r]efresh  [n]ew  [d]elete  [Esc] back ",
        app.acl_list.len()
    );
    let table = Table::new(std::iter::once(header).chain(rows), widths)
        .block(
            Block::default()
                .title(title.as_str())
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .column_spacing(1);
    f.render_widget(table, area);
}
