use crate::ui::{layout::centered_rect, theme::Theme};
use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect) {
    let popup_area = centered_rect(60, 24, area);
    f.render_widget(Clear, popup_area);

    let lines = vec![
        Line::from(""),
        Line::from(vec![Span::styled("  Navigation", Theme::key())]),
        Line::from(vec![
            Span::styled("  ↑↓ / j k   ", Theme::key()),
            Span::raw("Move cursor"),
        ]),
        Line::from(vec![
            Span::styled("  Enter       ", Theme::key()),
            Span::raw("Select / drill down"),
        ]),
        Line::from(vec![
            Span::styled("  Esc / q     ", Theme::key()),
            Span::raw("Go back / Quit"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled("  Topic List", Theme::key())]),
        Line::from(vec![
            Span::styled("  b           ", Theme::key()),
            Span::raw("Broker info"),
        ]),
        Line::from(vec![
            Span::styled("  g           ", Theme::key()),
            Span::raw("Consumer groups"),
        ]),
        Line::from(vec![
            Span::styled("  s           ", Theme::key()),
            Span::raw("Schema Registry"),
        ]),
        Line::from(vec![
            Span::styled("  a           ", Theme::key()),
            Span::raw("ACL management"),
        ]),
        Line::from(vec![
            Span::styled("  n           ", Theme::key()),
            Span::raw("New topic"),
        ]),
        Line::from(vec![
            Span::styled("  d           ", Theme::key()),
            Span::raw("Delete topic"),
        ]),
        Line::from(""),
        Line::from(vec![Span::styled("  Message Browser", Theme::key())]),
        Line::from(vec![
            Span::styled("  o           ", Theme::key()),
            Span::raw("Jump to offset"),
        ]),
        Line::from(vec![
            Span::styled("  p           ", Theme::key()),
            Span::raw("Produce message"),
        ]),
        Line::from(vec![
            Span::styled("  /           ", Theme::key()),
            Span::raw("Filter / search"),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  r           ", Theme::key()),
            Span::raw("Refresh current view"),
        ]),
        Line::from(vec![
            Span::styled("  ?           ", Theme::key()),
            Span::raw("This help screen"),
        ]),
        Line::from(""),
        Line::from(Span::styled("  Press any key to close", Theme::dim())),
    ];

    let para = Paragraph::new(lines).block(
        Block::default()
            .title(" Keyboard Shortcuts ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    );
    f.render_widget(para, popup_area);
}
