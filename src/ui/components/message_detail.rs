use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let msg = app
        .selected_message_idx
        .and_then(|i| app.messages.get(i));

    let Some(msg) = msg else {
        let para = Paragraph::new(" No message selected")
            .style(Theme::dim())
            .block(
                Block::default()
                    .title(" Message Detail ")
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(Theme::block_active()),
            );
        f.render_widget(para, area);
        return;
    };

    let ts = msg
        .timestamp
        .map(|t| t.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| "-".to_string());

    let mut lines: Vec<Line> = vec![
        Line::from(vec![
            Span::styled("  Offset:    ", Theme::key()),
            Span::styled(msg.offset.to_string(), Theme::normal()),
        ]),
        Line::from(vec![
            Span::styled("  Partition: ", Theme::key()),
            Span::styled(msg.partition.to_string(), Theme::normal()),
        ]),
        Line::from(vec![
            Span::styled("  Timestamp: ", Theme::key()),
            Span::styled(ts, Theme::normal()),
        ]),
        Line::from(vec![
            Span::styled("  Key:       ", Theme::key()),
            Span::styled(msg.key_display(), Theme::normal()),
        ]),
    ];

    if !msg.headers.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("  Headers:", Theme::key())));
        for (k, v) in &msg.headers {
            let val = String::from_utf8(v.clone()).unwrap_or_else(|_| format!("<binary {}B>", v.len()));
            lines.push(Line::from(vec![
                Span::styled("    ", Theme::normal()),
                Span::styled(format!("{} = ", k), Theme::dim()),
                Span::styled(val, Theme::normal()),
            ]));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled("  Value:", Theme::key())));
    for line in msg.value_pretty().lines() {
        lines.push(Line::from(vec![
            Span::styled("  ", Theme::normal()),
            Span::styled(line.to_string(), Theme::normal()),
        ]));
    }

    let title = format!(" Message #{} ─ {}/P{} ", msg.offset, msg.topic, msg.partition);
    let para = Paragraph::new(lines)
        .scroll((app.scroll_offset as u16, 0))
        .block(
            Block::default()
                .title(title.as_str())
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        );
    f.render_widget(para, area);
}
