use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, Paragraph},
    Frame,
};
use crate::app::App;
use crate::ui::{layout::centered_rect, theme::Theme};

pub fn render_error(f: &mut Frame, area: Rect, app: &App) {
    if let Some(msg) = &app.error_message {
        let popup_area = centered_rect(60, 5, area);
        f.render_widget(Clear, popup_area);
        let content = vec![
            Line::from(""),
            Line::from(Span::styled(format!("  {}", msg), Theme::error())),
            Line::from(""),
            Line::from(Span::styled("  Press any key to dismiss", Theme::dim())),
        ];
        let para = Paragraph::new(content).block(
            Block::default()
                .title(" Error ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::error()),
        );
        f.render_widget(para, popup_area);
    }
}
