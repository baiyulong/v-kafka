use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let content = vec![
        Line::from(Span::styled("  Message detail — Phase 4", Theme::dim())),
        Line::from(Span::styled("  j/k to scroll", Theme::dim())),
    ];
    let para = Paragraph::new(content)
        .scroll((app.scroll_offset as u16, 0))
        .block(
            Block::default()
                .title(" Message Detail ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        );
    f.render_widget(para, area);
}
