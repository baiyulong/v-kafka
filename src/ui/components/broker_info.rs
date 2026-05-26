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
        Line::from(Span::styled("  Broker info — Phase 3", Theme::dim())),
    ];
    let para = Paragraph::new(content).block(
        Block::default()
            .title(" Brokers ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    );
    f.render_widget(para, area);
}
