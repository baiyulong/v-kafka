use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Paragraph},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, _app: &App) {
    let content = vec![
        Line::from(Span::styled("  Consumer Groups — Phase 5", Theme::dim())),
    ];
    let para = Paragraph::new(content).block(
        Block::default()
            .title(" Consumer Groups ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    );
    f.render_widget(para, area);
}

pub fn render_detail(f: &mut Frame, area: Rect, _app: &App) {
    let content = vec![
        Line::from(Span::styled("  Consumer Group Detail — Phase 5", Theme::dim())),
        Line::from(Span::styled("  [R] Reset offsets", Theme::dim())),
    ];
    let para = Paragraph::new(content).block(
        Block::default()
            .title(" Group Detail ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active()),
    );
    f.render_widget(para, area);
}
