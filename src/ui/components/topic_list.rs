use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    // Placeholder — real topic list populated in Phase 3
    let placeholder = vec![
        ListItem::new(Line::from(vec![
            Span::styled("  __consumer_offsets", Theme::dim()),
            Span::styled("  (internal)", Theme::dim()),
        ])),
        ListItem::new(Line::from(Span::styled(
            "  Loading topics… (Phase 3)", Theme::dim(),
        ))),
    ];

    let list = List::new(placeholder)
        .block(
            Block::default()
                .title(" Topics  [b]rokers  [g]roups  [s]chema  [a]cl ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    state.select(Some(app.list_cursor));
    f.render_stateful_widget(list, area, &mut state);
}
