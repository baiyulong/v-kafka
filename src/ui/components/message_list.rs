use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let placeholder = vec![
        ListItem::new(Line::from(vec![
            Span::styled("  #0  ", Theme::key()),
            Span::styled("key=…  ", Theme::dim()),
            Span::styled("value preview — Phase 4", Theme::normal()),
        ])),
    ];

    let list = List::new(placeholder)
        .block(
            Block::default()
                .title(" Messages  [o]ffset  [p]roduce  [/]filter ")
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
