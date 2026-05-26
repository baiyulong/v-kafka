use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState},
    Frame,
};
use crate::app::App;
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .profile_manager
        .profiles
        .iter()
        .map(|cluster| {
            let line = Line::from(vec![
                Span::styled("  ", Theme::normal()),
                Span::styled(&cluster.name, Theme::normal()),
                Span::styled("  ", Theme::dim()),
                Span::styled(&cluster.bootstrap_servers, Theme::dim()),
            ]);
            ListItem::new(line)
        })
        .collect();

    let empty_hint = if items.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  No clusters configured. Press 'n' to add one.",
            Theme::dim(),
        )))]
    } else {
        vec![]
    };

    let display_items = if items.is_empty() { empty_hint } else { items };

    let list = List::new(display_items)
        .block(
            Block::default()
                .title(" Cluster Connections ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Theme::block_active()),
        )
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    if !app.profile_manager.profiles.is_empty() {
        state.select(Some(app.list_cursor));
    }
    f.render_stateful_widget(list, area, &mut state);
}
