use crate::app::{App, InputMode};
use crate::ui::theme::Theme;
use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let content = match app.input_mode {
        InputMode::Editing => Line::from(vec![
            Span::styled(" INPUT: ", Theme::warning()),
            Span::raw(&app.search_input),
            Span::raw("█"),
        ]),
        InputMode::Confirm => Line::from(vec![
            Span::styled(" CONFIRM ", Theme::error()),
            Span::raw(app.status_message.as_deref().unwrap_or("")),
        ]),
        InputMode::Normal => {
            let msg = app
                .status_message
                .as_deref()
                .unwrap_or(" q:quit  ?:help  /:search  r:refresh  n:new  d:delete");
            Line::from(Span::styled(msg, Theme::status_bar()))
        }
    };
    f.render_widget(Paragraph::new(content).style(Theme::status_bar()), area);
}
