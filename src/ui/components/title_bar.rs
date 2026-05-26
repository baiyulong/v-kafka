use crate::app::App;
use crate::ui::theme::Theme;
use ratatui::{
    layout::Rect,
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let cluster_name = app
        .active_cluster
        .as_ref()
        .map(|p| p.cluster.name.as_str())
        .unwrap_or("(no connection)");

    let view_name = format!("{:?}", app.current_view);

    let title = Line::from(vec![
        Span::styled(" v-kafka ", Theme::title_bar()),
        Span::styled("│ ", Theme::title_bar()),
        Span::styled(cluster_name, Theme::title_bar()),
        Span::styled(" › ", Theme::title_bar()),
        Span::styled(&view_name, Theme::title_bar()),
    ]);

    f.render_widget(Paragraph::new(title).style(Theme::title_bar()), area);
}
