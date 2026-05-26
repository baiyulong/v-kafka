use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
};

/// Build the 3-row main layout: title bar / content / status bar
pub fn build_layout(area: Rect) -> Vec<Rect> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),  // title bar
            Constraint::Min(0),     // content
            Constraint::Length(1),  // status bar
        ])
        .split(area)
        .to_vec()
}

/// Split area horizontally into two panes (e.g., list + detail)
pub fn split_horizontal(area: Rect, left_pct: u16) -> (Rect, Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(left_pct),
            Constraint::Percentage(100 - left_pct),
        ])
        .split(area);
    (chunks[0], chunks[1])
}

/// Center a rect of the given size within another rect
pub fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect {
        x,
        y,
        width: width.min(area.width),
        height: height.min(area.height),
    }
}
