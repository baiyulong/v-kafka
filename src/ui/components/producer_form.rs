use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    widgets::{Block, BorderType, Borders, Paragraph},
    Frame,
};
use crate::app::{App, ProducerForm};
use crate::ui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(area);

    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // topic
            Constraint::Length(3), // partition
            Constraint::Length(3), // key
            Constraint::Length(5), // value (taller)
            Constraint::Length(3), // headers
            Constraint::Length(3), // send button
            Constraint::Min(1),    // spacer + result
        ])
        .split(Block::default()
            .title(" Produce Message ─ [Tab] next field  [Enter] send  [Esc] back ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Theme::block_active())
            .inner(chunks[0]));

    let block = Block::default()
        .title(" Produce Message ─ [Tab] next field  [Enter] send  [Esc] back ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Theme::block_active());
    f.render_widget(block, chunks[0]);

    let form = &app.producer_form;

    // Render each field
    for (idx, label) in ProducerForm::FIELDS.iter().enumerate() {
        if idx >= inner_chunks.len() { break; }
        let is_focused = form.focused_field == idx;
        let border_style = if is_focused { Theme::block_active() } else { Theme::block() };

        if idx == 5 {
            // Send button
            let send_label = if is_focused {
                "  ▶  [ Send Message ]  ◀"
            } else {
                "     [ Send Message ]"
            };
            let btn = Paragraph::new(send_label)
                .style(if is_focused { Theme::selected() } else { Theme::dim() })
                .block(Block::default().borders(Borders::ALL).border_style(border_style));
            f.render_widget(btn, inner_chunks[idx]);
        } else {
            let value = form.field_value(idx);
            let display = if is_focused {
                format!("{}_", value)
            } else {
                value
            };
            let field = Paragraph::new(display)
                .style(if is_focused { Theme::normal() } else { Theme::dim() })
                .block(
                    Block::default()
                        .title(format!(" {} ", label))
                        .borders(Borders::ALL)
                        .border_style(border_style),
                );
            f.render_widget(field, inner_chunks[idx]);
        }
    }

    // Result line
    if let Some(result) = &form.last_result {
        let style = if result.starts_with("✓") { Theme::success() } else { Theme::error() };
        if let Some(last_chunk) = inner_chunks.last() {
            let para = Paragraph::new(format!("  {}", result)).style(style);
            f.render_widget(para, *last_chunk);
        }
    }

    // Hint bar
    let hint = "  [Tab] next field  [Shift+Tab] prev  [Enter] send  [Esc] back  Headers: key=val,key2=val2";
    let hint_para = Paragraph::new(hint).style(Theme::dim());
    f.render_widget(hint_para, chunks[1]);
}
