use crate::app::App;
use crate::ui::theme::Theme;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Cell, List, ListItem, ListState, Row, Table},
    Frame,
};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let topic = app.selected_topic.as_deref().unwrap_or("?");
    let partition = app.selected_partition.unwrap_or(-1);

    let title = format!(
        " Messages ─ {}/P{} ─ [o]ffset [t]ime [[] prev []] next [r]eload [p]roduce ",
        topic, partition
    );

    let block = Block::default()
        .title(title.as_str())
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Theme::block_active());

    if app.messages.is_empty() {
        let hint = if app.messages_loading {
            " Loading messages…"
        } else {
            " No messages. Press [r] to load, [o] to jump to offset."
        };
        let para = ratatui::widgets::Paragraph::new(hint)
            .style(Theme::dim())
            .block(block);
        f.render_widget(para, area);
        return;
    }

    // Header + rows layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(block.inner(area));

    f.render_widget(block, area);

    // Column header
    let header = Row::new(vec![
        Cell::from("OFFSET").style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(ratatui::style::Color::Cyan),
        ),
        Cell::from("TIMESTAMP").style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(ratatui::style::Color::Cyan),
        ),
        Cell::from("KEY").style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(ratatui::style::Color::Cyan),
        ),
        Cell::from("VALUE").style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(ratatui::style::Color::Cyan),
        ),
    ]);

    let _rows: Vec<Row> = app
        .messages
        .iter()
        .enumerate()
        .map(|(i, msg)| {
            let ts = msg
                .timestamp
                .map(|t| t.format("%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "-".to_string());
            let key = msg.key_display();
            let val = msg.value_preview(60);
            let style = if app.selected_message_idx == Some(i) {
                Theme::selected()
            } else if i % 2 == 0 {
                Theme::normal()
            } else {
                Style::default().fg(ratatui::style::Color::Gray)
            };
            Row::new(vec![
                Cell::from(format!("{:>12}", msg.offset)),
                Cell::from(ts),
                Cell::from(key),
                Cell::from(val),
            ])
            .style(style)
        })
        .collect();

    let widths = [
        Constraint::Length(14),
        Constraint::Length(17),
        Constraint::Length(22),
        Constraint::Min(20),
    ];

    let header_row = Table::new([header], widths).block(Block::default());
    f.render_widget(header_row, chunks[0]);

    // Message list (scrollable)
    let items: Vec<ListItem> = app
        .messages
        .iter()
        .map(|msg| {
            let ts = msg
                .timestamp
                .map(|t| t.format("%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "    -        ".to_string());
            let key = {
                let k = msg.key_display();
                if k.len() > 20 {
                    format!("{}…", &k[..19])
                } else {
                    format!("{:<20}", k)
                }
            };
            let val = msg.value_preview(50);
            ListItem::new(Line::from(vec![
                Span::styled(format!("{:>12}  ", msg.offset), Theme::key()),
                Span::styled(format!("{}  ", ts), Theme::dim()),
                Span::styled(format!("{}  ", key), Theme::normal()),
                Span::styled(val, Theme::dim()),
            ]))
        })
        .collect();

    let list = List::new(items)
        .highlight_style(Theme::selected())
        .highlight_symbol("▶ ");

    let mut state = ListState::default();
    state.select(app.selected_message_idx);
    f.render_stateful_widget(list, chunks[1], &mut state);

    // Footer
    let info = if app.messages_loading {
        "  Loading…".to_string()
    } else {
        let first = app.messages.first().map(|m| m.offset).unwrap_or(0);
        let last = app.messages.last().map(|m| m.offset).unwrap_or(0);
        format!(
            "  {} messages  offsets {}–{}  [Enter] detail  [/] filter  [j/k] navigate",
            app.messages.len(),
            first,
            last
        )
    };
    let footer = ratatui::widgets::Paragraph::new(info).style(Theme::dim());
    f.render_widget(footer, chunks[2]);

    // Overlay: input prompt
    if app.message_input != crate::app::MessageInput::None {
        let prompt = match app.message_input {
            crate::app::MessageInput::Offset => format!("Jump to offset: {}_", app.search_input),
            crate::app::MessageInput::Timestamp => format!("Timestamp (ms): {}_", app.search_input),
            crate::app::MessageInput::Filter => format!("Filter: {}_", app.search_input),
            crate::app::MessageInput::None => String::new(),
        };
        let popup_area = Rect {
            x: area.x + 2,
            y: area.y + area.height.saturating_sub(4),
            width: area.width.saturating_sub(4),
            height: 3,
        };
        let popup = ratatui::widgets::Paragraph::new(prompt.clone())
            .style(Theme::normal())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Theme::block_active()),
            );
        f.render_widget(ratatui::widgets::Clear, popup_area);
        f.render_widget(popup, popup_area);
    }
}
