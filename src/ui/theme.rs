use ratatui::style::{Color, Modifier, Style};

pub struct Theme;

impl Theme {
    // Title bar
    pub fn title_bar() -> Style {
        Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD)
    }

    // Status bar
    pub fn status_bar() -> Style {
        Style::default().fg(Color::Black).bg(Color::DarkGray)
    }

    // Selected item in a list
    pub fn selected() -> Style {
        Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD)
    }

    // Normal list item
    pub fn normal() -> Style {
        Style::default().fg(Color::White)
    }

    // Dimmed / secondary text
    pub fn dim() -> Style {
        Style::default().fg(Color::DarkGray)
    }

    // Highlighted key or header
    pub fn key() -> Style {
        Style::default().fg(Color::Yellow)
    }

    // Success / good state
    pub fn success() -> Style {
        Style::default().fg(Color::Green)
    }

    // Warning
    pub fn warning() -> Style {
        Style::default().fg(Color::Yellow)
    }

    // Error / bad state
    pub fn error() -> Style {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    }

    // Block border
    pub fn block() -> Style {
        Style::default().fg(Color::DarkGray)
    }

    // Active / focused block border
    pub fn block_active() -> Style {
        Style::default().fg(Color::Cyan)
    }
}
