use anyhow::Result;
use crossterm::event::{self, Event, KeyEvent};
use std::time::Duration;
use tokio::sync::mpsc;

pub mod handler;

/// Application-level events
pub enum AppEvent {
    Key(KeyEvent),
    Tick,
}

/// Drives the event loop; emits AppEvent via an async channel
pub struct EventHandler {
    rx: mpsc::UnboundedReceiver<AppEvent>,
}

impl EventHandler {
    pub fn new(tick_rate_ms: u64) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            loop {
                if event::poll(Duration::from_millis(tick_rate_ms)).unwrap_or(false) {
                    if let Ok(Event::Key(key)) = event::read() {
                        let _ = tx_clone.send(AppEvent::Key(key));
                    }
                } else {
                    let _ = tx_clone.send(AppEvent::Tick);
                }
            }
        });

        Self { rx }
    }

    pub async fn next(&mut self) -> Result<AppEvent> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("Event channel closed"))
    }
}
