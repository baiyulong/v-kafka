use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::{App, InputMode, View};

/// Dispatch a key event to the correct handler based on current view and input mode
pub async fn handle_key_event(key: KeyEvent, app: &mut App) -> Result<()> {
    // Global quit bindings (always active unless in an input field)
    if app.input_mode == InputMode::Normal {
        match key.code {
            // Ctrl+C always quits
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.should_quit = true;
                return Ok(());
            }
            // 'q' quits or goes back
            KeyCode::Char('q') => {
                if app.view_stack.is_empty() {
                    app.should_quit = true;
                } else {
                    app.navigate_back();
                }
                return Ok(());
            }
            KeyCode::Esc => {
                if !app.view_stack.is_empty() {
                    app.navigate_back();
                }
                return Ok(());
            }
            KeyCode::Char('?') => {
                app.navigate_to(View::Help);
                return Ok(());
            }
            _ => {}
        }
    }

    // Dismiss error popup on any key
    if app.error_message.is_some() {
        app.error_message = None;
        return Ok(());
    }

    match app.input_mode {
        InputMode::Normal => handle_normal_mode(key, app).await,
        InputMode::Editing => { handle_editing_mode(key, app); Ok(()) }
        InputMode::Confirm => { handle_confirm_mode(key, app); Ok(()) }
    }
}

async fn handle_normal_mode(key: KeyEvent, app: &mut App) -> Result<()> {
    match &app.current_view {
        View::ClusterList => handle_cluster_list(key, app).await,
        View::TopicList => { handle_topic_list(key, app); Ok(()) }
        View::PartitionDetail => { handle_partition_detail(key, app); Ok(()) }
        View::MessageBrowser => handle_message_browser(key, app).await,
        View::MessageDetail => { handle_message_detail(key, app); Ok(()) }
        View::ConsumerGroups => { handle_consumer_groups(key, app); Ok(()) }
        View::ConsumerGroupDetail => { handle_consumer_group_detail(key, app); Ok(()) }
        View::BrokerInfo => { handle_broker_info(key, app); Ok(()) }
        View::Help => {
            app.navigate_back();
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn handle_cluster_list(key: KeyEvent, app: &mut App) -> Result<()> {
    let count = app.profile_manager.profiles.len();
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            if count > 0 {
                app.list_cursor = (app.list_cursor + 1).min(count - 1);
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter if count > 0 => {
            let profile = app.profile_manager.profiles[app.list_cursor].clone();
            app.active_cluster = Some(crate::config::profile::ClusterProfile { cluster: profile });
            app.navigate_to(View::TopicList);
            app.set_status("Connecting…");
        }
        KeyCode::Char('n') => {
            // TODO: open connection form
            app.set_status("Press 'n' to add a new cluster (not yet implemented)");
        }
        KeyCode::Char('d') if count > 0 => {
            let idx = app.list_cursor;
            app.profile_manager.remove(idx)?;
            app.list_cursor = app.list_cursor.saturating_sub(1);
            app.set_status("Cluster removed");
        }
        _ => {}
    }
    Ok(())
}

fn handle_topic_list(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            app.list_cursor = app.list_cursor.saturating_add(1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter => {
            app.navigate_to(View::PartitionDetail);
        }
        KeyCode::Char('b') => {
            app.navigate_to(View::BrokerInfo);
        }
        KeyCode::Char('g') => {
            app.navigate_to(View::ConsumerGroups);
        }
        KeyCode::Char('s') => {
            app.navigate_to(View::SchemaRegistry);
        }
        KeyCode::Char('a') => {
            app.navigate_to(View::AclManagement);
        }
        KeyCode::Char('/') => {
            app.input_mode = InputMode::Editing;
            app.search_input.clear();
        }
        KeyCode::Char('r') => {
            app.set_status("Refreshing topics…");
        }
        _ => {}
    }
}

fn handle_partition_detail(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Enter => {
            app.navigate_to(View::MessageBrowser);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.list_cursor = app.list_cursor.saturating_add(1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        _ => {}
    }
}

async fn handle_message_browser(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            app.list_cursor = app.list_cursor.saturating_add(1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter => {
            app.navigate_to(View::MessageDetail);
        }
        KeyCode::Char('o') => {
            app.input_mode = InputMode::Editing;
            app.search_input.clear();
            app.set_status("Jump to offset: ");
        }
        KeyCode::Char('p') => {
            app.navigate_to(View::ProducerForm);
        }
        KeyCode::Char('/') => {
            app.input_mode = InputMode::Editing;
            app.search_input.clear();
        }
        _ => {}
    }
    Ok(())
}

fn handle_message_detail(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            app.scroll_offset = app.scroll_offset.saturating_add(1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.scroll_offset = app.scroll_offset.saturating_sub(1);
        }
        _ => {}
    }
}

fn handle_consumer_groups(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            app.list_cursor = app.list_cursor.saturating_add(1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter => {
            app.navigate_to(View::ConsumerGroupDetail);
        }
        KeyCode::Char('r') => {
            app.set_status("Refreshing consumer groups…");
        }
        _ => {}
    }
}

fn handle_consumer_group_detail(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            app.list_cursor = app.list_cursor.saturating_add(1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Char('R') => {
            // Reset offsets — open confirm dialog
            app.input_mode = InputMode::Confirm;
            app.set_status("Reset offsets? (y/n)");
        }
        _ => {}
    }
}

fn handle_broker_info(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Char('r') => {
            app.set_status("Refreshing broker info…");
        }
        _ => {}
    }
}

fn handle_editing_mode(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.search_input.clear();
        }
        KeyCode::Enter => {
            // Commit the search/input
            app.input_mode = InputMode::Normal;
        }
        KeyCode::Backspace => {
            app.search_input.pop();
        }
        KeyCode::Char(c) => {
            app.search_input.push(c);
        }
        _ => {}
    }
}

fn handle_confirm_mode(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            // Confirmed action — handled per view
            app.input_mode = InputMode::Normal;
            app.set_status("Action confirmed");
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.status_message = None;
        }
        _ => {}
    }
}
