use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use crate::app::{App, ClusterFormField, InputMode, View, AUTH_MECHANISMS};
use crate::kafka::client::KafkaClient;

pub async fn handle_key_event(key: KeyEvent, app: &mut App) -> Result<()> {
    // Ctrl+C always quits
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        app.should_quit = true;
        return Ok(());
    }

    // Dismiss error popup on any key
    if app.error_message.is_some() {
        app.error_message = None;
        return Ok(());
    }

    match app.input_mode {
        InputMode::Normal => handle_normal(key, app).await,
        InputMode::Editing => handle_editing(key, app).await,
        InputMode::Confirm => handle_confirm(key, app),
    }
}

// ─── Normal mode ─────────────────────────────────────────────────────────────

async fn handle_normal(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => {
            if app.view_stack.is_empty() {
                app.should_quit = true;
            } else {
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

    match &app.current_view {
        View::ClusterList => handle_cluster_list(key, app).await,
        View::TopicList   => { handle_topic_list(key, app); Ok(()) }
        View::PartitionDetail => { handle_partition_detail(key, app); Ok(()) }
        View::MessageBrowser  => { handle_message_browser(key, app); Ok(()) }
        View::MessageDetail   => { handle_message_detail(key, app); Ok(()) }
        View::ConsumerGroups  => { handle_consumer_groups(key, app); Ok(()) }
        View::ConsumerGroupDetail => { handle_consumer_group_detail(key, app); Ok(()) }
        View::BrokerInfo      => { handle_broker_info(key, app); Ok(()) }
        View::Help            => { app.navigate_back(); Ok(()) }
        _ => Ok(()),
    }
}

async fn handle_cluster_list(key: KeyEvent, app: &mut App) -> Result<()> {
    let count = app.profile_manager.profiles.len();
    match key.code {
        KeyCode::Down | KeyCode::Char('j') if count > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter if count > 0 => {
            connect_to_cluster(app).await?;
        }
        KeyCode::Char('n') => {
            app.open_new_cluster_form();
        }
        KeyCode::Char('e') if count > 0 => {
            let idx = app.list_cursor;
            app.open_edit_cluster_form(idx);
        }
        KeyCode::Char('d') if count > 0 => {
            app.input_mode = InputMode::Confirm;
            app.set_status(format!(
                "Delete '{}'? (y/n)",
                app.profile_manager.profiles[app.list_cursor].name
            ));
        }
        _ => {}
    }
    Ok(())
}

async fn connect_to_cluster(app: &mut App) -> Result<()> {
    let cluster = app.profile_manager.profiles[app.list_cursor].clone();
    app.set_status(format!("Connecting to {}…", cluster.name));

    match KafkaClient::new(&cluster) {
        Ok(client) => {
            match client.test_connection(Duration::from_secs(5)) {
                Ok(info) => {
                    let msg = format!(
                        "Connected: {} broker(s) at {}",
                        info.broker_count,
                        cluster.bootstrap_servers
                    );
                    app.set_status(msg);
                    app.active_cluster = Some(crate::config::profile::ClusterProfile { cluster });
                    app.kafka_client = Some(client);
                    app.navigate_to(View::TopicList);
                }
                Err(e) => {
                    app.set_error(format!("Connection failed: {}", e));
                }
            }
        }
        Err(e) => {
            app.set_error(format!("Client init failed: {}", e));
        }
    }
    Ok(())
}

fn handle_topic_list(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => app.list_cursor = app.list_cursor.saturating_add(1),
        KeyCode::Up | KeyCode::Char('k')   => app.list_cursor = app.list_cursor.saturating_sub(1),
        KeyCode::Enter => app.navigate_to(View::PartitionDetail),
        KeyCode::Char('b') => app.navigate_to(View::BrokerInfo),
        KeyCode::Char('g') => app.navigate_to(View::ConsumerGroups),
        KeyCode::Char('s') => app.navigate_to(View::SchemaRegistry),
        KeyCode::Char('a') => app.navigate_to(View::AclManagement),
        KeyCode::Char('p') => app.navigate_to(View::ProducerForm),
        KeyCode::Char('r') => app.set_status("Refreshing topics…"),
        KeyCode::Char('/') => {
            app.input_mode = InputMode::Editing;
            app.search_input.clear();
        }
        _ => {}
    }
}

fn handle_partition_detail(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Enter => app.navigate_to(View::MessageBrowser),
        KeyCode::Down | KeyCode::Char('j') => app.list_cursor = app.list_cursor.saturating_add(1),
        KeyCode::Up | KeyCode::Char('k')   => app.list_cursor = app.list_cursor.saturating_sub(1),
        _ => {}
    }
}

fn handle_message_browser(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => app.list_cursor = app.list_cursor.saturating_add(1),
        KeyCode::Up | KeyCode::Char('k')   => app.list_cursor = app.list_cursor.saturating_sub(1),
        KeyCode::Enter => app.navigate_to(View::MessageDetail),
        KeyCode::Char('o') => {
            app.input_mode = InputMode::Editing;
            app.search_input.clear();
            app.set_status("Jump to offset: ");
        }
        KeyCode::Char('p') => app.navigate_to(View::ProducerForm),
        KeyCode::Char('/') => {
            app.input_mode = InputMode::Editing;
            app.search_input.clear();
        }
        _ => {}
    }
}

fn handle_message_detail(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => app.scroll_offset = app.scroll_offset.saturating_add(1),
        KeyCode::Up | KeyCode::Char('k')   => app.scroll_offset = app.scroll_offset.saturating_sub(1),
        _ => {}
    }
}

fn handle_consumer_groups(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => app.list_cursor = app.list_cursor.saturating_add(1),
        KeyCode::Up | KeyCode::Char('k')   => app.list_cursor = app.list_cursor.saturating_sub(1),
        KeyCode::Enter => app.navigate_to(View::ConsumerGroupDetail),
        KeyCode::Char('r') => app.set_status("Refreshing consumer groups…"),
        _ => {}
    }
}

fn handle_consumer_group_detail(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => app.list_cursor = app.list_cursor.saturating_add(1),
        KeyCode::Up | KeyCode::Char('k')   => app.list_cursor = app.list_cursor.saturating_sub(1),
        KeyCode::Char('R') => {
            app.input_mode = InputMode::Confirm;
            app.set_status("Reset offsets to earliest? (y/n)");
        }
        _ => {}
    }
}

fn handle_broker_info(key: KeyEvent, app: &mut App) {
    if let KeyCode::Char('r') = key.code {
        app.set_status("Refreshing broker info…");
    }
}

// ─── Editing mode (cluster form) ─────────────────────────────────────────────

async fn handle_editing(key: KeyEvent, app: &mut App) -> Result<()> {
    // Non-form views use editing mode for search input
    if app.current_view != View::ClusterForm {
        match key.code {
            KeyCode::Esc => {
                app.input_mode = InputMode::Normal;
                app.search_input.clear();
            }
            KeyCode::Enter => app.input_mode = InputMode::Normal,
            KeyCode::Backspace => { app.search_input.pop(); }
            KeyCode::Char(c) => app.search_input.push(c),
            _ => {}
        }
        return Ok(());
    }

    // Cluster form editing
    handle_form_editing(key, app).await
}

async fn handle_form_editing(key: KeyEvent, app: &mut App) -> Result<()> {
    let fields = app.cluster_form.fields();
    let max = fields.len().saturating_sub(1);
    let focused = app.cluster_form.focused_field_index.min(max);
    let current_field = fields[focused].clone();

    match key.code {
        // Esc: if actively typing, stop typing; else leave form
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
        }

        // Tab / Shift+Tab: move between fields
        KeyCode::Tab => {
            app.cluster_form.focused_field_index = (focused + 1).min(max);
        }
        KeyCode::BackTab => {
            app.cluster_form.focused_field_index = focused.saturating_sub(1);
        }

        KeyCode::Enter => {
            match &current_field {
                ClusterFormField::Submit => {
                    // Save and test connection
                    save_and_test(app).await?;
                }
                ClusterFormField::AuthMechanism => {
                    // Toggle auth cycling on Enter
                    let next = (app.cluster_form.auth_index + 1) % AUTH_MECHANISMS.len();
                    app.cluster_form.auth_index = next;
                    // Reset field index since fields change
                    app.cluster_form.focused_field_index = focused.min(
                        ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1
                    );
                }
                ClusterFormField::VerifyHostname => {
                    app.cluster_form.verify_hostname = !app.cluster_form.verify_hostname;
                }
                _ => {
                    // For text fields Enter moves to next field
                    app.cluster_form.focused_field_index = (focused + 1).min(max);
                }
            }
        }

        // Arrow keys for selector fields
        KeyCode::Down | KeyCode::Char('j') => {
            match &current_field {
                ClusterFormField::AuthMechanism => {
                    let next = (app.cluster_form.auth_index + 1).min(AUTH_MECHANISMS.len() - 1);
                    app.cluster_form.auth_index = next;
                    app.cluster_form.focused_field_index = focused.min(
                        ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1
                    );
                }
                ClusterFormField::VerifyHostname => {
                    app.cluster_form.verify_hostname = false;
                }
                _ => {
                    app.cluster_form.focused_field_index = (focused + 1).min(max);
                }
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            match &current_field {
                ClusterFormField::AuthMechanism => {
                    app.cluster_form.auth_index = app.cluster_form.auth_index.saturating_sub(1);
                    app.cluster_form.focused_field_index = focused.min(
                        ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1
                    );
                }
                ClusterFormField::VerifyHostname => {
                    app.cluster_form.verify_hostname = true;
                }
                _ => {
                    app.cluster_form.focused_field_index = focused.saturating_sub(1);
                }
            }
        }

        // Text input for string fields
        KeyCode::Backspace => {
            if let Some(s) = app.cluster_form.focused_string_mut() {
                s.pop();
            }
        }
        KeyCode::Char(c) => {
            if let Some(s) = app.cluster_form.focused_string_mut() {
                s.push(c);
            }
        }

        _ => {}
    }
    Ok(())
}

async fn save_and_test(app: &mut App) -> Result<()> {
    let form = &app.cluster_form;
    if form.name.trim().is_empty() || form.bootstrap_servers.trim().is_empty() {
        app.set_error("Name and Bootstrap Servers are required");
        return Ok(());
    }

    let config = app.cluster_form.to_cluster_config();
    let name = config.name.clone();

    // Test connection first
    app.set_status(format!("Testing connection to {}…", config.bootstrap_servers));
    match KafkaClient::new(&config) {
        Ok(client) => {
            match client.test_connection(Duration::from_secs(5)) {
                Ok(info) => {
                    // Save profile
                    if let Some(idx) = app.cluster_form_edit_index {
                        app.profile_manager.profiles[idx] = config;
                        app.profile_manager.save()?;
                    } else {
                        app.profile_manager.add(config)?;
                    }
                    app.input_mode = InputMode::Normal;
                    app.navigate_back();
                    app.set_status(format!(
                        "✓ '{}' saved — {} broker(s) reachable",
                        name, info.broker_count
                    ));
                }
                Err(e) => {
                    // Still save but warn
                    if let Some(idx) = app.cluster_form_edit_index {
                        app.profile_manager.profiles[idx] = app.cluster_form.to_cluster_config();
                        app.profile_manager.save()?;
                    } else {
                        app.profile_manager.add(app.cluster_form.to_cluster_config())?;
                    }
                    app.input_mode = InputMode::Normal;
                    app.navigate_back();
                    app.set_status(format!("Saved '{}' (connection test failed: {})", name, e));
                }
            }
        }
        Err(e) => {
            app.set_error(format!("Client error: {}", e));
        }
    }
    Ok(())
}

// ─── Confirm mode ─────────────────────────────────────────────────────────────

fn handle_confirm(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            match &app.current_view {
                View::ClusterList => {
                    let idx = app.list_cursor;
                    if let Err(e) = app.profile_manager.remove(idx) {
                        app.set_error(format!("Failed to delete: {}", e));
                    } else {
                        app.list_cursor = app.list_cursor.saturating_sub(1);
                        app.set_status("Cluster deleted");
                    }
                }
                _ => {}
            }
            app.input_mode = InputMode::Normal;
            app.status_message = app.status_message.take().filter(|_| false); // clear confirm prompt
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.status_message = None;
        }
        _ => {}
    }
    Ok(())
}
