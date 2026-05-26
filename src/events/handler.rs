use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use crate::app::{App, ClusterFormField, InputMode, View, AUTH_MECHANISMS};
use crate::kafka::client::KafkaClient;
use crate::kafka::metadata::{fetch_cluster_metadata, fetch_watermarks};

pub async fn handle_key_event(key: KeyEvent, app: &mut App) -> Result<()> {
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        app.should_quit = true;
        return Ok(());
    }

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
        View::ClusterList     => handle_cluster_list(key, app).await,
        View::TopicList       => handle_topic_list(key, app).await,
        View::PartitionDetail => handle_partition_detail(key, app).await,
        View::MessageBrowser  => { handle_message_browser(key, app); Ok(()) }
        View::MessageDetail   => { handle_message_detail(key, app); Ok(()) }
        View::ConsumerGroups  => { handle_consumer_groups(key, app); Ok(()) }
        View::ConsumerGroupDetail => { handle_consumer_group_detail(key, app); Ok(()) }
        View::BrokerInfo      => handle_broker_info(key, app).await,
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
            match fetch_cluster_metadata(&client.config, Duration::from_secs(8)) {
                Ok(meta) => {
                    let msg = format!(
                        "Connected — {} broker(s), {} topics",
                        meta.brokers.len(),
                        meta.topics.iter().filter(|t| !t.is_internal).count()
                    );
                    app.metadata = meta;
                    app.active_cluster = Some(crate::config::profile::ClusterProfile { cluster });
                    app.kafka_client = Some(client);
                    app.filter.clear();
                    app.navigate_to(View::TopicList);
                    app.set_status(msg);
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

async fn handle_topic_list(key: KeyEvent, app: &mut App) -> Result<()> {
    let topics = app.filtered_topics();
    let count = topics.len();

    match key.code {
        KeyCode::Down | KeyCode::Char('j') if count > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter if count > 0 => {
            // Record selected topic and load watermarks
            let topic_name = topics[app.list_cursor.min(count - 1)].name.clone();
            app.selected_topic = Some(topic_name.clone());
            app.watermarks.clear();
            // Fetch watermarks in blocking thread to avoid blocking async runtime
            if let Some(client) = &app.kafka_client {
                let cfg = client.config.clone();
                let tn = topic_name.clone();
                match tokio::task::spawn_blocking(move || {
                    fetch_watermarks(&cfg, &tn, Duration::from_secs(8))
                }).await {
                    Ok(Ok(wm)) => {
                        app.watermarks = wm;
                        // Also update partition watermarks in metadata cache
                        if let Some(t) = app.metadata.topics.iter_mut().find(|t| t.name == topic_name) {
                            for p in t.partitions.iter_mut() {
                                if let Some(&(_, low, high)) = app.watermarks.iter().find(|(id, _, _)| *id == p.id) {
                                    p.low_watermark = Some(low);
                                    p.high_watermark = Some(high);
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => app.set_status(format!("Watermark fetch failed: {}", e)),
                    Err(e) => app.set_status(format!("Task error: {}", e)),
                }
            }
            app.navigate_to(View::PartitionDetail);
        }
        KeyCode::Char('b') => app.navigate_to(View::BrokerInfo),
        KeyCode::Char('g') => app.navigate_to(View::ConsumerGroups),
        KeyCode::Char('s') => app.navigate_to(View::SchemaRegistry),
        KeyCode::Char('a') => app.navigate_to(View::AclManagement),
        KeyCode::Char('p') => app.navigate_to(View::ProducerForm),
        KeyCode::Char('r') => {
            refresh_metadata(app).await?;
        }
        KeyCode::Char('/') => {
            app.input_mode = InputMode::Editing;
            app.search_input = app.filter.clone();
        }
        KeyCode::Esc if !app.filter.is_empty() => {
            app.filter.clear();
        }
        _ => {}
    }
    Ok(())
}

async fn handle_partition_detail(key: KeyEvent, app: &mut App) -> Result<()> {
    let partitions = app.selected_topic_meta()
        .map(|t| t.partitions.len())
        .unwrap_or(0);

    match key.code {
        KeyCode::Down | KeyCode::Char('j') if partitions > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(partitions - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter => {
            // Record selected partition
            if let Some(p) = app.selected_topic_meta()
                .and_then(|t| t.partitions.get(app.list_cursor))
            {
                app.selected_partition = Some(p.id);
            }
            app.navigate_to(View::MessageBrowser);
        }
        KeyCode::Char('r') => {
            // Refresh watermarks for this topic
            if let (Some(client), Some(topic)) = (&app.kafka_client, &app.selected_topic) {
                let cfg = client.config.clone();
                let tn = topic.clone();
                match tokio::task::spawn_blocking(move || {
                    fetch_watermarks(&cfg, &tn, Duration::from_secs(8))
                }).await {
                    Ok(Ok(wm)) => {
                        app.watermarks = wm;
                        app.set_status("Offsets refreshed");
                    }
                    Ok(Err(e)) => app.set_error(format!("Refresh failed: {}", e)),
                    Err(e) => app.set_error(format!("Task error: {}", e)),
                }
            }
        }
        _ => {}
    }
    Ok(())
}

async fn handle_broker_info(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            let n = app.metadata.brokers.len();
            if n > 0 { app.list_cursor = (app.list_cursor + 1).min(n - 1); }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Char('r') => {
            refresh_metadata(app).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn refresh_metadata(app: &mut App) -> Result<()> {
    if let Some(client) = &app.kafka_client {
        let cfg = client.config.clone();
        app.set_status("Refreshing…");
        match tokio::task::spawn_blocking(move || {
            fetch_cluster_metadata(&cfg, Duration::from_secs(8))
        }).await {
            Ok(Ok(meta)) => {
                let msg = format!(
                    "Refreshed — {} brokers, {} topics",
                    meta.brokers.len(),
                    meta.topics.iter().filter(|t| !t.is_internal).count()
                );
                app.metadata = meta;
                app.set_status(msg);
            }
            Ok(Err(e)) => app.set_error(format!("Refresh failed: {}", e)),
            Err(e) => app.set_error(format!("Task error: {}", e)),
        }
    }
    Ok(())
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

// ─── Editing mode ─────────────────────────────────────────────────────────────

async fn handle_editing(key: KeyEvent, app: &mut App) -> Result<()> {
    if app.current_view != View::ClusterForm {
        // Search/filter mode for topic list
        match key.code {
            KeyCode::Esc => {
                app.input_mode = InputMode::Normal;
                // Don't clear filter — just stop editing
            }
            KeyCode::Enter => {
                app.filter = app.search_input.clone();
                app.list_cursor = 0;
                app.input_mode = InputMode::Normal;
            }
            KeyCode::Backspace => { app.search_input.pop(); }
            KeyCode::Char(c) => app.search_input.push(c),
            _ => {}
        }
        return Ok(());
    }
    handle_form_editing(key, app).await
}

async fn handle_form_editing(key: KeyEvent, app: &mut App) -> Result<()> {
    let fields = app.cluster_form.fields();
    let max = fields.len().saturating_sub(1);
    let focused = app.cluster_form.focused_field_index.min(max);
    let current_field = fields[focused].clone();

    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.navigate_back();
        }
        KeyCode::Tab => {
            app.cluster_form.focused_field_index = (focused + 1).min(max);
        }
        KeyCode::BackTab => {
            app.cluster_form.focused_field_index = focused.saturating_sub(1);
        }
        KeyCode::Enter => {
            match &current_field {
                ClusterFormField::Submit => save_and_test(app).await?,
                ClusterFormField::AuthMechanism => {
                    let next = (app.cluster_form.auth_index + 1) % AUTH_MECHANISMS.len();
                    app.cluster_form.auth_index = next;
                    let new_max = ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1;
                    app.cluster_form.focused_field_index = focused.min(new_max);
                }
                ClusterFormField::VerifyHostname => {
                    app.cluster_form.verify_hostname = !app.cluster_form.verify_hostname;
                }
                _ => {
                    app.cluster_form.focused_field_index = (focused + 1).min(max);
                }
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            match &current_field {
                ClusterFormField::AuthMechanism => {
                    let next = (app.cluster_form.auth_index + 1).min(AUTH_MECHANISMS.len() - 1);
                    app.cluster_form.auth_index = next;
                    let new_max = ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1;
                    app.cluster_form.focused_field_index = focused.min(new_max);
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
                    let new_max = ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1;
                    app.cluster_form.focused_field_index = focused.min(new_max);
                }
                ClusterFormField::VerifyHostname => {
                    app.cluster_form.verify_hostname = true;
                }
                _ => {
                    app.cluster_form.focused_field_index = focused.saturating_sub(1);
                }
            }
        }
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

    app.set_status(format!("Testing connection to {}…", config.bootstrap_servers));
    match KafkaClient::new(&config) {
        Ok(client) => {
            let cfg = client.config.clone();
            match tokio::task::spawn_blocking(move || {
                fetch_cluster_metadata(&cfg, Duration::from_secs(8))
            }).await {
                Ok(Ok(meta)) => {
                    if let Some(idx) = app.cluster_form_edit_index {
                        app.profile_manager.profiles[idx] = config;
                    } else {
                        app.profile_manager.profiles.push(config);
                    }
                    app.profile_manager.save()?;
                    app.input_mode = InputMode::Normal;
                    app.navigate_back();
                    app.set_status(format!(
                        "✓ '{}' saved — {} broker(s), {} topics",
                        name,
                        meta.brokers.len(),
                        meta.topics.iter().filter(|t| !t.is_internal).count()
                    ));
                }
                Ok(Err(e)) => {
                    // Save anyway, warn about connection
                    if let Some(idx) = app.cluster_form_edit_index {
                        app.profile_manager.profiles[idx] = app.cluster_form.to_cluster_config();
                    } else {
                        app.profile_manager.profiles.push(app.cluster_form.to_cluster_config());
                    }
                    app.profile_manager.save()?;
                    app.input_mode = InputMode::Normal;
                    app.navigate_back();
                    app.set_status(format!("Saved '{}' (connection test: {})", name, e));
                }
                Err(e) => app.set_error(format!("Task error: {}", e)),
            }
        }
        Err(e) => app.set_error(format!("Client error: {}", e)),
    }
    Ok(())
}

// ─── Confirm mode ─────────────────────────────────────────────────────────────

fn handle_confirm(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            if let View::ClusterList = app.current_view {
                let idx = app.list_cursor;
                if let Err(e) = app.profile_manager.remove(idx) {
                    app.set_error(format!("Failed to delete: {}", e));
                } else {
                    app.list_cursor = app.list_cursor.saturating_sub(1);
                    app.set_status("Cluster deleted");
                }
            }
            app.input_mode = InputMode::Normal;
            app.status_message = None;
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.status_message = None;
        }
        _ => {}
    }
    Ok(())
}
