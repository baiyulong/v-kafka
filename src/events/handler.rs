use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use crate::app::{App, ClusterFormField, InputMode, MessageInput, ProducerForm, View, AUTH_MECHANISMS};
use crate::kafka::admin::{delete_acl, describe_acls};
use crate::kafka::client::KafkaClient;
use crate::kafka::consumer::{fetch_messages_blocking, fetch_messages_from_timestamp, PAGE_SIZE};
use crate::kafka::consumer_group::{
    fetch_group_offsets, list_consumer_groups, reset_group_offsets, OffsetReset,
};
use crate::kafka::metadata::{fetch_cluster_metadata, fetch_watermarks};
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;

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
        InputMode::Normal  => handle_normal(key, app).await,
        InputMode::Editing => handle_editing(key, app).await,
        InputMode::Confirm => handle_confirm(key, app).await,
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
        View::ClusterList         => handle_cluster_list(key, app).await,
        View::TopicList           => handle_topic_list(key, app).await,
        View::PartitionDetail     => handle_partition_detail(key, app).await,
        View::MessageBrowser      => handle_message_browser(key, app).await,
        View::MessageDetail       => { handle_message_detail(key, app); Ok(()) }
        View::ConsumerGroups      => handle_consumer_groups(key, app).await,
        View::ConsumerGroupDetail => handle_consumer_group_detail(key, app).await,
        View::ProducerForm        => { handle_producer_normal(key, app); Ok(()) }
        View::BrokerInfo          => handle_broker_info(key, app).await,
        View::AclManagement       => handle_acl_management(key, app).await,
        View::Help                => { app.navigate_back(); Ok(()) }
        _                         => Ok(()),
    }
}

// ─── Cluster list ─────────────────────────────────────────────────────────────

async fn handle_cluster_list(key: KeyEvent, app: &mut App) -> Result<()> {
    let count = app.profile_manager.profiles.len();
    match key.code {
        KeyCode::Down | KeyCode::Char('j') if count > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter if count > 0 => connect_to_cluster(app).await?,
        KeyCode::Char('n') => app.open_new_cluster_form(),
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
                Err(e) => app.set_error(format!("Connection failed: {}", e)),
            }
        }
        Err(e) => app.set_error(format!("Client init failed: {}", e)),
    }
    Ok(())
}

// ─── Topic list ──────────────────────────────────────────────────────────────

async fn handle_topic_list(key: KeyEvent, app: &mut App) -> Result<()> {
    let count = app.filtered_topics().len();

    match key.code {
        KeyCode::Down | KeyCode::Char('j') if count > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter if count > 0 => {
            let topic_name = app.filtered_topics()[app.list_cursor.min(count - 1)].name.clone();
            app.selected_topic = Some(topic_name.clone());
            app.watermarks.clear();
            if let Some(client) = &app.kafka_client {
                let cfg = client.config.clone();
                let tn = topic_name.clone();
                match tokio::task::spawn_blocking(move || {
                    fetch_watermarks(&cfg, &tn, Duration::from_secs(8))
                })
                .await
                {
                    Ok(Ok(wm)) => {
                        app.watermarks = wm;
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
        KeyCode::Char('g') => {
            app.navigate_to(View::ConsumerGroups);
            load_consumer_groups(app).await?;
        }
        KeyCode::Char('s') => app.navigate_to(View::SchemaRegistry),
        KeyCode::Char('a') => {
            app.navigate_to(View::AclManagement);
            load_acls(app).await?;
        }
        KeyCode::Char('p') => {
            // Pre-fill producer with selected topic
            if count > 0 {
                let topic = app.filtered_topics()[app.list_cursor.min(count - 1)].name.clone();
                app.producer_form = ProducerForm { topic, ..Default::default() };
            }
            app.navigate_to(View::ProducerForm);
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Char('r') => refresh_metadata(app).await?,
        KeyCode::Char('/') => {
            app.input_mode = InputMode::Editing;
            app.search_input = app.filter.clone();
        }
        KeyCode::Esc if !app.filter.is_empty() => app.filter.clear(),
        _ => {}
    }
    Ok(())
}

// ─── Partition detail ────────────────────────────────────────────────────────

async fn handle_partition_detail(key: KeyEvent, app: &mut App) -> Result<()> {
    let partitions = app.selected_topic_meta().map(|t| t.partitions.len()).unwrap_or(0);

    match key.code {
        KeyCode::Down | KeyCode::Char('j') if partitions > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(partitions - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter => {
            if let Some(p) = app.selected_topic_meta().and_then(|t| t.partitions.get(app.list_cursor)) {
                let partition_id = p.id;
                let high = app.watermarks.iter()
                    .find(|(id, _, _)| *id == partition_id)
                    .map(|(_, _, h)| *h)
                    .unwrap_or(0);
                app.selected_partition = Some(partition_id);
                // Start from max(0, high - PAGE_SIZE) so we see latest messages
                app.messages_start_offset = (high - PAGE_SIZE as i64).max(0);
                app.messages.clear();
                app.selected_message_idx = None;
                app.scroll_offset = 0;
            }
            app.navigate_to(View::MessageBrowser);
            load_messages(app).await?;
        }
        KeyCode::Char('r') => {
            if let (Some(client), Some(topic)) = (&app.kafka_client, &app.selected_topic) {
                let cfg = client.config.clone();
                let tn = topic.clone();
                match tokio::task::spawn_blocking(move || fetch_watermarks(&cfg, &tn, Duration::from_secs(8))).await {
                    Ok(Ok(wm)) => { app.watermarks = wm; app.set_status("Offsets refreshed"); }
                    Ok(Err(e)) => app.set_error(format!("Refresh failed: {}", e)),
                    Err(e) => app.set_error(format!("Task error: {}", e)),
                }
            }
        }
        _ => {}
    }
    Ok(())
}

// ─── Message browser ─────────────────────────────────────────────────────────

async fn handle_message_browser(key: KeyEvent, app: &mut App) -> Result<()> {
    let msg_count = app.messages.len();

    match key.code {
        KeyCode::Down | KeyCode::Char('j') if msg_count > 0 => {
            let idx = app.selected_message_idx.unwrap_or(0);
            app.selected_message_idx = Some((idx + 1).min(msg_count - 1));
        }
        KeyCode::Up | KeyCode::Char('k') => {
            let idx = app.selected_message_idx.unwrap_or(0);
            app.selected_message_idx = Some(idx.saturating_sub(1));
        }
        KeyCode::Enter if msg_count > 0 => {
            if app.selected_message_idx.is_none() {
                app.selected_message_idx = Some(0);
            }
            app.scroll_offset = 0;
            app.navigate_to(View::MessageDetail);
        }
        KeyCode::Char('r') => load_messages(app).await?,
        // Previous page
        KeyCode::Char('[') | KeyCode::Left => {
            let start = (app.messages_start_offset - PAGE_SIZE as i64).max(0);
            app.messages_start_offset = start;
            app.selected_message_idx = None;
            load_messages(app).await?;
        }
        // Next page
        KeyCode::Char(']') | KeyCode::Right => {
            let next = app.messages.last().map(|m| m.offset + 1).unwrap_or(app.messages_start_offset);
            app.messages_start_offset = next;
            app.selected_message_idx = None;
            load_messages(app).await?;
        }
        // Jump to offset
        KeyCode::Char('o') => {
            app.message_input = MessageInput::Offset;
            app.search_input.clear();
            app.input_mode = InputMode::Editing;
        }
        // Jump to timestamp
        KeyCode::Char('t') => {
            app.message_input = MessageInput::Timestamp;
            app.search_input.clear();
            app.input_mode = InputMode::Editing;
        }
        // Filter messages
        KeyCode::Char('/') => {
            app.message_input = MessageInput::Filter;
            app.search_input = app.filter.clone();
            app.input_mode = InputMode::Editing;
        }
        // Produce to this partition
        KeyCode::Char('p') => {
            let topic = app.selected_topic.clone().unwrap_or_default();
            let partition = app.selected_partition.map(|p| p.to_string()).unwrap_or_default();
            app.producer_form = ProducerForm { topic, partition, ..Default::default() };
            app.navigate_to(View::ProducerForm);
            app.input_mode = InputMode::Editing;
        }
        _ => {}
    }
    Ok(())
}

async fn load_messages(app: &mut App) -> Result<()> {
    let (topic, partition, start_offset) = match (&app.selected_topic, app.selected_partition) {
        (Some(t), Some(p)) => (t.clone(), p, app.messages_start_offset),
        _ => return Ok(()),
    };
    let client_cfg = match &app.kafka_client {
        Some(c) => c.config.clone(),
        None => return Ok(()),
    };

    app.messages_loading = true;
    app.set_status(format!("Loading messages from offset {}…", start_offset));

    let high = app.watermarks.iter()
        .find(|(id, _, _)| *id == partition)
        .map(|(_, _, h)| *h)
        .unwrap_or(0);

    let result = tokio::task::spawn_blocking(move || {
        fetch_messages_blocking(&client_cfg, &topic, partition, start_offset, PAGE_SIZE, high)
    })
    .await;

    app.messages_loading = false;
    match result {
        Ok(Ok(msgs)) => {
            app.set_status(format!("Loaded {} messages", msgs.len()));
            if app.selected_message_idx.is_none() && !msgs.is_empty() {
                app.selected_message_idx = Some(0);
            }
            app.messages = msgs;
        }
        Ok(Err(e)) => app.set_error(format!("Message load failed: {}", e)),
        Err(e) => app.set_error(format!("Task error: {}", e)),
    }
    Ok(())
}

// ─── Message detail ──────────────────────────────────────────────────────────

fn handle_message_detail(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => app.scroll_offset = app.scroll_offset.saturating_add(1),
        KeyCode::Up | KeyCode::Char('k')   => app.scroll_offset = app.scroll_offset.saturating_sub(1),
        _ => {}
    }
}

// ─── Consumer groups ─────────────────────────────────────────────────────────

async fn handle_consumer_groups(key: KeyEvent, app: &mut App) -> Result<()> {
    let count = app.consumer_groups.len();
    match key.code {
        KeyCode::Down | KeyCode::Char('j') if count > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Enter if count > 0 => {
            let group = app.consumer_groups[app.list_cursor.min(count - 1)].group_id.clone();
            app.selected_group = Some(group.clone());
            app.consumer_group_offsets.clear();
            app.navigate_to(View::ConsumerGroupDetail);
            // Load offsets for all known topic-partitions
            load_group_offsets(app).await?;
        }
        KeyCode::Char('r') => load_consumer_groups(app).await?,
        _ => {}
    }
    Ok(())
}

async fn handle_consumer_group_detail(key: KeyEvent, app: &mut App) -> Result<()> {
    let count = app.consumer_group_offsets.len();
    match key.code {
        KeyCode::Down | KeyCode::Char('j') if count > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Char('r') => load_group_offsets(app).await?,
        KeyCode::Char('R') => {
            // Reset to earliest
            reset_offsets_action(app, OffsetReset::Earliest).await?;
        }
        KeyCode::Char('L') => {
            // Reset to latest
            reset_offsets_action(app, OffsetReset::Latest).await?;
        }
        _ => {}
    }
    Ok(())
}

async fn load_consumer_groups(app: &mut App) -> Result<()> {
    let cfg = match &app.kafka_client {
        Some(c) => c.config.clone(),
        None => return Ok(()),
    };
    app.consumer_groups_loading = true;
    app.set_status("Loading consumer groups…");
    let result = tokio::task::spawn_blocking(move || list_consumer_groups(&cfg)).await;
    app.consumer_groups_loading = false;
    match result {
        Ok(Ok(groups)) => {
            app.set_status(format!("Loaded {} consumer groups", groups.len()));
            app.consumer_groups = groups;
        }
        Ok(Err(e)) => app.set_error(format!("Failed to list groups: {}", e)),
        Err(e) => app.set_error(format!("Task error: {}", e)),
    }
    Ok(())
}

async fn load_group_offsets(app: &mut App) -> Result<()> {
    let group_id = match &app.selected_group {
        Some(g) => g.clone(),
        None => return Ok(()),
    };
    let cfg = match &app.kafka_client {
        Some(c) => c.config.clone(),
        None => return Ok(()),
    };
    // Collect all partitions from cached metadata
    let partitions: Vec<(String, i32)> = app.metadata.topics.iter()
        .filter(|t| !t.is_internal)
        .flat_map(|t| t.partitions.iter().map(|p| (t.name.clone(), p.id)))
        .collect();

    if partitions.is_empty() {
        app.set_status("No topics in metadata — connect first");
        return Ok(());
    }

    app.group_offsets_loading = true;
    app.set_status(format!("Loading offsets for {}…", group_id));
    let result = tokio::task::spawn_blocking(move || {
        fetch_group_offsets(&cfg, &group_id, &partitions)
    }).await;
    app.group_offsets_loading = false;
    match result {
        Ok(Ok(offsets)) => {
            // Only keep partitions with committed or non-zero high watermark
            app.consumer_group_offsets = offsets.into_iter()
                .filter(|o| o.committed_offset >= 0 || o.high_watermark > 0)
                .collect();
            let total_lag: i64 = app.consumer_group_offsets.iter().map(|o| o.lag()).sum();
            app.set_status(format!(
                "{} partitions loaded, total lag: {}",
                app.consumer_group_offsets.len(), total_lag
            ));
        }
        Ok(Err(e)) => app.set_error(format!("Failed to fetch offsets: {}", e)),
        Err(e) => app.set_error(format!("Task error: {}", e)),
    }
    Ok(())
}

async fn reset_offsets_action(app: &mut App, reset_to: OffsetReset) -> Result<()> {
    let group_id = match &app.selected_group {
        Some(g) => g.clone(),
        None => return Ok(()),
    };
    let cfg = match &app.kafka_client {
        Some(c) => c.config.clone(),
        None => return Ok(()),
    };
    let partitions: Vec<(String, i32)> = app.consumer_group_offsets.iter()
        .map(|o| (o.topic.clone(), o.partition))
        .collect();
    let label = match reset_to { OffsetReset::Earliest => "earliest", OffsetReset::Latest => "latest", _ => "?" };
    app.set_status(format!("Resetting {} to {}…", group_id, label));
    let result = tokio::task::spawn_blocking(move || {
        reset_group_offsets(&cfg, &group_id, &partitions, reset_to)
    }).await;
    match result {
        Ok(Ok(())) => {
            app.set_status(format!("Reset complete — reloading offsets…"));
            load_group_offsets(app).await?;
        }
        Ok(Err(e)) => app.set_error(format!("Reset failed: {}", e)),
        Err(e) => app.set_error(format!("Task error: {}", e)),
    }
    Ok(())
}

// ─── Broker info ─────────────────────────────────────────────────────────────

async fn handle_broker_info(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Down | KeyCode::Char('j') => {
            let n = app.metadata.brokers.len();
            if n > 0 { app.list_cursor = (app.list_cursor + 1).min(n - 1); }
        }
        KeyCode::Up | KeyCode::Char('k') => app.list_cursor = app.list_cursor.saturating_sub(1),
        KeyCode::Char('r') => refresh_metadata(app).await?,
        _ => {}
    }
    Ok(())
}

// ─── ACL management ──────────────────────────────────────────────────────────

async fn handle_acl_management(key: KeyEvent, app: &mut App) -> Result<()> {
    let count = app.acl_list.len();
    match key.code {
        KeyCode::Down | KeyCode::Char('j') if count > 0 => {
            app.list_cursor = (app.list_cursor + 1).min(count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.list_cursor = app.list_cursor.saturating_sub(1);
        }
        KeyCode::Char('r') => load_acls(app).await?,
        KeyCode::Char('d') if count > 0 => {
            app.input_mode = InputMode::Confirm;
            let acl = &app.acl_list[app.list_cursor];
            app.set_status(format!(
                "Delete ACL {} {} {}? (y/n)",
                acl.resource_type, acl.name, acl.operation
            ));
        }
        _ => {}
    }
    Ok(())
}

async fn load_acls(app: &mut App) -> Result<()> {
    let cfg = match &app.kafka_client {
        Some(c) => c.config.clone(),
        None => return Ok(()),
    };
    app.acl_loading = true;
    app.set_status("Loading ACLs…");
    let admin: AdminClient<DefaultClientContext> = cfg.create()?;
    match describe_acls(&admin).await {
        Ok(acls) => {
            app.set_status(format!("Loaded {} ACL entries", acls.len()));
            app.acl_list = acls;
        }
        Err(e) => app.set_error(format!("ACL fetch failed: {}", e)),
    }
    app.acl_loading = false;
    Ok(())
}

// ─── Producer form ────────────────────────────────────────────────────────────

fn handle_producer_normal(key: KeyEvent, app: &mut App) {
    match key.code {
        KeyCode::Enter | KeyCode::Char('i') => {
            app.input_mode = InputMode::Editing;
        }
        KeyCode::Esc => app.navigate_back(),
        _ => {}
    }
}

// ─── Metadata refresh ─────────────────────────────────────────────────────────

async fn refresh_metadata(app: &mut App) -> Result<()> {
    if let Some(client) = &app.kafka_client {
        let cfg = client.config.clone();
        app.set_status("Refreshing…");
        match tokio::task::spawn_blocking(move || fetch_cluster_metadata(&cfg, Duration::from_secs(8))).await {
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

// ─── Editing mode ─────────────────────────────────────────────────────────────

async fn handle_editing(key: KeyEvent, app: &mut App) -> Result<()> {
    match &app.current_view {
        View::ClusterForm  => handle_form_editing(key, app).await,
        View::ProducerForm => handle_producer_editing(key, app).await,
        View::MessageBrowser => handle_message_input(key, app).await,
        _ => {
            // Generic topic/group filter
            match key.code {
                KeyCode::Esc => { app.input_mode = InputMode::Normal; }
                KeyCode::Enter => {
                    app.filter = app.search_input.clone();
                    app.list_cursor = 0;
                    app.input_mode = InputMode::Normal;
                }
                KeyCode::Backspace => { app.search_input.pop(); }
                KeyCode::Char(c) => app.search_input.push(c),
                _ => {}
            }
            Ok(())
        }
    }
}

// ─── Cluster form editing ─────────────────────────────────────────────────────

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
        KeyCode::Enter => match &current_field {
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
            _ => { app.cluster_form.focused_field_index = (focused + 1).min(max); }
        },
        KeyCode::Down | KeyCode::Char('j') => match &current_field {
            ClusterFormField::AuthMechanism => {
                let next = (app.cluster_form.auth_index + 1).min(AUTH_MECHANISMS.len() - 1);
                app.cluster_form.auth_index = next;
                let new_max = ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1;
                app.cluster_form.focused_field_index = focused.min(new_max);
            }
            ClusterFormField::VerifyHostname => app.cluster_form.verify_hostname = false,
            _ => { app.cluster_form.focused_field_index = (focused + 1).min(max); }
        },
        KeyCode::Up | KeyCode::Char('k') => match &current_field {
            ClusterFormField::AuthMechanism => {
                app.cluster_form.auth_index = app.cluster_form.auth_index.saturating_sub(1);
                let new_max = ClusterFormField::fields_for(app.cluster_form.current_auth()).len() - 1;
                app.cluster_form.focused_field_index = focused.min(new_max);
            }
            ClusterFormField::VerifyHostname => app.cluster_form.verify_hostname = true,
            _ => { app.cluster_form.focused_field_index = focused.saturating_sub(1); }
        },
        KeyCode::Backspace => {
            if let Some(s) = app.cluster_form.focused_string_mut() { s.pop(); }
        }
        KeyCode::Char(c) => {
            if let Some(s) = app.cluster_form.focused_string_mut() { s.push(c); }
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
            match tokio::task::spawn_blocking(move || fetch_cluster_metadata(&cfg, Duration::from_secs(8))).await {
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
                        name, meta.brokers.len(),
                        meta.topics.iter().filter(|t| !t.is_internal).count()
                    ));
                }
                Ok(Err(e)) => {
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

// ─── Producer form editing ─────────────────────────────────────────────────────

async fn handle_producer_editing(key: KeyEvent, app: &mut App) -> Result<()> {
    let field_count = ProducerForm::FIELDS.len();
    let focused = app.producer_form.focused_field;

    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
            app.navigate_back();
        }
        KeyCode::Tab => {
            app.producer_form.focused_field = (focused + 1) % field_count;
        }
        KeyCode::BackTab => {
            app.producer_form.focused_field = if focused == 0 { field_count - 1 } else { focused - 1 };
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.producer_form.focused_field = (focused + 1).min(field_count - 1);
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.producer_form.focused_field = focused.saturating_sub(1);
        }
        KeyCode::Enter => {
            if focused == field_count - 1 {
                // Send button
                send_message_action(app).await?;
            } else {
                app.producer_form.focused_field = (focused + 1).min(field_count - 1);
            }
        }
        KeyCode::Backspace => {
            if let Some(s) = app.producer_form.current_str_mut() { s.pop(); }
        }
        KeyCode::Char(c) => {
            if let Some(s) = app.producer_form.current_str_mut() { s.push(c); }
        }
        _ => {}
    }
    Ok(())
}

async fn send_message_action(app: &mut App) -> Result<()> {
    let topic = app.producer_form.topic.trim().to_string();
    if topic.is_empty() {
        app.set_error("Topic is required");
        return Ok(());
    }
    let value = app.producer_form.value.clone();
    let key = app.producer_form.key.clone();
    let partition_str = app.producer_form.partition.trim().to_string();
    let headers_str = app.producer_form.headers.clone();
    let cfg = match &app.kafka_client {
        Some(c) => c.config.clone(),
        None => { app.set_error("Not connected"); return Ok(()); }
    };

    let partition: Option<i32> = partition_str.parse().ok();
    let key_bytes = if key.is_empty() { None } else { Some(key.into_bytes()) };
    let headers: Vec<(String, Vec<u8>)> = headers_str.split(',')
        .filter_map(|h| {
            let mut parts = h.splitn(2, '=');
            let k = parts.next()?.trim().to_string();
            let v = parts.next().unwrap_or("").trim().as_bytes().to_vec();
            if k.is_empty() { None } else { Some((k, v)) }
        })
        .collect();

    app.set_status("Sending…");
    let value_bytes = value.into_bytes();
    let key_ref = key_bytes.clone();

    let result = tokio::spawn(async move {
        use rdkafka::producer::{FutureProducer, FutureRecord};
        use rdkafka::message::OwnedHeaders;
        use std::time::Duration;

        let producer: FutureProducer = cfg.create()?;
        let mut record = FutureRecord::to(&topic).payload(value_bytes.as_slice());
        if let Some(k) = &key_ref {
            record = record.key(k.as_slice());
        }
        if let Some(p) = partition {
            record = record.partition(p);
        }
        let mut owned_headers = OwnedHeaders::new();
        for (k, v) in &headers {
            owned_headers = owned_headers.insert(rdkafka::message::Header { key: k, value: Some(v.as_slice()) });
        }
        record = record.headers(owned_headers);
        let (p, o) = producer.send(record, Duration::from_secs(10))
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Send failed: {}", e))?;
        Ok::<(i32, i64), anyhow::Error>((p, o))
    }).await;

    match result {
        Ok(Ok((p, o))) => {
            let msg = format!("✓ Sent to partition {} offset {}", p, o);
            app.producer_form.last_result = Some(msg.clone());
            app.set_status(msg);
        }
        Ok(Err(e)) => {
            let msg = format!("✗ {}", e);
            app.producer_form.last_result = Some(msg.clone());
            app.set_error(msg);
        }
        Err(e) => app.set_error(format!("Task error: {}", e)),
    }
    Ok(())
}

// ─── Message browser input (offset/timestamp/filter) ─────────────────────────

async fn handle_message_input(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Esc => {
            app.message_input = MessageInput::None;
            app.input_mode = InputMode::Normal;
            app.search_input.clear();
        }
        KeyCode::Enter => {
            let input = app.search_input.trim().to_string();
            let mode = app.message_input.clone();
            app.message_input = MessageInput::None;
            app.input_mode = InputMode::Normal;
            app.search_input.clear();

            match mode {
                MessageInput::Offset => {
                    if let Ok(offset) = input.parse::<i64>() {
                        app.messages_start_offset = offset;
                        app.selected_message_idx = None;
                        load_messages(app).await?;
                    } else {
                        app.set_error(format!("Invalid offset: {}", input));
                    }
                }
                MessageInput::Timestamp => {
                    if let Ok(ts) = input.parse::<i64>() {
                        let (topic, partition) = match (&app.selected_topic, app.selected_partition) {
                            (Some(t), Some(p)) => (t.clone(), p),
                            _ => return Ok(()),
                        };
                        let cfg = match &app.kafka_client {
                            Some(c) => c.config.clone(),
                            None => return Ok(()),
                        };
                        app.set_status(format!("Seeking to timestamp {}…", ts));
                        let result = tokio::task::spawn_blocking(move || {
                            fetch_messages_from_timestamp(&cfg, &topic, partition, ts, PAGE_SIZE)
                        }).await;
                        match result {
                            Ok(Ok(msgs)) => {
                                app.set_status(format!("Loaded {} messages from timestamp", msgs.len()));
                                if !msgs.is_empty() {
                                    app.messages_start_offset = msgs[0].offset;
                                }
                                app.messages = msgs;
                                app.selected_message_idx = if app.messages.is_empty() { None } else { Some(0) };
                            }
                            Ok(Err(e)) => app.set_error(format!("Timestamp seek failed: {}", e)),
                            Err(e) => app.set_error(format!("Task error: {}", e)),
                        }
                    } else {
                        app.set_error("Enter Unix timestamp in milliseconds");
                    }
                }
                MessageInput::Filter => {
                    app.filter = input;
                    app.list_cursor = 0;
                }
                MessageInput::None => {}
            }
        }
        KeyCode::Backspace => { app.search_input.pop(); }
        KeyCode::Char(c) => app.search_input.push(c),
        _ => {}
    }
    Ok(())
}

// ─── Confirm mode ─────────────────────────────────────────────────────────────

async fn handle_confirm(key: KeyEvent, app: &mut App) -> Result<()> {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            let view = app.current_view.clone();
            match view {
                View::ClusterList => {
                    let idx = app.list_cursor;
                    if let Err(e) = app.profile_manager.remove(idx) {
                        app.set_error(format!("Failed to delete: {}", e));
                    } else {
                        app.list_cursor = app.list_cursor.saturating_sub(1);
                        app.set_status("Cluster deleted");
                    }
                }
                View::AclManagement => {
                    if let Some(acl) = app.acl_list.get(app.list_cursor).cloned() {
                        let cfg = match &app.kafka_client {
                            Some(c) => c.config.clone(),
                            None => { app.set_error("Not connected"); app.input_mode = InputMode::Normal; return Ok(()); }
                        };
                        app.set_status("Deleting ACL…");
                        let admin: AdminClient<DefaultClientContext> = cfg.create()?;
                        match delete_acl(&admin, &acl).await {
                            Ok(()) => {
                                app.acl_list.remove(app.list_cursor);
                                app.list_cursor = app.list_cursor.saturating_sub(1);
                                app.set_status("ACL deleted");
                            }
                            Err(e) => app.set_error(format!("Delete ACL failed: {}", e)),
                        }
                    }
                }
                _ => {}
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
