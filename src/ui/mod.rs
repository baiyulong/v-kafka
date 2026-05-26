pub mod components;
pub mod layout;
pub mod theme;

use ratatui::Frame;
use crate::app::{App, View};

pub fn render(f: &mut Frame, app: &App) {
    let chunks = layout::build_layout(f.area());

    // Title bar
    components::title_bar::render(f, chunks[0], app);

    // Main content area
    match &app.current_view {
        View::ClusterList => components::cluster_list::render(f, chunks[1], app),
        View::ClusterForm => components::cluster_form::render(f, chunks[1], app),
        View::BrokerInfo => components::broker_info::render(f, chunks[1], app),
        View::TopicList => components::topic_list::render(f, chunks[1], app),
        View::PartitionDetail => components::partition_view::render(f, chunks[1], app),
        View::MessageBrowser => components::message_list::render(f, chunks[1], app),
        View::MessageDetail => components::message_detail::render(f, chunks[1], app),
        View::ConsumerGroups => components::consumer_groups::render(f, chunks[1], app),
        View::ConsumerGroupDetail => components::consumer_groups::render_detail(f, chunks[1], app),
        View::ProducerForm => components::producer_form::render(f, chunks[1], app),
        View::SchemaRegistry => components::schema_registry::render(f, chunks[1], app),
        View::AclManagement => components::acl_view::render(f, chunks[1], app),
        View::Help => components::help::render(f, f.area()),
    }

    // Status bar
    components::status_bar::render(f, chunks[2], app);

    // Error popup overlay
    if app.error_message.is_some() {
        components::dialog::render_error(f, f.area(), app);
    }
}
