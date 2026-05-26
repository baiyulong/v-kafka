use anyhow::Result;
use crate::config::profile::{ClusterProfile, ProfileManager};

/// Top-level view/screen of the application
#[derive(Debug, Clone, PartialEq)]
pub enum View {
    /// Connection/cluster selection screen
    ClusterList,
    /// Broker information for the connected cluster
    BrokerInfo,
    /// Topic list
    TopicList,
    /// Partition detail for a selected topic
    PartitionDetail,
    /// Message browser (Offset Explorer core)
    MessageBrowser,
    /// Message detail
    MessageDetail,
    /// Consumer group list
    ConsumerGroups,
    /// Consumer group partition detail
    ConsumerGroupDetail,
    /// Producer form
    ProducerForm,
    /// Schema Registry browser
    SchemaRegistry,
    /// Schema detail
    SchemaDetail,
    /// ACL management
    AclManagement,
    /// Help overlay
    Help,
}

/// Input mode determines how keyboard events are routed
#[derive(Debug, Clone, PartialEq)]
pub enum InputMode {
    Normal,
    /// Text input is active (search, form fields, etc.)
    Editing,
    /// A confirmation dialog is open
    Confirm,
}

/// Global application state
pub struct App {
    /// Whether the app should exit on next loop iteration
    pub should_quit: bool,
    /// Current view being rendered
    pub current_view: View,
    /// Previous view stack for back-navigation
    pub view_stack: Vec<View>,
    /// Current input mode
    pub input_mode: InputMode,
    /// Cluster profile manager
    pub profile_manager: ProfileManager,
    /// Currently connected cluster (if any)
    pub active_cluster: Option<ClusterProfile>,
    /// Cursor/selection index for the current list view
    pub list_cursor: usize,
    /// Scroll offset for message/content panels
    pub scroll_offset: u64,
    /// Current search/filter string
    pub search_input: String,
    /// Status bar message (shown at bottom)
    pub status_message: Option<String>,
    /// Error message (shown in a popup)
    pub error_message: Option<String>,
    /// Selected topic name
    pub selected_topic: Option<String>,
    /// Selected partition id
    pub selected_partition: Option<i32>,
    /// Selected consumer group id
    pub selected_group: Option<String>,
}

impl App {
    pub async fn new() -> Result<Self> {
        let profile_manager = ProfileManager::load()?;
        Ok(Self {
            should_quit: false,
            current_view: View::ClusterList,
            view_stack: Vec::new(),
            input_mode: InputMode::Normal,
            profile_manager,
            active_cluster: None,
            list_cursor: 0,
            scroll_offset: 0,
            search_input: String::new(),
            status_message: None,
            error_message: None,
            selected_topic: None,
            selected_partition: None,
            selected_group: None,
        })
    }

    /// Navigate to a new view, pushing current view onto the stack
    pub fn navigate_to(&mut self, view: View) {
        let prev = std::mem::replace(&mut self.current_view, view);
        self.view_stack.push(prev);
        self.list_cursor = 0;
        self.scroll_offset = 0;
    }

    /// Navigate back to the previous view
    pub fn navigate_back(&mut self) {
        if let Some(prev) = self.view_stack.pop() {
            self.current_view = prev;
            self.list_cursor = 0;
            self.scroll_offset = 0;
        }
    }

    /// Set a transient status bar message
    pub fn set_status(&mut self, msg: impl Into<String>) {
        self.status_message = Some(msg.into());
    }

    /// Set an error popup message
    pub fn set_error(&mut self, msg: impl Into<String>) {
        self.error_message = Some(msg.into());
    }

    /// Called each tick; clears transient messages after a while (handled via
    /// tick count in a real implementation)
    pub async fn on_tick(&mut self) {
        // Status message auto-clear is handled by tick counting in a real impl
    }
}
