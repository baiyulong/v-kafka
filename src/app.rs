use crate::config::cluster::{
    AuthMechanism, ClusterConfig, SaslConfig, SchemaRegistryConfig, SslConfig,
};
use crate::config::profile::{ClusterProfile, ProfileManager};
use crate::kafka::admin::AclEntry;
use crate::kafka::client::KafkaClient;
use crate::kafka::consumer::KafkaMessage;
use crate::kafka::consumer_group::{GroupInfo, GroupPartitionOffset};
use crate::kafka::metadata::CachedMetadata;
use crate::kafka::schema_registry::{SchemaDetail, SchemaRegistryClient};
use anyhow::Result;

/// Top-level view/screen of the application
#[derive(Debug, Clone, PartialEq)]
pub enum View {
    ClusterList,
    ClusterForm,
    BrokerInfo,
    TopicList,
    PartitionDetail,
    MessageBrowser,
    MessageDetail,
    ConsumerGroups,
    ConsumerGroupDetail,
    ProducerForm,
    SchemaRegistry,
    AclManagement,
    Help,
}

/// What the user is currently typing in the message browser
#[derive(Debug, Clone, PartialEq, Default)]
pub enum MessageInput {
    #[default]
    None,
    Offset,
    Timestamp,
    Filter,
}

/// Producer form state
#[derive(Debug, Default, Clone)]
pub struct ProducerForm {
    pub topic: String,
    pub partition: String,
    pub key: String,
    pub value: String,
    pub headers: String,
    pub focused_field: usize,
    pub last_result: Option<String>,
}

impl ProducerForm {
    pub const FIELDS: &'static [&'static str] = &[
        "Topic",
        "Partition (empty=auto)",
        "Key",
        "Value",
        "Headers (k=v,k2=v2)",
        "[ Send ]",
    ];

    pub fn current_str_mut(&mut self) -> Option<&mut String> {
        match self.focused_field {
            0 => Some(&mut self.topic),
            1 => Some(&mut self.partition),
            2 => Some(&mut self.key),
            3 => Some(&mut self.value),
            4 => Some(&mut self.headers),
            _ => None,
        }
    }

    pub fn field_value(&self, idx: usize) -> String {
        match idx {
            0 => self.topic.clone(),
            1 => self.partition.clone(),
            2 => self.key.clone(),
            3 => self.value.clone(),
            4 => self.headers.clone(),
            5 => String::new(),
            _ => String::new(),
        }
    }
}

/// Input mode determines how keyboard events are routed
#[derive(Debug, Clone, PartialEq)]
pub enum InputMode {
    Normal,
    Editing,
    Confirm,
}

/// Which field is focused in the cluster creation form
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterFormField {
    Name,
    BootstrapServers,
    AuthMechanism,
    // SSL fields
    CaCert,
    ClientCert,
    ClientKey,
    ClientKeyPassword,
    VerifyHostname,
    // SASL fields
    SaslUsername,
    SaslPassword,
    // Kerberos
    KerberosPrincipal,
    KerberosKeytab,
    KerberosServiceName,
    // Schema Registry
    SchemaRegistryUrl,
    SchemaRegistryUser,
    SchemaRegistryPassword,
    // Done
    Submit,
}

impl ClusterFormField {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Name => "Name",
            Self::BootstrapServers => "Bootstrap Servers",
            Self::AuthMechanism => "Auth Mechanism",
            Self::CaCert => "CA Certificate Path",
            Self::ClientCert => "Client Certificate Path",
            Self::ClientKey => "Client Key Path",
            Self::ClientKeyPassword => "Client Key Password",
            Self::VerifyHostname => "Verify Hostname (y/n)",
            Self::SaslUsername => "SASL Username",
            Self::SaslPassword => "SASL Password",
            Self::KerberosPrincipal => "Kerberos Principal",
            Self::KerberosKeytab => "Kerberos Keytab Path",
            Self::KerberosServiceName => "Kerberos Service Name",
            Self::SchemaRegistryUrl => "Schema Registry URL (optional)",
            Self::SchemaRegistryUser => "Schema Registry Username",
            Self::SchemaRegistryPassword => "Schema Registry Password",
            Self::Submit => "[ Save & Test Connection ]",
        }
    }

    /// Return the ordered list of fields for a given auth mechanism
    pub fn fields_for(auth: &AuthMechanism) -> Vec<ClusterFormField> {
        let mut fields = vec![Self::Name, Self::BootstrapServers, Self::AuthMechanism];
        match auth {
            AuthMechanism::Plaintext => {}
            AuthMechanism::Ssl => {
                fields.extend([
                    Self::CaCert,
                    Self::ClientCert,
                    Self::ClientKey,
                    Self::ClientKeyPassword,
                    Self::VerifyHostname,
                ]);
            }
            AuthMechanism::SaslPlain
            | AuthMechanism::SaslScramSha256
            | AuthMechanism::SaslScramSha512 => {
                fields.extend([Self::SaslUsername, Self::SaslPassword]);
                if matches!(
                    auth,
                    AuthMechanism::SaslScramSha256 | AuthMechanism::SaslScramSha512
                ) {
                    fields.extend([Self::CaCert, Self::VerifyHostname]);
                }
            }
            AuthMechanism::Kerberos => {
                fields.extend([
                    Self::KerberosPrincipal,
                    Self::KerberosKeytab,
                    Self::KerberosServiceName,
                ]);
            }
        }
        fields.extend([Self::SchemaRegistryUrl, Self::Submit]);
        fields
    }
}

/// Form state for creating/editing a cluster connection
#[derive(Debug, Default)]
pub struct ClusterForm {
    pub name: String,
    pub bootstrap_servers: String,
    pub auth_index: usize, // index into AUTH_MECHANISMS
    pub ca_cert: String,
    pub client_cert: String,
    pub client_key: String,
    pub client_key_password: String,
    pub verify_hostname: bool,
    pub sasl_username: String,
    pub sasl_password: String,
    pub kerberos_principal: String,
    pub kerberos_keytab: String,
    pub kerberos_service_name: String,
    pub schema_registry_url: String,
    pub schema_registry_user: String,
    pub schema_registry_password: String,
    /// Index into fields_for(current auth)
    pub focused_field_index: usize,
    /// Edit state for the auth mechanism selector
    pub editing_auth: bool,
}

pub const AUTH_MECHANISMS: &[AuthMechanism] = &[
    AuthMechanism::Plaintext,
    AuthMechanism::Ssl,
    AuthMechanism::SaslPlain,
    AuthMechanism::SaslScramSha256,
    AuthMechanism::SaslScramSha512,
    AuthMechanism::Kerberos,
];

impl ClusterForm {
    pub fn current_auth(&self) -> &AuthMechanism {
        &AUTH_MECHANISMS[self.auth_index]
    }

    pub fn fields(&self) -> Vec<ClusterFormField> {
        ClusterFormField::fields_for(self.current_auth())
    }

    pub fn current_field(&self) -> ClusterFormField {
        let fields = self.fields();
        fields[self.focused_field_index.min(fields.len() - 1)].clone()
    }

    pub fn field_value(&self, field: &ClusterFormField) -> String {
        match field {
            ClusterFormField::Name => self.name.clone(),
            ClusterFormField::BootstrapServers => self.bootstrap_servers.clone(),
            ClusterFormField::AuthMechanism => self.auth_label(),
            ClusterFormField::CaCert => self.ca_cert.clone(),
            ClusterFormField::ClientCert => self.client_cert.clone(),
            ClusterFormField::ClientKey => self.client_key.clone(),
            ClusterFormField::ClientKeyPassword => "*".repeat(self.client_key_password.len()),
            ClusterFormField::VerifyHostname => {
                if self.verify_hostname {
                    "yes".into()
                } else {
                    "no".into()
                }
            }
            ClusterFormField::SaslUsername => self.sasl_username.clone(),
            ClusterFormField::SaslPassword => "*".repeat(self.sasl_password.len()),
            ClusterFormField::KerberosPrincipal => self.kerberos_principal.clone(),
            ClusterFormField::KerberosKeytab => self.kerberos_keytab.clone(),
            ClusterFormField::KerberosServiceName => self.kerberos_service_name.clone(),
            ClusterFormField::SchemaRegistryUrl => self.schema_registry_url.clone(),
            ClusterFormField::SchemaRegistryUser => self.schema_registry_user.clone(),
            ClusterFormField::SchemaRegistryPassword => {
                "*".repeat(self.schema_registry_password.len())
            }
            ClusterFormField::Submit => String::new(),
        }
    }

    /// Get the mutable string for the currently focused text field
    pub fn focused_string_mut(&mut self) -> Option<&mut String> {
        let field = self.current_field().clone();
        match field {
            ClusterFormField::Name => Some(&mut self.name),
            ClusterFormField::BootstrapServers => Some(&mut self.bootstrap_servers),
            ClusterFormField::CaCert => Some(&mut self.ca_cert),
            ClusterFormField::ClientCert => Some(&mut self.client_cert),
            ClusterFormField::ClientKey => Some(&mut self.client_key),
            ClusterFormField::ClientKeyPassword => Some(&mut self.client_key_password),
            ClusterFormField::SaslUsername => Some(&mut self.sasl_username),
            ClusterFormField::SaslPassword => Some(&mut self.sasl_password),
            ClusterFormField::KerberosPrincipal => Some(&mut self.kerberos_principal),
            ClusterFormField::KerberosKeytab => Some(&mut self.kerberos_keytab),
            ClusterFormField::KerberosServiceName => Some(&mut self.kerberos_service_name),
            ClusterFormField::SchemaRegistryUrl => Some(&mut self.schema_registry_url),
            ClusterFormField::SchemaRegistryUser => Some(&mut self.schema_registry_user),
            ClusterFormField::SchemaRegistryPassword => Some(&mut self.schema_registry_password),
            _ => None,
        }
    }

    pub fn auth_label(&self) -> String {
        match self.current_auth() {
            AuthMechanism::Plaintext => "PLAINTEXT".into(),
            AuthMechanism::Ssl => "SSL/TLS".into(),
            AuthMechanism::SaslPlain => "SASL/PLAIN".into(),
            AuthMechanism::SaslScramSha256 => "SASL/SCRAM-SHA-256".into(),
            AuthMechanism::SaslScramSha512 => "SASL/SCRAM-SHA-512".into(),
            AuthMechanism::Kerberos => "Kerberos (GSSAPI)".into(),
        }
    }

    /// Build a ClusterConfig from the form values
    pub fn to_cluster_config(&self) -> ClusterConfig {
        let schema_registry = if self.schema_registry_url.trim().is_empty() {
            None
        } else {
            Some(SchemaRegistryConfig {
                url: self.schema_registry_url.trim().to_string(),
                username: if self.schema_registry_user.is_empty() {
                    None
                } else {
                    Some(self.schema_registry_user.clone())
                },
                password: if self.schema_registry_password.is_empty() {
                    None
                } else {
                    Some(self.schema_registry_password.clone())
                },
            })
        };

        ClusterConfig {
            name: self.name.trim().to_string(),
            bootstrap_servers: self.bootstrap_servers.trim().to_string(),
            auth: self.current_auth().clone(),
            ssl: SslConfig {
                ca_cert_path: if self.ca_cert.is_empty() {
                    None
                } else {
                    Some(self.ca_cert.clone())
                },
                client_cert_path: if self.client_cert.is_empty() {
                    None
                } else {
                    Some(self.client_cert.clone())
                },
                client_key_path: if self.client_key.is_empty() {
                    None
                } else {
                    Some(self.client_key.clone())
                },
                client_key_password: if self.client_key_password.is_empty() {
                    None
                } else {
                    Some(self.client_key_password.clone())
                },
                verify_hostname: self.verify_hostname,
            },
            sasl: SaslConfig {
                username: if self.sasl_username.is_empty() {
                    None
                } else {
                    Some(self.sasl_username.clone())
                },
                password: if self.sasl_password.is_empty() {
                    None
                } else {
                    Some(self.sasl_password.clone())
                },
                kerberos_principal: if self.kerberos_principal.is_empty() {
                    None
                } else {
                    Some(self.kerberos_principal.clone())
                },
                kerberos_keytab: if self.kerberos_keytab.is_empty() {
                    None
                } else {
                    Some(self.kerberos_keytab.clone())
                },
                kerberos_service_name: if self.kerberos_service_name.is_empty() {
                    None
                } else {
                    Some(self.kerberos_service_name.clone())
                },
            },
            schema_registry,
            client_id: None,
            group_id: None,
        }
    }
}

/// Global application state
pub struct App {
    pub should_quit: bool,
    pub current_view: View,
    pub view_stack: Vec<View>,
    pub input_mode: InputMode,
    pub profile_manager: ProfileManager,
    pub active_cluster: Option<ClusterProfile>,
    /// Live Kafka client for the active cluster
    pub kafka_client: Option<KafkaClient>,
    /// Cached cluster metadata (topics, brokers)
    pub metadata: CachedMetadata,
    /// Is a metadata refresh currently in progress?
    pub loading: bool,
    /// Filter string applied to the current list view
    pub filter: String,
    pub list_cursor: usize,
    pub scroll_offset: u64,
    pub search_input: String,
    pub status_message: Option<String>,
    pub error_message: Option<String>,
    pub selected_topic: Option<String>,
    pub selected_partition: Option<i32>,
    /// Cached watermarks for the selected topic: Vec<(partition_id, low, high)>
    pub watermarks: Vec<(i32, i64, i64)>,
    pub selected_group: Option<String>,
    /// State for cluster creation/edit form
    pub cluster_form: ClusterForm,
    /// Index of cluster being edited (None = new)
    pub cluster_form_edit_index: Option<usize>,
    /// New-topic form fields
    pub new_topic_name: String,
    pub new_topic_partitions: String,
    pub new_topic_replication: String,
    // ── Phase 4: Message browser ─────────────────────────────────────────────
    pub messages: Vec<KafkaMessage>,
    pub messages_loading: bool,
    /// Start offset of current page (-1 = beginning)
    pub messages_start_offset: i64,
    pub selected_message_idx: Option<usize>,
    /// What the user is currently typing in message browser
    pub message_input: MessageInput,
    // ── Phase 5: Consumer groups ──────────────────────────────────────────────
    pub consumer_groups: Vec<GroupInfo>,
    pub consumer_groups_loading: bool,
    pub consumer_group_offsets: Vec<GroupPartitionOffset>,
    pub group_offsets_loading: bool,
    // ── Phase 6: Producer form ────────────────────────────────────────────────
    pub producer_form: ProducerForm,
    // ── Phase 9: ACL management ───────────────────────────────────────────────
    pub acl_list: Vec<AclEntry>,
    pub acl_loading: bool,
    // ── Phase 8: Schema Registry browser ─────────────────────────────────────
    pub schema_subjects: Vec<String>,
    pub schema_subjects_loading: bool,
    pub schema_subjects_cursor: usize,
    /// Versions for the selected subject
    pub schema_versions: Vec<i32>,
    /// Currently displayed schema detail
    pub schema_detail: Option<SchemaDetail>,
    pub schema_detail_loading: bool,
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
            kafka_client: None,
            metadata: CachedMetadata::default(),
            loading: false,
            filter: String::new(),
            list_cursor: 0,
            scroll_offset: 0,
            search_input: String::new(),
            status_message: None,
            error_message: None,
            selected_topic: None,
            selected_partition: None,
            watermarks: Vec::new(),
            selected_group: None,
            cluster_form: ClusterForm::default(),
            cluster_form_edit_index: None,
            new_topic_name: String::new(),
            new_topic_partitions: "1".to_string(),
            new_topic_replication: "1".to_string(),
            messages: Vec::new(),
            messages_loading: false,
            messages_start_offset: -1,
            selected_message_idx: None,
            message_input: MessageInput::None,
            consumer_groups: Vec::new(),
            consumer_groups_loading: false,
            consumer_group_offsets: Vec::new(),
            group_offsets_loading: false,
            producer_form: ProducerForm::default(),
            acl_list: Vec::new(),
            acl_loading: false,
            schema_subjects: Vec::new(),
            schema_subjects_loading: false,
            schema_subjects_cursor: 0,
            schema_versions: Vec::new(),
            schema_detail: None,
            schema_detail_loading: false,
        })
    }

    pub fn navigate_to(&mut self, view: View) {
        let prev = std::mem::replace(&mut self.current_view, view);
        self.view_stack.push(prev);
        self.list_cursor = 0;
        self.scroll_offset = 0;
    }

    pub fn navigate_back(&mut self) {
        if let Some(prev) = self.view_stack.pop() {
            self.current_view = prev;
            self.list_cursor = 0;
            self.scroll_offset = 0;
        }
    }

    pub fn set_status(&mut self, msg: impl Into<String>) {
        self.status_message = Some(msg.into());
    }

    pub fn set_error(&mut self, msg: impl Into<String>) {
        self.error_message = Some(msg.into());
    }

    /// Open the cluster form for a new cluster
    pub fn open_new_cluster_form(&mut self) {
        self.cluster_form = ClusterForm::default();
        self.cluster_form_edit_index = None;
        self.input_mode = InputMode::Editing;
        self.navigate_to(View::ClusterForm);
    }

    /// Open the cluster form to edit an existing profile
    pub fn open_edit_cluster_form(&mut self, index: usize) {
        if let Some(cluster) = self.profile_manager.profiles.get(index) {
            let c = cluster.clone();
            let (sr_url, sr_user, sr_pass) = c
                .schema_registry
                .as_ref()
                .map(|sr| {
                    (
                        sr.url.clone(),
                        sr.username.clone().unwrap_or_default(),
                        sr.password.clone().unwrap_or_default(),
                    )
                })
                .unwrap_or_default();
            let form = ClusterForm {
                name: c.name.clone(),
                bootstrap_servers: c.bootstrap_servers.clone(),
                auth_index: AUTH_MECHANISMS
                    .iter()
                    .position(|a| a == &c.auth)
                    .unwrap_or(0),
                ca_cert: c.ssl.ca_cert_path.clone().unwrap_or_default(),
                client_cert: c.ssl.client_cert_path.clone().unwrap_or_default(),
                client_key: c.ssl.client_key_path.clone().unwrap_or_default(),
                client_key_password: c.ssl.client_key_password.clone().unwrap_or_default(),
                verify_hostname: c.ssl.verify_hostname,
                sasl_username: c.sasl.username.clone().unwrap_or_default(),
                sasl_password: c.sasl.password.clone().unwrap_or_default(),
                kerberos_principal: c.sasl.kerberos_principal.clone().unwrap_or_default(),
                kerberos_keytab: c.sasl.kerberos_keytab.clone().unwrap_or_default(),
                kerberos_service_name: c.sasl.kerberos_service_name.clone().unwrap_or_default(),
                schema_registry_url: sr_url,
                schema_registry_user: sr_user,
                schema_registry_password: sr_pass,
                ..Default::default()
            };
            self.cluster_form = form;
            self.cluster_form_edit_index = Some(index);
            self.input_mode = InputMode::Editing;
            self.navigate_to(View::ClusterForm);
        }
    }

    /// Return topics filtered by current filter string
    pub fn filtered_topics(&self) -> Vec<&crate::kafka::metadata::TopicMeta> {
        self.metadata
            .topics
            .iter()
            .filter(|t| {
                if self.filter.is_empty() {
                    true
                } else {
                    t.name.to_lowercase().contains(&self.filter.to_lowercase())
                }
            })
            .collect()
    }

    /// Return the currently selected topic metadata
    pub fn selected_topic_meta(&self) -> Option<&crate::kafka::metadata::TopicMeta> {
        let name = self.selected_topic.as_deref()?;
        self.metadata.topics.iter().find(|t| t.name == name)
    }

    /// Build a SchemaRegistryClient from the active cluster's config (if configured)
    pub fn schema_registry_client(&self) -> Option<SchemaRegistryClient> {
        let cluster = self.active_cluster.as_ref()?;
        let sr = cluster.cluster.schema_registry.as_ref()?;
        Some(SchemaRegistryClient::new(
            sr.url.clone(),
            sr.username.clone(),
            sr.password.clone(),
        ))
    }

    pub async fn on_tick(&mut self) {}
}
