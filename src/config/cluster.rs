use serde::{Deserialize, Serialize};

/// Authentication mechanism
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMechanism {
    Plaintext,
    Ssl,
    SaslPlain,
    SaslScramSha256,
    SaslScramSha512,
    Kerberos,
}

impl Default for AuthMechanism {
    fn default() -> Self {
        Self::Plaintext
    }
}

/// SSL configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SslConfig {
    pub ca_cert_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub client_key_password: Option<String>,
    pub verify_hostname: bool,
}

/// SASL configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SaslConfig {
    pub username: Option<String>,
    pub password: Option<String>,
    /// Kerberos principal
    pub kerberos_principal: Option<String>,
    /// Kerberos keytab path
    pub kerberos_keytab: Option<String>,
    /// Kerberos service name (default: "kafka")
    pub kerberos_service_name: Option<String>,
}

/// Schema Registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistryConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// A single Kafka cluster connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Display name for the cluster
    pub name: String,
    /// Comma-separated list of bootstrap brokers
    pub bootstrap_servers: String,
    /// Authentication mechanism
    #[serde(default)]
    pub auth: AuthMechanism,
    /// SSL settings (used when auth = ssl or sasl_*)
    #[serde(default)]
    pub ssl: SslConfig,
    /// SASL credentials
    #[serde(default)]
    pub sasl: SaslConfig,
    /// Optional Schema Registry
    pub schema_registry: Option<SchemaRegistryConfig>,
    /// Custom client ID
    pub client_id: Option<String>,
    /// Consumer group ID used for message inspection (leave empty to auto-derive).
    /// Must be authorized by cluster ACLs. Example: "my-team-group"
    #[serde(default)]
    pub group_id: Option<String>,
}

impl ClusterConfig {
    /// Build rdkafka ClientConfig entries from this cluster config
    pub fn to_rdkafka_config(&self) -> Vec<(String, String)> {
        let mut cfg = vec![
            ("bootstrap.servers".to_string(), self.bootstrap_servers.clone()),
            (
                "client.id".to_string(),
                self.client_id
                    .clone()
                    .unwrap_or_else(|| "v-kafka".to_string()),
            ),
        ];

        match &self.auth {
            AuthMechanism::Plaintext => {
                cfg.push(("security.protocol".to_string(), "plaintext".to_string()));
            }
            AuthMechanism::Ssl => {
                cfg.push(("security.protocol".to_string(), "ssl".to_string()));
                self.apply_ssl_config(&mut cfg);
            }
            AuthMechanism::SaslPlain => {
                cfg.push(("security.protocol".to_string(), "sasl_plaintext".to_string()));
                cfg.push(("sasl.mechanism".to_string(), "PLAIN".to_string()));
                self.apply_sasl_config(&mut cfg);
            }
            AuthMechanism::SaslScramSha256 => {
                cfg.push(("security.protocol".to_string(), "sasl_ssl".to_string()));
                cfg.push(("sasl.mechanism".to_string(), "SCRAM-SHA-256".to_string()));
                self.apply_ssl_config(&mut cfg);
                self.apply_sasl_config(&mut cfg);
            }
            AuthMechanism::SaslScramSha512 => {
                cfg.push(("security.protocol".to_string(), "sasl_ssl".to_string()));
                cfg.push(("sasl.mechanism".to_string(), "SCRAM-SHA-512".to_string()));
                self.apply_ssl_config(&mut cfg);
                self.apply_sasl_config(&mut cfg);
            }
            AuthMechanism::Kerberos => {
                cfg.push(("security.protocol".to_string(), "sasl_plaintext".to_string()));
                cfg.push(("sasl.mechanism".to_string(), "GSSAPI".to_string()));
                if let Some(sn) = &self.sasl.kerberos_service_name {
                    cfg.push(("sasl.kerberos.service.name".to_string(), sn.clone()));
                } else {
                    cfg.push(("sasl.kerberos.service.name".to_string(), "kafka".to_string()));
                }
                if let Some(principal) = &self.sasl.kerberos_principal {
                    cfg.push(("sasl.kerberos.principal".to_string(), principal.clone()));
                }
                if let Some(keytab) = &self.sasl.kerberos_keytab {
                    cfg.push(("sasl.kerberos.keytab".to_string(), keytab.clone()));
                }
            }
        }

        cfg
    }

    fn apply_ssl_config(&self, cfg: &mut Vec<(String, String)>) {
        if let Some(ca) = &self.ssl.ca_cert_path {
            cfg.push(("ssl.ca.location".to_string(), ca.clone()));
        }
        if let Some(cert) = &self.ssl.client_cert_path {
            cfg.push(("ssl.certificate.location".to_string(), cert.clone()));
        }
        if let Some(key) = &self.ssl.client_key_path {
            cfg.push(("ssl.key.location".to_string(), key.clone()));
        }
        if let Some(pw) = &self.ssl.client_key_password {
            cfg.push(("ssl.key.password".to_string(), pw.clone()));
        }
        cfg.push((
            "enable.ssl.certificate.verification".to_string(),
            if self.ssl.verify_hostname { "true" } else { "false" }.to_string(),
        ));
    }

    fn apply_sasl_config(&self, cfg: &mut Vec<(String, String)>) {
        if let Some(user) = &self.sasl.username {
            cfg.push(("sasl.username".to_string(), user.clone()));
        }
        if let Some(pw) = &self.sasl.password {
            cfg.push(("sasl.password".to_string(), pw.clone()));
        }
    }
}
