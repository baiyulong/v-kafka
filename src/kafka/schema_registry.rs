use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::time::Duration;

/// Lightweight blocking Schema Registry HTTP client (uses ureq, no tokio deps).
#[derive(Debug, Clone)]
pub struct SchemaRegistryClient {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaDetail {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    pub schema: String,
    #[serde(rename = "schemaType", default = "default_schema_type")]
    pub schema_type: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SchemaById {
    pub schema: String,
    #[serde(rename = "schemaType", default = "default_schema_type")]
    pub schema_type: String,
}

fn default_schema_type() -> String {
    "AVRO".to_string()
}

impl SchemaRegistryClient {
    pub fn new(url: String, username: Option<String>, password: Option<String>) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            username,
            password,
        }
    }

    fn get(&self, path: &str) -> Result<ureq::Response> {
        let url = format!("{}{}", self.url, path);
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(10))
            .build();
        let mut req = agent.get(&url);
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            req = req.set("Authorization", &basic_auth(u, p));
        }
        req.call()
            .map_err(|e| anyhow!("Schema Registry {}: {}", path, e))
    }

    /// GET /subjects → all subject names
    pub fn list_subjects(&self) -> Result<Vec<String>> {
        let body: Vec<String> = self.get("/subjects")?.into_json()?;
        Ok(body)
    }

    /// GET /subjects/{subject}/versions → version numbers
    pub fn list_versions(&self, subject: &str) -> Result<Vec<i32>> {
        let path = format!("/subjects/{}/versions", subject);
        let body: Vec<i32> = self.get(&path)?.into_json()?;
        Ok(body)
    }

    /// GET /subjects/{subject}/versions/{version} → full schema detail
    pub fn get_schema_version(&self, subject: &str, version: i32) -> Result<SchemaDetail> {
        let path = format!("/subjects/{}/versions/{}", subject, version);
        let detail: SchemaDetail = self.get(&path)?.into_json()?;
        Ok(detail)
    }

    /// GET /subjects/{subject}/versions/latest
    pub fn get_latest_schema(&self, subject: &str) -> Result<SchemaDetail> {
        let path = format!("/subjects/{}/versions/latest", subject);
        let detail: SchemaDetail = self.get(&path)?.into_json()?;
        Ok(detail)
    }

    /// GET /schemas/ids/{id} → schema JSON string (for Confluent wire format decoding)
    pub fn get_schema_by_id(&self, id: i32) -> Result<(String, String)> {
        let path = format!("/schemas/ids/{}", id);
        let body: SchemaById = self.get(&path)?.into_json()?;
        Ok((body.schema, body.schema_type))
    }
}

fn basic_auth(user: &str, pass: &str) -> String {
    let encoded = base64_encode(format!("{}:{}", user, pass).as_bytes());
    format!("Basic {}", encoded)
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(TABLE[(b0 >> 2) & 0x3f] as char);
        out.push(TABLE[((b0 << 4) | (b1 >> 4)) & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((b1 << 2) | (b2 >> 6)) & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[b2 & 0x3f] as char
        } else {
            '='
        });
    }
    out
}
