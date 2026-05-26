use anyhow::{Context, Result};
use rdkafka::{
    admin::AdminClient,
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext, StreamConsumer},
    metadata::Metadata,
    producer::FutureProducer,
};
use std::time::Duration;

use crate::config::cluster::ClusterConfig;

pub struct KafkaClient {
    pub config: ClientConfig,
}

impl KafkaClient {
    pub fn new(cluster: &ClusterConfig) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (k, v) in cluster.to_rdkafka_config() {
            config.set(k, v);
        }
        // group.id is required by rdkafka for partition assignment and message fetching.
        // Use the user-configured group_id, or derive from SASL username.
        if config.get("group.id").is_none() {
            let group_id = cluster
                .group_id
                .as_deref()
                .map(|g| g.to_string())
                .or_else(|| {
                    cluster.sasl.username.as_deref().map(|u| {
                        // e.g. "kaf-mct2" → "mct2-vk" (strip common prefix)
                        let base = u.strip_prefix("kaf-").unwrap_or(u);
                        format!("{}-vk", base)
                    })
                })
                .unwrap_or_else(|| "v-kafka-inspector".to_string());
            config.set("group.id", group_id);
        }
        Ok(Self { config })
    }

    pub fn admin_client(&self) -> Result<AdminClient<DefaultClientContext>> {
        self.config.create().context("Creating admin client")
    }

    pub fn consumer(&self, group_id: &str) -> Result<StreamConsumer<DefaultConsumerContext>> {
        let mut cfg = self.config.clone();
        cfg.set("group.id", group_id);
        cfg.set("enable.auto.commit", "false");
        cfg.set("auto.offset.reset", "earliest");
        cfg.create().context("Creating consumer")
    }

    pub fn producer(&self) -> Result<FutureProducer> {
        self.config.create().context("Creating producer")
    }

    /// Fetch cluster metadata as a connectivity test.
    pub fn test_connection(&self, timeout: Duration) -> Result<ClusterInfo> {
        let consumer: BaseConsumer = self.config.create().context("Creating test consumer")?;
        let metadata = consumer
            .fetch_metadata(None, timeout)
            .context("Fetching metadata")?;
        Ok(ClusterInfo::from_metadata(&metadata))
    }

    /// Fetch full metadata for all topics
    pub fn fetch_metadata(&self, timeout: Duration) -> Result<Metadata> {
        let consumer: BaseConsumer = self.config.create().context("Creating metadata consumer")?;
        consumer
            .fetch_metadata(None, timeout)
            .context("Fetching metadata")
    }

    /// Fetch metadata for a single topic
    pub fn fetch_topic_metadata(&self, topic: &str, timeout: Duration) -> Result<Metadata> {
        let consumer: BaseConsumer = self.config.create().context("Creating metadata consumer")?;
        consumer
            .fetch_metadata(Some(topic), timeout)
            .context("Fetching topic metadata")
    }
}

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub broker_count: usize,
    pub controller_id: i32,
    pub brokers: Vec<BrokerInfo>,
}

#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

impl ClusterInfo {
    pub fn from_metadata(meta: &Metadata) -> Self {
        let brokers: Vec<BrokerInfo> = meta
            .brokers()
            .iter()
            .map(|b| BrokerInfo {
                id: b.id(),
                host: b.host().to_string(),
                port: b.port(),
            })
            .collect();
        Self {
            broker_count: brokers.len(),
            controller_id: -1,
            brokers,
        }
    }
}
