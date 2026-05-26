//! Cached Kafka cluster metadata — populated asynchronously, read by the UI.

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::Metadata;
use rdkafka::ClientConfig;
use std::time::Duration;
use anyhow::Result;

#[derive(Debug, Clone, Default)]
pub struct CachedMetadata {
    pub brokers: Vec<BrokerMeta>,
    pub topics: Vec<TopicMeta>,
    pub controller_id: i32,
}

#[derive(Debug, Clone)]
pub struct BrokerMeta {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub name: String,
    pub partitions: Vec<PartitionMeta>,
    pub is_internal: bool,
}

impl TopicMeta {
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn replication_factor(&self) -> usize {
        self.partitions.first().map(|p| p.replicas.len()).unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionMeta {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub error: Option<String>,
    /// Populated separately via watermark queries
    pub low_watermark: Option<i64>,
    pub high_watermark: Option<i64>,
}

impl PartitionMeta {
    pub fn lag(&self) -> Option<i64> {
        match (self.low_watermark, self.high_watermark) {
            (Some(low), Some(high)) => Some(high - low),
            _ => None,
        }
    }
}

/// Fetch and parse cluster metadata into our cache model
pub fn fetch_cluster_metadata(config: &ClientConfig, timeout: Duration) -> Result<CachedMetadata> {
    let consumer: BaseConsumer = config.create()?;
    let raw: Metadata = consumer.fetch_metadata(None, timeout)?;

    let brokers: Vec<BrokerMeta> = raw.brokers().iter().map(|b| BrokerMeta {
        id: b.id(),
        host: b.host().to_string(),
        port: b.port(),
    }).collect();

    let topics: Vec<TopicMeta> = raw.topics().iter().map(|t| {
        let name = t.name().to_string();
        let is_internal = name.starts_with("__");
        let partitions: Vec<PartitionMeta> = t.partitions().iter().map(|p| {
            let error = p.error().map(|e| format!("{:?}", e));
            PartitionMeta {
                id: p.id(),
                leader: p.leader(),
                replicas: p.replicas().to_vec(),
                isr: p.isr().to_vec(),
                error,
                low_watermark: None,
                high_watermark: None,
            }
        }).collect();
        TopicMeta { name, partitions, is_internal }
    }).collect();

    Ok(CachedMetadata {
        brokers,
        topics,
        controller_id: -1,
    })
}

/// Fetch watermarks (earliest/latest offsets) for all partitions of a topic
pub fn fetch_watermarks(
    config: &ClientConfig,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<(i32, i64, i64)>> {
    let consumer: BaseConsumer = config.create()?;

    // First fetch metadata to discover partition count
    let meta = consumer.fetch_metadata(Some(topic), timeout)?;
    let topic_meta = meta.topics().iter().find(|t| t.name() == topic)
        .ok_or_else(|| anyhow::anyhow!("Topic '{}' not found", topic))?;

    let mut results = Vec::new();
    for partition in topic_meta.partitions() {
        let pid = partition.id();
        match consumer.fetch_watermarks(topic, pid, timeout) {
            Ok((low, high)) => results.push((pid, low, high)),
            Err(e) => {
                tracing::warn!("Watermark error for {}:{} — {}", topic, pid, e);
                results.push((pid, -1, -1));
            }
        }
    }
    results.sort_by_key(|(pid, _, _)| *pid);
    Ok(results)
}
