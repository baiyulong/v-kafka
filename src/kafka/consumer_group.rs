use anyhow::Result;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub group_id: String,
    pub state: String,
    pub protocol: String,
    pub members: usize,
}

#[derive(Debug, Clone)]
pub struct GroupPartitionOffset {
    pub topic: String,
    pub partition: i32,
    /// -1 means no committed offset
    pub committed_offset: i64,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

impl GroupPartitionOffset {
    pub fn lag(&self) -> i64 {
        if self.committed_offset < 0 {
            // No committed offset: entire partition is "lag"
            (self.high_watermark - self.low_watermark).max(0)
        } else {
            (self.high_watermark - self.committed_offset).max(0)
        }
    }
}

pub enum OffsetReset {
    Earliest,
    Latest,
    Specific(i64),
}

/// List all consumer groups visible to this client.
pub fn list_consumer_groups(config: &ClientConfig) -> Result<Vec<GroupInfo>> {
    let consumer: BaseConsumer = config.create()?;
    let groups = consumer.fetch_group_list(None, Duration::from_secs(10))?;
    let mut result: Vec<GroupInfo> = groups
        .groups()
        .iter()
        .filter(|g| !g.name().starts_with("_")) // skip internal groups
        .map(|g| GroupInfo {
            group_id: g.name().to_string(),
            state: g.state().to_string(),
            protocol: g.protocol().to_string(),
            members: g.members().len(),
        })
        .collect();
    result.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    Ok(result)
}

/// Fetch committed offsets and watermarks for each partition in the given list.
pub fn fetch_group_offsets(
    config: &ClientConfig,
    group_id: &str,
    partitions: &[(String, i32)],
) -> Result<Vec<GroupPartitionOffset>> {
    let mut cfg = config.clone();
    cfg.set("group.id", group_id);
    cfg.set("enable.auto.commit", "false");
    let consumer: BaseConsumer = cfg.create()?;

    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        tpl.add_partition(topic, *partition);
    }

    consumer.assign(&tpl)?;
    let committed = consumer.committed(Duration::from_secs(8))?;

    let mut results = Vec::new();
    for elem in committed.elements() {
        let committed_offset = match elem.offset() {
            Offset::Offset(o) if o >= 0 => o,
            _ => -1,
        };
        let (low, high) = consumer
            .fetch_watermarks(elem.topic(), elem.partition(), Duration::from_secs(5))
            .unwrap_or((0, 0));
        results.push(GroupPartitionOffset {
            topic: elem.topic().to_string(),
            partition: elem.partition(),
            committed_offset,
            low_watermark: low,
            high_watermark: high,
        });
    }
    results.sort_by(|a, b| {
        a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition))
    });
    Ok(results)
}

/// Reset committed offsets for a consumer group to earliest, latest, or a specific offset.
/// The group must be inactive (no running consumers) for this to take effect.
pub fn reset_group_offsets(
    config: &ClientConfig,
    group_id: &str,
    partitions: &[(String, i32)],
    reset_to: OffsetReset,
) -> Result<()> {
    let mut cfg = config.clone();
    cfg.set("group.id", group_id);
    cfg.set("enable.auto.commit", "false");
    let consumer: BaseConsumer = cfg.create()?;

    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        let actual_offset = match reset_to {
            OffsetReset::Earliest => {
                let (low, _) = consumer
                    .fetch_watermarks(topic, *partition, Duration::from_secs(5))?;
                low
            }
            OffsetReset::Latest => {
                let (_, high) = consumer
                    .fetch_watermarks(topic, *partition, Duration::from_secs(5))?;
                high
            }
            OffsetReset::Specific(o) => o,
        };
        tpl.add_partition_offset(topic, *partition, Offset::Offset(actual_offset))?;
    }

    consumer.commit(&tpl, CommitMode::Sync)?;
    Ok(())
}
