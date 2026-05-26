use anyhow::Result;
use chrono::{DateTime, Utc};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::time::Duration;

pub const PAGE_SIZE: usize = 50;

/// A single Kafka message with decoded metadata
#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub timestamp: Option<DateTime<Utc>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl KafkaMessage {
    pub fn from_owned(msg: &OwnedMessage) -> Self {
        use rdkafka::message::Headers;
        let timestamp = msg.timestamp().to_millis().and_then(|ms| {
            DateTime::from_timestamp_millis(ms)
        });
        let headers = msg
            .headers()
            .map(|h| {
                (0..h.count())
                    .map(|i| {
                        let header = h.get(i);
                        (
                            header.key.to_string(),
                            header.value.unwrap_or_default().to_vec(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            key: msg.key().map(|k| k.to_vec()),
            payload: msg.payload().map(|p| p.to_vec()),
            timestamp,
            headers,
        }
    }

    pub fn key_display(&self) -> String {
        match &self.key {
            None => "(null)".to_string(),
            Some(k) => String::from_utf8(k.clone())
                .unwrap_or_else(|_| format!("<binary {}B>", k.len())),
        }
    }

    pub fn value_preview(&self, max_len: usize) -> String {
        match &self.payload {
            None => "(null)".to_string(),
            Some(p) => {
                let s = String::from_utf8(p.clone())
                    .unwrap_or_else(|_| format!("<binary {}B>", p.len()));
                // Collapse whitespace for preview
                let collapsed: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
                if collapsed.len() > max_len {
                    format!("{}…", &collapsed[..max_len])
                } else {
                    collapsed
                }
            }
        }
    }

    pub fn value_pretty(&self) -> String {
        match &self.payload {
            None => "(null)".to_string(),
            Some(p) => {
                if let Ok(s) = String::from_utf8(p.clone()) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
                        return serde_json::to_string_pretty(&v).unwrap_or(s);
                    }
                    s
                } else {
                    format!("<binary {} bytes>", p.len())
                }
            }
        }
    }
}

/// Fetch up to `limit` messages starting from a given offset (blocking).
/// start_offset: >= 0 = specific offset, -1 = earliest, -2 = latest
/// high_watermark: stop early if reached (0 = don't check)
pub fn fetch_messages_blocking(
    config: &ClientConfig,
    topic: &str,
    partition: i32,
    start_offset: i64,
    limit: usize,
    high_watermark: i64,
) -> Result<Vec<KafkaMessage>> {
    let consumer: BaseConsumer = config.clone().create()?;
    let offset = match start_offset {
        -1 => Offset::Beginning,
        -2 => Offset::End,
        n => Offset::Offset(n),
    };
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, offset)?;
    consumer.assign(&tpl)?;

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let poll_timeout = Duration::from_millis(500);
    let mut messages = Vec::with_capacity(limit);
    loop {
        if messages.len() >= limit || std::time::Instant::now() >= deadline {
            break;
        }
        match consumer.poll(poll_timeout) {
            Some(Ok(msg)) => {
                let msg_offset = msg.offset();
                let owned = msg.detach();
                messages.push(KafkaMessage::from_owned(&owned));
                if high_watermark > 0 && msg_offset >= high_watermark - 1 {
                    break;
                }
            }
            Some(Err(e)) => return Err(e.into()),
            // poll timeout: keep retrying until deadline (connection/auth setup may take time)
            None => {}
        }
    }
    Ok(messages)
}

/// Fetch messages from a specific timestamp onwards (blocking).
pub fn fetch_messages_from_timestamp(
    config: &ClientConfig,
    topic: &str,
    partition: i32,
    timestamp_ms: i64,
    limit: usize,
) -> Result<Vec<KafkaMessage>> {
    let consumer: BaseConsumer = config.clone().create()?;
    // offsets_for_times repurposes the TPL offset field as a timestamp
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))?;
    let resolved = consumer.offsets_for_times(tpl, Duration::from_secs(10))?;
    let actual_offset = resolved
        .find_partition(topic, partition)
        .and_then(|p| match p.offset() {
            Offset::Offset(o) if o >= 0 => Some(o),
            _ => None,
        });
    match actual_offset {
        None => Ok(vec![]),
        Some(o) => fetch_messages_blocking(config, topic, partition, o, limit, 0),
    }
}
