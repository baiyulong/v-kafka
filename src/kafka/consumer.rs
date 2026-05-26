use anyhow::Result;
use chrono::{DateTime, Utc};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::{Message, Offset, TopicPartitionList};
use std::time::Duration;

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
        let timestamp = msg.timestamp().to_millis().map(|ms| {
            DateTime::from_timestamp_millis(ms).unwrap_or_default()
        });
        let headers = msg
            .headers()
            .map(|h| {
                (0..h.count())
                    .filter_map(|i| {
                        let header = h.get(i);
                        Some((
                            header.key.to_string(),
                            header.value.unwrap_or_default().to_vec(),
                        ))
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
}

/// Fetch up to `limit` messages starting from `start_offset` in a partition
pub async fn fetch_messages(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    start_offset: Offset,
    limit: usize,
) -> Result<Vec<KafkaMessage>> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, start_offset)?;
    consumer.assign(&tpl)?;

    let mut messages = Vec::with_capacity(limit);
    while messages.len() < limit {
        match tokio::time::timeout(
            Duration::from_secs(5),
            consumer.recv(),
        )
        .await
        {
            Ok(Ok(msg)) => {
                let owned = msg.detach();
                messages.push(KafkaMessage::from_owned(&owned));
            }
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => break, // timeout — no more messages
        }
    }

    Ok(messages)
}
