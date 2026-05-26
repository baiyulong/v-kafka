use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

/// Send a single message to a Kafka topic
pub async fn send_message(
    producer: &FutureProducer,
    topic: &str,
    partition: Option<i32>,
    key: Option<&[u8]>,
    payload: &[u8],
    headers: Vec<(String, Vec<u8>)>,
) -> Result<(i32, i64)> {
    use rdkafka::message::OwnedHeaders;

    let mut record = FutureRecord::to(topic).payload(payload);
    if let Some(k) = key {
        record = record.key(k);
    }
    if let Some(p) = partition {
        record = record.partition(p);
    }

    let mut owned_headers = OwnedHeaders::new();
    for (k, v) in &headers {
        owned_headers = owned_headers.insert(rdkafka::message::Header {
            key: k,
            value: Some(v.as_slice()),
        });
    }
    record = record.headers(owned_headers);

    let delivery = producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| anyhow::anyhow!("Send failed: {}", e))?;

    Ok(delivery)
}
