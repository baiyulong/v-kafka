use anyhow::Result;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::metadata::Metadata;
use rdkafka::ClientConfig;
use std::time::Duration;

/// Fetch cluster metadata (brokers + topic list)
pub async fn fetch_metadata(
    admin: &AdminClient<DefaultClientContext>,
    timeout: Duration,
) -> Result<Metadata> {
    // AdminClient doesn't expose metadata directly; use a temporary consumer
    // We store the ClientConfig separately for metadata fetches.
    todo!("Implemented in Phase 3")
}

/// Create a new topic
pub async fn create_topic(
    admin: &AdminClient<DefaultClientContext>,
    name: &str,
    partitions: i32,
    replication: i32,
) -> Result<()> {
    let topic = NewTopic::new(name, partitions, TopicReplication::Fixed(replication));
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin.create_topics([&topic], &opts).await?;
    for result in results {
        result.map_err(|(name, err)| {
            anyhow::anyhow!("Failed to create topic {}: {:?}", name, err)
        })?;
    }
    Ok(())
}

/// Delete topics by name
pub async fn delete_topics(
    admin: &AdminClient<DefaultClientContext>,
    names: &[&str],
) -> Result<()> {
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin.delete_topics(names, &opts).await?;
    for result in results {
        result.map_err(|(name, err)| {
            anyhow::anyhow!("Failed to delete topic {}: {:?}", name, err)
        })?;
    }
    Ok(())
}
