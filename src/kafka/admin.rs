use anyhow::Result;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use std::time::Duration;

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

/// A simplified ACL entry for display purposes
#[derive(Debug, Clone)]
pub struct AclEntry {
    pub resource_type: String,
    pub name: String,
    pub pattern_type: String,
    pub principal: String,
    pub host: String,
    pub operation: String,
    pub permission: String,
}

/// Fetch all ACL entries.
/// Note: rdkafka 0.36 does not expose ACL admin API bindings.
pub async fn describe_acls(
    _admin: &AdminClient<DefaultClientContext>,
) -> Result<Vec<AclEntry>> {
    Ok(vec![])
}

/// Delete an ACL entry (not available in rdkafka 0.36 bindings).
pub async fn delete_acl(
    _admin: &AdminClient<DefaultClientContext>,
    _entry: &AclEntry,
) -> Result<()> {
    anyhow::bail!("ACL deletion not supported in rdkafka 0.36 bindings")
}
