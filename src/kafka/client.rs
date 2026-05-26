use anyhow::Result;
use rdkafka::{
    admin::AdminClient,
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{DefaultConsumerContext, StreamConsumer},
    producer::FutureProducer,
};

use crate::config::cluster::ClusterConfig;

pub struct KafkaClient {
    pub config: ClientConfig,
}

impl KafkaClient {
    /// Build a KafkaClient from a ClusterConfig
    pub fn new(cluster: &ClusterConfig) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (k, v) in cluster.to_rdkafka_config() {
            config.set(k, v);
        }
        Ok(Self { config })
    }

    pub fn admin_client(&self) -> Result<AdminClient<DefaultClientContext>> {
        Ok(self.config.create()?)
    }

    pub fn consumer(&self, group_id: &str) -> Result<StreamConsumer<DefaultConsumerContext>> {
        let mut cfg = self.config.clone();
        cfg.set("group.id", group_id);
        cfg.set("enable.auto.commit", "false");
        cfg.set("auto.offset.reset", "earliest");
        Ok(cfg.create()?)
    }

    pub fn producer(&self) -> Result<FutureProducer> {
        Ok(self.config.create()?)
    }
}
