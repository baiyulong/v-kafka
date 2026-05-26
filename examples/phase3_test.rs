use rdkafka::config::ClientConfig;
use v_kafka::kafka::metadata::{fetch_cluster_metadata, fetch_watermarks};
use std::time::Duration;

fn main() {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", "n1-ikp-us.lenovo.com:9092,n2-ikp-us.lenovo.com:9092,n3-ikp-us.lenovo.com:9092")
       .set("security.protocol", "SASL_SSL")
       .set("sasl.mechanism", "SCRAM-SHA-512")
       .set("sasl.username", "kaf-mct2")
       .set("sasl.password", "jv70h4he")
       .set("ssl.ca.location", "/root/projects/v-kafka/ssl/lenovo-ca-bundle.pem")
       .set("enable.ssl.certificate.verification", "false")
       .set("socket.timeout.ms", "8000");

    println!("=== Phase 3: Metadata Test (IK-US) ===\n");

    match fetch_cluster_metadata(&cfg, Duration::from_secs(8)) {
        Ok(meta) => {
            println!("Brokers ({}):", meta.brokers.len());
            for b in &meta.brokers {
                println!("  Broker {} → {}:{}", b.id, b.host, b.port);
            }

            let user_topics: Vec<_> = meta.topics.iter().filter(|t| !t.is_internal).collect();
            println!("\nUser Topics ({}):", user_topics.len());
            for t in &user_topics {
                println!("  {:40} partitions:{} repl:{}", t.name, t.partition_count(), t.replication_factor());
            }

            // Fetch watermarks for first topic
            if let Some(first) = user_topics.first() {
                println!("\nWatermarks for '{}':", first.name);
                match fetch_watermarks(&cfg, &first.name, Duration::from_secs(8)) {
                    Ok(wm) => {
                        for (pid, low, high) in &wm {
                            println!("  Partition {:>3}: earliest={:<12} latest={:<12} msgs={}", pid, low, high, high - low);
                        }
                    }
                    Err(e) => println!("  Error: {}", e),
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}
