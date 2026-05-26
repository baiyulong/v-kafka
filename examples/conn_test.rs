use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::time::Duration;

fn test(name: &str, servers: &str, user: &str, pass: &str, ca: &str) {
    println!("\n=== {} ===", name);
    println!("Servers: {}", servers);

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", servers)
       .set("security.protocol", "SASL_SSL")
       .set("sasl.mechanism", "SCRAM-SHA-512")
       .set("sasl.username", user)
       .set("sasl.password", pass)
       .set("ssl.ca.location", ca)
       .set("enable.ssl.certificate.verification", "false")
       .set("socket.connection.setup.timeout.ms", "8000")
       .set("socket.timeout.ms", "8000");

    match cfg.create::<BaseConsumer>() {
        Ok(consumer) => {
            match consumer.fetch_metadata(None, Duration::from_secs(8)) {
                Ok(meta) => {
                    println!("✓ Connected! Brokers: {}", meta.brokers().len());
                    for b in meta.brokers() {
                        println!("  Broker {} → {}:{}", b.id(), b.host(), b.port());
                    }
                    let visible: Vec<_> = meta.topics().iter()
                        .filter(|t| !t.name().starts_with("__"))
                        .take(5)
                        .collect();
                    println!("  Total topics: {} (first {} shown):", meta.topics().len(), visible.len());
                    for t in visible {
                        println!("    - {} ({} partitions)", t.name(), t.partitions().len());
                    }
                }
                Err(e) => println!("✗ Metadata error: {}", e),
            }
        }
        Err(e) => println!("✗ Client create error: {}", e),
    }
}

fn main() {
    test(
        "KK-Prod (Shenyang)",
        "n1-kkp.lenovo.com:30902,n2-kkp.lenovo.com:30902,n3-kkp.lenovo.com:30902",
        "kaf-mct2", "jfHK9qfQP4XN",
        "/root/projects/v-kafka/ssl/lenovo-ca-bundle.pem"
    );
    test(
        "IK-US (Reston)",
        "n1-ikp-us.lenovo.com:9092,n2-ikp-us.lenovo.com:9092,n3-ikp-us.lenovo.com:9092",
        "kaf-mct2", "jv70h4he",
        "/root/projects/v-kafka/ssl/lenovo-ca-bundle.pem"
    );
}
