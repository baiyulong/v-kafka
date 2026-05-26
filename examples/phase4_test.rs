use v_kafka::config::cluster::{AuthMechanism, ClusterConfig, SaslConfig, SslConfig};
use v_kafka::kafka::client::KafkaClient;
use v_kafka::kafka::consumer::{fetch_messages_blocking, PAGE_SIZE};
use v_kafka::kafka::consumer_group::list_consumer_groups;
use v_kafka::kafka::metadata::{fetch_cluster_metadata, fetch_watermarks};
use std::time::Duration;

fn main() {
    let cluster = ClusterConfig {
        name: "IK-US".into(),
        bootstrap_servers: "n1-ikp-us.lenovo.com:9092,n2-ikp-us.lenovo.com:9092,n3-ikp-us.lenovo.com:9092".into(),
        auth: AuthMechanism::SaslScramSha512,
        ssl: SslConfig {
            ca_cert_path: Some("/root/projects/v-kafka/ssl/lenovo-ca-bundle.pem".into()),
            verify_hostname: false,
            ..Default::default()
        },
        sasl: SaslConfig {
            username: Some("kaf-mct2".into()),
            password: Some("jv70h4he".into()),
            ..Default::default()
        },
        schema_registry: None,
        client_id: None,
        group_id: Some("mct2".into()),
    };
    let client = KafkaClient::new(&cluster).expect("client init");
    let cfg = &client.config;

    // ─── Phase 4: Message fetch ───────────────────────────────────────────────
    println!("=== Phase 4: Message Browser ===");
    let meta = fetch_cluster_metadata(cfg, Duration::from_secs(10)).unwrap();
    // Pick first non-internal topic
    let topic_name = meta.topics.iter()
        .find(|t| !t.is_internal)
        .map(|t| t.name.clone())
        .expect("no topics");

    // Get watermarks
    let watermarks = fetch_watermarks(cfg, &topic_name, Duration::from_secs(8)).unwrap();
    let (_, low, high) = watermarks[0];
    let start = (high - PAGE_SIZE as i64).max(low);
    println!("Topic: {}  P0  low={} high={}", topic_name, low, high);

    if high > low {
        let msgs = fetch_messages_blocking(cfg, &topic_name, 0, start, PAGE_SIZE, high).unwrap();
        println!("Loaded {} messages:", msgs.len());
        for msg in msgs.iter().take(3) {
            println!("  #{:<8} key={:<20} value={}", msg.offset, msg.key_display(), msg.value_preview(60));
        }
    } else {
        println!("  (partition is empty, offset {}/{})", low, high);
        // Try first topic with messages
        for t in meta.topics.iter().filter(|t| !t.is_internal).take(10) {
            let wm = fetch_watermarks(cfg, &t.name, Duration::from_secs(5)).unwrap_or_default();
            if let Some((_, l, h)) = wm.first() {
                if h > l {
                    let s = (h - PAGE_SIZE as i64).max(*l);
                    let msgs = fetch_messages_blocking(cfg, &t.name, 0, s, PAGE_SIZE, *h).unwrap();
                    println!("Topic {} P0: loaded {} msgs (offsets {}..{})", t.name, msgs.len(), s, h);
                    if let Some(m) = msgs.first() {
                        println!("  First: #{} key={} value={}", m.offset, m.key_display(), m.value_preview(80));
                    }
                    break;
                }
            }
        }
    }

    // ─── Phase 5: Consumer groups ─────────────────────────────────────────────
    println!("\n=== Phase 5: Consumer Groups ===");
    let groups = list_consumer_groups(cfg).unwrap();
    println!("Found {} consumer groups:", groups.len());
    for g in groups.iter().take(8) {
        println!("  {:<40} state={:<8} members={}", g.group_id, g.state, g.members);
    }
}
