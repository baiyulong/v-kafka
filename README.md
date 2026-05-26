# v-kafka

A full-featured terminal UI Kafka client written in Rust — inspired by k9s and lazygit.

![Rust](https://img.shields.io/badge/rust-1.75%2B-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

## Features

| Module | Capabilities |
|---|---|
| **Connection Management** | Multiple cluster profiles, auto-saved to `~/.config/v-kafka/config.toml` |
| **Authentication** | PLAINTEXT, SSL, SASL-PLAIN, SASL-SCRAM-256/512, Kerberos (GSSAPI) |
| **Broker Info** | Cluster ID, Controller, full broker list |
| **Topic List** | Browse all topics, filter by name, create & delete |
| **Partition View** | Leader, Replicas, ISR, Earliest/Latest offsets |
| **Message Browser** | Page through messages, jump by offset or timestamp, filter by content |
| **Message Detail** | Key, Value (JSON pretty-print), Headers, metadata — auto-decodes Avro |
| **Consumer Groups** | List groups, view per-partition Lag, Reset offsets (Earliest/Latest/Specific) |
| **Producer** | Send messages with custom Topic/Partition/Key/Value/Headers |
| **Schema Registry** | Browse subjects, view Avro/JSON Schema with version history |
| **Avro Decoding** | Confluent wire format auto-detected, decoded via Schema Registry |
| **ACL View** | List ACL entries (create/delete planned — pending rdkafka 0.37) |

## Installation

### From source

```bash
git clone https://github.com/baiyulong/v-kafka.git
cd v-kafka
cargo build --release
./target/release/vk
```

### Prerequisites

```bash
# Ubuntu / Debian
sudo apt-get install cmake libssl-dev libsasl2-dev pkg-config

# macOS
brew install cmake openssl
```

## Configuration

Config file: `~/.config/v-kafka/config.toml`

### Minimal (PLAINTEXT)

```toml
[[clusters]]
name = "Local"
bootstrap_servers = "localhost:9092"
auth = "plaintext"
```

### SASL/SSL (SCRAM-SHA-512)

```toml
[[clusters]]
name = "Production"
bootstrap_servers = "broker1:9092,broker2:9092,broker3:9092"
auth = "sasl_scram_sha512"
client_id = "v-kafka"
group_id = "my-team-group"   # must be ACL-authorized on your cluster

[clusters.ssl]
ca_cert_path = "/path/to/ca-bundle.pem"
verify_hostname = false       # set true in production

[clusters.sasl]
username = "your-user"
password = "your-password"

[clusters.schema_registry]
url = "https://schema-registry:8081"
username = "sr-user"
password = "sr-password"
```

### Auth mechanism values

| Value | Mechanism |
|---|---|
| `plaintext` | No authentication |
| `ssl` | TLS only (mutual TLS) |
| `sasl_plain` | SASL/PLAIN |
| `sasl_scram_sha256` | SASL/SCRAM-SHA-256 |
| `sasl_scram_sha512` | SASL/SCRAM-SHA-512 |
| `kerberos` | GSSAPI/Kerberos |

> **Note on `group_id`**: Some clusters restrict which consumer group IDs a user can use via ACLs. Set `group_id` to a group your user is authorized to read as. v-kafka sets `enable.auto.commit=false` so it never commits offsets, but the group.id is still sent to the broker for authorization.

## Key Bindings

### Global

| Key | Action |
|---|---|
| `q` / `Esc` | Go back / quit |
| `?` | Show help |
| `Ctrl+C` | Quit immediately |

### Cluster List

| Key | Action |
|---|---|
| `↑↓` / `jk` | Navigate |
| `Enter` | Connect to cluster |
| `n` | New cluster |
| `e` | Edit cluster |
| `d` | Delete cluster |

### Topic List (after connecting)

| Key | Action |
|---|---|
| `Enter` | View partitions |
| `b` | Broker info |
| `g` | Consumer groups |
| `s` | Schema Registry |
| `a` | ACL management |
| `p` | Open producer form |
| `n` | New topic |
| `d` | Delete topic |
| `r` | Refresh |
| `/` | Filter topics |

### Message Browser

| Key | Action |
|---|---|
| `Enter` | View message detail |
| `o` | Jump to offset |
| `t` | Jump to timestamp |
| `/` | Filter by content |
| `r` | Reload messages |

### Consumer Groups

| Key | Action |
|---|---|
| `Enter` | View group detail (partition lag) |
| `R` | Reset offsets to Earliest |
| `r` | Refresh |

### Schema Registry

| Key | Action |
|---|---|
| `↑↓` | Navigate subjects |
| `Enter` | Load latest schema |
| `↑↓` / `PgUp/PgDn` | Scroll schema detail |
| `r` | Refresh subjects |

## Avro Decoding

v-kafka automatically detects the [Confluent wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format) (magic byte `0x00` + 4-byte schema ID). When a Schema Registry is configured for the cluster, it fetches the schema on first use and caches it in memory for subsequent messages.

If no Schema Registry is configured, it shows the schema ID and a hint.

## Architecture

```
src/
├── main.rs           — tokio runtime + TUI event loop
├── app.rs            — App state machine (all views, all state)
├── config/           — Cluster config + profile manager (~/.config/v-kafka/)
├── kafka/
│   ├── client.rs     — rdkafka ClientConfig builder
│   ├── metadata.rs   — topic/broker metadata with caching
│   ├── consumer.rs   — message fetch (BaseConsumer, blocking)
│   ├── consumer_group.rs — group list, lag, offset reset
│   ├── admin.rs      — topic & ACL admin
│   ├── producer.rs   — FutureProducer send
│   └── schema_registry.rs — Schema Registry REST client (ureq)
├── decoder/
│   ├── mod.rs        — Decoder trait + auto_decode_value()
│   ├── avro.rs       — Avro decoder (Confluent wire format + Schema Registry)
│   ├── json.rs       — JSON formatter
│   └── text.rs       — UTF-8 text
├── ui/
│   ├── mod.rs        — top-level render() dispatcher
│   ├── theme.rs      — color palette
│   └── components/   — one file per view
└── events/
    ├── handler.rs    — keyboard event handlers (all views)
    └── mod.rs        — EventHandler (crossterm poll + tick)
```

## License

MIT
