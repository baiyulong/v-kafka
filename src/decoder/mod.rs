pub mod avro;
pub mod json;
pub mod protobuf;
pub mod text;

use crate::kafka::schema_registry::SchemaRegistryClient;

/// The output of decoding a message payload
#[derive(Debug, Clone)]
pub struct DecodedMessage {
    pub key: Option<String>,
    pub value: Option<String>,
    pub format: DecoderFormat,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecoderFormat {
    Text,
    Json,
    Avro,
    Protobuf,
}

/// Common trait for all message decoders
pub trait Decoder: Send + Sync {
    fn decode_key(&self, bytes: &[u8]) -> String;
    fn decode_value(&self, bytes: &[u8]) -> String;
    fn format(&self) -> DecoderFormat;
}

/// Auto-detect format and decode bytes to a display string.
/// Detects Confluent Avro wire format (0x00 + 4-byte schema ID) if `registry` is provided.
pub fn auto_decode_value(
    bytes: &[u8],
    registry: Option<&SchemaRegistryClient>,
) -> (String, DecoderFormat) {
    // Confluent Avro wire format: magic byte 0x00
    if bytes.len() >= 5 && bytes[0] == 0x00 {
        if let Some(reg) = registry {
            let decoder = avro::AvroDecoder::with_registry(reg.clone());
            return (decoder.decode_bytes(bytes), DecoderFormat::Avro);
        }
        // No registry — show schema ID hint
        let schema_id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        return (
            format!(
                "<Confluent Avro schema_id={} — configure Schema Registry to decode>",
                schema_id
            ),
            DecoderFormat::Avro,
        );
    }
    // Try UTF-8 → JSON → plain text
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            return (
                serde_json::to_string_pretty(&v).unwrap_or_else(|_| s.to_string()),
                DecoderFormat::Json,
            );
        }
        return (s.to_string(), DecoderFormat::Text);
    }
    // Binary
    let hex: String = bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");
    (
        format!("<binary {} bytes>\n{}", bytes.len(), hex),
        DecoderFormat::Text,
    )
}
