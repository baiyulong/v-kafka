use super::{Decoder, DecoderFormat};
use crate::kafka::schema_registry::SchemaRegistryClient;
use apache_avro::{from_avro_datum, Schema};
use std::collections::HashMap;
use std::sync::Mutex;

/// Avro decoder supporting both raw Avro and Confluent wire format (magic byte + schema ID).
pub struct AvroDecoder {
    registry: Option<SchemaRegistryClient>,
    /// Cache of schema_id → parsed Apache Avro Schema
    schema_cache: Mutex<HashMap<i32, Schema>>,
}

impl AvroDecoder {
    pub fn new() -> Self {
        Self {
            registry: None,
            schema_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_registry(registry: SchemaRegistryClient) -> Self {
        Self {
            registry: Some(registry),
            schema_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Decode bytes that may be in Confluent wire format or raw Avro.
    pub fn decode_bytes(&self, bytes: &[u8]) -> String {
        // Confluent wire format: 0x00 + 4-byte big-endian schema ID + Avro binary
        if bytes.len() >= 5 && bytes[0] == 0x00 {
            let schema_id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
            return self.decode_confluent(bytes, schema_id);
        }
        // Try UTF-8 text first
        if let Ok(s) = std::str::from_utf8(bytes) {
            return s.to_string();
        }
        // Hex fallback
        hex_dump(bytes)
    }

    fn decode_confluent(&self, bytes: &[u8], schema_id: i32) -> String {
        let schema = match self.get_schema(schema_id) {
            Ok(s) => s,
            Err(e) => {
                return format!(
                    "<Avro id={} — schema unavailable: {}>\nHex: {}",
                    schema_id,
                    e,
                    hex_dump(&bytes[5..])
                )
            }
        };
        let mut cursor = std::io::Cursor::new(&bytes[5..]);
        match from_avro_datum(&schema, &mut cursor, None) {
            Ok(value) => avro_value_to_json(&value),
            Err(e) => format!("<Avro decode error: {}>\nHex: {}", e, hex_dump(&bytes[5..])),
        }
    }

    fn get_schema(&self, schema_id: i32) -> anyhow::Result<Schema> {
        {
            let cache = self.schema_cache.lock().unwrap();
            if let Some(s) = cache.get(&schema_id) {
                return Ok(s.clone());
            }
        }
        let registry = self.registry.as_ref().ok_or_else(|| {
            anyhow::anyhow!("No Schema Registry configured — cannot resolve schema {}", schema_id)
        })?;
        let (schema_json, _schema_type) = registry.get_schema_by_id(schema_id)?;
        let schema = Schema::parse_str(&schema_json)?;
        self.schema_cache.lock().unwrap().insert(schema_id, schema.clone());
        Ok(schema)
    }
}

fn hex_dump(bytes: &[u8]) -> String {
    bytes
        .chunks(16)
        .map(|row| {
            let hex: String = row.iter().map(|b| format!("{:02x} ", b)).collect();
            let printable: String = row.iter().map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' }).collect();
            format!("{:<48} {}", hex, printable)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Convert apache_avro::types::Value to a pretty-printed JSON string.
fn avro_value_to_json(value: &apache_avro::types::Value) -> String {
    let json = avro_to_json_value(value);
    serde_json::to_string_pretty(&json).unwrap_or_else(|_| format!("{:?}", value))
}

fn avro_to_json_value(value: &apache_avro::types::Value) -> serde_json::Value {
    use apache_avro::types::Value as Av;
    use serde_json::Value as Jv;
    match value {
        Av::Null => Jv::Null,
        Av::Boolean(b) => Jv::Bool(*b),
        Av::Int(i) => Jv::Number((*i).into()),
        Av::Long(l) => Jv::Number((*l).into()),
        Av::Float(f) => serde_json::Number::from_f64(*f as f64).map(Jv::Number).unwrap_or(Jv::Null),
        Av::Double(d) => serde_json::Number::from_f64(*d).map(Jv::Number).unwrap_or(Jv::Null),
        Av::Bytes(b) | Av::Fixed(_, b) => {
            Jv::String(b.iter().map(|x| format!("{:02x}", x)).collect::<Vec<_>>().join(""))
        }
        Av::String(s) => Jv::String(s.clone()),
        Av::Enum(_, s) => Jv::String(s.clone()),
        Av::Union(_, inner) => avro_to_json_value(inner),
        Av::Array(items) => Jv::Array(items.iter().map(avro_to_json_value).collect()),
        Av::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), avro_to_json_value(v));
            }
            Jv::Object(obj)
        }
        Av::Record(fields) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in fields {
                obj.insert(k.clone(), avro_to_json_value(v));
            }
            Jv::Object(obj)
        }
        Av::Date(d) => Jv::Number((*d).into()),
        Av::TimeMillis(t) => Jv::Number((*t).into()),
        Av::TimeMicros(t) => Jv::Number((*t).into()),
        Av::TimestampMillis(ts) => Jv::Number((*ts).into()),
        Av::TimestampMicros(ts) => Jv::Number((*ts).into()),
        Av::Decimal(d) => Jv::String(format!("{:?}", d)),
        Av::Duration(dur) => Jv::String(format!("{:?}", dur)),
        Av::Uuid(u) => Jv::String(u.to_string()),
        _ => Jv::String(format!("{:?}", value)),
    }
}

impl Decoder for AvroDecoder {
    fn decode_key(&self, bytes: &[u8]) -> String {
        self.decode_bytes(bytes)
    }

    fn decode_value(&self, bytes: &[u8]) -> String {
        self.decode_bytes(bytes)
    }

    fn format(&self) -> DecoderFormat {
        DecoderFormat::Avro
    }
}

