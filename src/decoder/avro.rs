use super::{Decoder, DecoderFormat};

/// Avro decoder — Schema Registry integration added in Phase 7
pub struct AvroDecoder;

impl Decoder for AvroDecoder {
    fn decode_key(&self, bytes: &[u8]) -> String {
        // Phase 7: use apache-avro + schema_registry_converter
        format!("<avro {} bytes>", bytes.len())
    }

    fn decode_value(&self, bytes: &[u8]) -> String {
        format!("<avro {} bytes>", bytes.len())
    }

    fn format(&self) -> DecoderFormat {
        DecoderFormat::Avro
    }
}
