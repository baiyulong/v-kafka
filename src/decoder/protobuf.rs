use super::{Decoder, DecoderFormat};

/// Protobuf decoder — prost integration added in Phase 7
pub struct ProtobufDecoder;

impl Decoder for ProtobufDecoder {
    fn decode_key(&self, bytes: &[u8]) -> String {
        format!("<protobuf {} bytes>", bytes.len())
    }

    fn decode_value(&self, bytes: &[u8]) -> String {
        format!("<protobuf {} bytes>", bytes.len())
    }

    fn format(&self) -> DecoderFormat {
        DecoderFormat::Protobuf
    }
}
