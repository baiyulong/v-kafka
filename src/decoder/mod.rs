pub mod avro;
pub mod json;
pub mod protobuf;
pub mod text;

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
