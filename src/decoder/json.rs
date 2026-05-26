use super::{Decoder, DecoderFormat};

pub struct JsonDecoder;

impl Decoder for JsonDecoder {
    fn decode_key(&self, bytes: &[u8]) -> String {
        try_pretty_json(bytes).unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned())
    }

    fn decode_value(&self, bytes: &[u8]) -> String {
        try_pretty_json(bytes).unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned())
    }

    fn format(&self) -> DecoderFormat {
        DecoderFormat::Json
    }
}

fn try_pretty_json(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    serde_json::to_string_pretty(&value).ok()
}
