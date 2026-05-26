use super::{Decoder, DecoderFormat};

pub struct TextDecoder;

impl Decoder for TextDecoder {
    fn decode_key(&self, bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn decode_value(&self, bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn format(&self) -> DecoderFormat {
        DecoderFormat::Text
    }
}
