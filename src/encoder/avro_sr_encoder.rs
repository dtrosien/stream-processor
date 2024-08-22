use crate::encoder::Encoder;
use apache_avro::types::Value;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::Serialize;
use std::sync::Arc;

pub struct AvroSREncoder {
    encoder: AvroEncoder,
}

impl AvroSREncoder {
    pub fn new_simple(encoder: AvroEncoder) -> Self {
        AvroSREncoder { encoder }
    }

    pub fn new(encoder: AvroEncoder) -> Arc<Encoder> {
        Arc::new(Encoder::AvroSREncoder(Arc::new(AvroSREncoder { encoder })))
    }

    pub fn encode(
        &self,
        item: impl Serialize,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Option<Vec<u8>> {
        let payload = match self.encoder.encode_struct(item, subject_name_strategy) {
            Ok(v) => v,
            Err(e) => panic!("Error getting payload: {}", e),
        };
        Some(payload)
    }

    pub fn encode_val(
        &self,
        item: Value,
        subject_name_strategy: &SubjectNameStrategy,
    ) -> Option<Vec<u8>> {
        if let Value::Record(r) = item {
            let r = r.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();

            let payload = match self.encoder.encode(r, subject_name_strategy) {
                Ok(v) => v,
                Err(e) => panic!("Error getting payload: {}", e),
            };
            Some(payload)
        } else {
            panic!("todo")
        }
    }
}
