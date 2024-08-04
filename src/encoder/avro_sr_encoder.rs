use apache_avro::types::Value;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::Serialize;

pub struct AvroSREncoder {
    encoder: AvroEncoder,
    subject_name_strategy: SubjectNameStrategy,
}
impl AvroSREncoder {
    pub fn new(encoder: AvroEncoder, subject_name_strategy: SubjectNameStrategy) -> Self {
        AvroSREncoder {
            encoder,
            subject_name_strategy,
        }
    }

    pub fn encode(&self, item: impl Serialize) -> Option<Vec<u8>> {
        let payload = match self
            .encoder
            .encode_struct(item, &self.subject_name_strategy)
        {
            Ok(v) => v,
            Err(e) => panic!("Error getting payload: {}", e),
        };
        Some(payload)
    }

    pub fn encode_val(&self, item: Value) -> Option<Vec<u8>> {
        if let Value::Record(r) = item {
            let r = r.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();

            let payload = match self.encoder.encode(r, &self.subject_name_strategy) {
                Ok(v) => v,
                Err(e) => panic!("Error getting payload: {}", e),
            };
            Some(payload)
        } else {
            panic!("todo")
        }
    }
}
