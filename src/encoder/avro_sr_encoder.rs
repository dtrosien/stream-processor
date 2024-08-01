use crate::container::{Batch, BatchContainer, GenericBatchContainer};

use apache_avro::types::Value;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::Serialize;
use std::any::Any;
use std::sync::Arc;

pub struct AvroSREncoder {
    encoder: AvroEncoder,
    subject_name_strategy: SubjectNameStrategy,
}
impl AvroSREncoder {
    fn encode(&self, item: impl Serialize) -> Option<Vec<u8>> {
        let payload = match self
            .encoder
            .encode_struct(item, &self.subject_name_strategy)
        {
            Ok(v) => v,
            Err(e) => panic!("Error getting payload: {}", e),
        };
        // Some(GenericBatchContainer::new(
        //     Arc::new(payload) as Arc<dyn Any>,
        //     None,
        //     MsgType::Raw(RawTypes::Bytes),
        // ))
        Some(payload)
    }

    fn encode_val(&self, item: Value) -> Option<Vec<u8>> {
        if let Value::Record(r) = item {
            let r = r.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();

            let payload = match self.encoder.encode(r, &self.subject_name_strategy) {
                Ok(v) => v,
                Err(e) => panic!("Error getting payload: {}", e),
            };
            // Some(GenericBatchContainer::new(
            //     Arc::new(Batch::AnyBatch(payload)),
            //     None,
            //     MsgType::Raw(RawTypes::Bytes),
            // ))
            Some(payload)
        } else {
            panic!("todo")
        }
    }
}
