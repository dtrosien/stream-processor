use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::MixedBatch;
use apache_avro::{to_avro_datum, to_value, AvroSchema, Schema};
use parquet::data_type::AsBytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct DummyConsumer {}

impl DummyConsumer {
    pub fn new() -> Arc<Self> {
        Arc::new(DummyConsumer {})
    }
}

impl DataSource for DummyConsumer {
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let artificial_msgs = vec!["hallo", "dies", "ist", "ein", "test"];
        let magic_byte = 0u8;
        let id_bytes = 1_u32.to_be_bytes();
        let msg: Vec<Arc<Vec<u8>>> = artificial_msgs
            .into_iter()
            .map(|s| {
                let msg = StringMessage {
                    message: s.to_string(),
                };

                let schema = StringMessage::get_schema();
                let avro_binary = to_avro_datum(&schema, to_value(msg).unwrap()).unwrap();

                let mut byte_vector = Vec::new();
                byte_vector.push(magic_byte);
                byte_vector.extend(id_bytes);
                byte_vector.extend_from_slice(&avro_binary);
                Arc::new(byte_vector)
            })
            .collect();

        let batch = Arc::new(Batch::Mixed(MixedBatch::Bytes(msg)));
        let container: Arc<dyn BatchContainer> = GenericBatchContainer::new(batch, None);
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {}
}
#[derive(Debug, Serialize, Deserialize, AvroSchema)]
pub struct StringMessage {
    pub message: String,
}
