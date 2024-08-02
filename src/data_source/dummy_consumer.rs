use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::MixedBatch;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use std::any::Any;
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

        let msg: Vec<Arc<Vec<u8>>> = artificial_msgs
            .into_iter()
            .map(|s| Arc::new(s.as_bytes().to_vec()))
            .collect();

        let batch = Arc::new(Batch::Mixed(MixedBatch::Bytes(msg)));
        let container: Arc<dyn BatchContainer> = GenericBatchContainer::new(batch, None);
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {}
}
