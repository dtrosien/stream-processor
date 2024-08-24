use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::MixedBatch;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub struct KafkaConsumer {
    consumer: StreamConsumer,
}

impl KafkaConsumer {
    pub fn new(client_config: ClientConfig) -> Arc<Self> {
        let consumer = client_config
            .create()
            .expect("Failed to create Kafka consumer");
        Arc::new(KafkaConsumer { consumer })
    }
}

impl DataSource for KafkaConsumer {
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        // let msg = self
        //     .consumer
        //     .recv()
        //     .await
        //     .unwrap()
        //     .payload()
        //     .unwrap()
        //     .to_vec();

        let artificial_msgs = vec!["hallo", "dies", "ist", "ein", "test"];

        let msg: Vec<Arc<Vec<u8>>> = artificial_msgs
            .into_iter()
            .map(|s| Arc::new(s.as_bytes().to_vec()))
            .collect();

        let batch = Arc::new(Batch::Mixed(MixedBatch::Bytes(msg)));
        let container: Arc<dyn BatchContainer> =
            GenericBatchContainer::new(batch, None, HashMap::default());
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {
        self.consumer
            .commit_consumer_state(CommitMode::Sync)
            .unwrap()
    }
}
