use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::{MsgType, RawTypes};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use std::any::Any;
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

        let msg: Vec<u8> = vec![1, 2];

        let batch = Arc::new(Batch::AnyBatch(vec![
            Arc::new(msg.clone()),
            Arc::new(msg.clone()),
        ]));
        let container: Arc<dyn BatchContainer> =
            GenericBatchContainer::new(batch, None, MsgType::Raw(RawTypes::Bytes));
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {
        self.consumer
            .commit_consumer_state(CommitMode::Sync)
            .unwrap()
    }
}
