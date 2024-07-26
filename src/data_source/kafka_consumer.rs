use crate::container::{GenericMsgContainer, MsgContainer};
use crate::data_source::DataSource;
use crate::type_definitions::{MsgType, RawTypes};
use rdkafka::consumer::StreamConsumer;
use rdkafka::Message;
use std::any::Any;
use std::sync::Arc;

struct KafkaConsumer {
    consumer: StreamConsumer,
}

impl DataSource for KafkaConsumer {
    fn read_batch(&self) -> Arc<dyn MsgContainer> {
        // let msg = self
        //     .consumer
        //     .recv()
        //     .await
        //     .unwrap()
        //     .payload()
        //     .unwrap()
        //     .to_vec();

        let msg: Vec<u8> = vec![1, 2];
        let container = GenericMsgContainer::new(
            Arc::new(msg) as Arc<dyn Any>,
            None,
            MsgType::Raw(RawTypes::Bytes),
        );
        container
    }
}
