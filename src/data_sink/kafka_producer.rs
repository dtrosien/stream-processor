use crate::container::{Batch, BatchContainer};
use crate::data_sink::DataSink;
use crate::type_definitions::{MsgType, RawTypes};
use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

struct KafkaProducer {
    producer: FutureProducer,
}

#[async_trait]
impl DataSink for KafkaProducer {
    fn write(&self, input: Arc<dyn BatchContainer>) {
        let msg_type = input.clone().get_batch_type();

        if let MsgType::Raw(RawTypes::Bytes) = msg_type.as_ref() {
            let batch = input.clone().get_batch();

            if let Batch::AnyBatch(bytes_batch) = batch.as_ref() {
                bytes_batch.iter().for_each(|item| {
                    let payload = item.downcast_ref::<Vec<u8>>().unwrap();
                    let _ = self.producer.send(
                        FutureRecord::to(&input.clone().get_sink().unwrap())
                            .payload(payload)
                            //.headers(headers)
                            .key(&format!("Key {}", Uuid::new_v4())),
                        Duration::from_secs(0),
                    );
                    // .await;
                })
            }
        } else {
            panic!("{:?} not supported by writer", msg_type.as_ref())
        }
    }
}
