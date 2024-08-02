use crate::container::{Batch, BatchContainer};
use crate::data_sink::DataSink;
use crate::type_definitions::UniformBatch;
use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub struct KafkaProducer {
    //  producer: FutureProducer,
}

#[async_trait]
impl DataSink for KafkaProducer {
    fn write(&self, input: Arc<dyn BatchContainer>) {
        let batch = input.clone().get_batch();

        if let Batch::Uniform(UniformBatch::Bytes(bytes_batch)) = batch.as_ref() {
            bytes_batch.iter().for_each(|item| {
                // let _ = self.producer.send(
                //     FutureRecord::to(&input.clone().get_sink().unwrap())
                //         .payload(item.as_slice())
                //         //.headers(headers)
                //         .key(&format!("Key {}", Uuid::new_v4())),
                //     Duration::from_secs(0),
                // );
                // .await;

                println!("writer got bites: {}", item.len())
            })
        }
    }
}
