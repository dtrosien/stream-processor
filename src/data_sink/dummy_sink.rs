use crate::container::{Batch, BatchContainer};
use crate::data_sink::DataSink;
use crate::type_definitions::{ErrorBatch, UniformBatch};
use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

pub struct DummySink {}

#[async_trait]
impl DataSink for DummySink {
    fn write(
        &self,
        input: Arc<dyn BatchContainer>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let batch = input.clone().get_batch();

        if let Batch::Uniform(UniformBatch::Bytes(bytes_batch)) = batch.as_ref() {
            bytes_batch.iter().for_each(|item| {
                println!(
                    "writer got container:{} with num bites: {}",
                    input.clone().get_type_name().unwrap(),
                    item.len()
                )
            });
            Box::new(vec![].into_iter())
        } else {
            warn!("Got Error Batch");
            Box::new(vec![input].into_iter())
        }
    }
}
