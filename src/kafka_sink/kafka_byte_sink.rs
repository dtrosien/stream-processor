use crate::kafka_sink::general::{generate_tracing_header, send_bytes_msg, KafkaSink};
use crate::sink::Sink;
use futures_util::future::try_join_all;
use rdkafka::message::ToBytes;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use serde::Serialize;
use std::fmt::Debug;
use std::time::Instant;
use tracing::debug;

#[derive(Clone)]
pub struct KafkaByteSink {
    pub producer: FutureProducer,
    pub topic: String,
    pub batch_size: u32,
}

impl KafkaByteSink {
    pub fn new(topic: &str, producer_config: ClientConfig, batch_size: u32) -> Self {
        let producer: FutureProducer = producer_config.create().expect("Producer creation error");
        KafkaByteSink {
            producer,
            topic: topic.to_string(),
            batch_size,
        }
    }
}

impl KafkaSink for KafkaByteSink {
    fn get_producer(&self) -> &FutureProducer {
        &self.producer
    }

    fn get_topic(&self) -> &str {
        &self.topic
    }
}

impl<O> Sink<O> for KafkaByteSink
where
    O: Serialize + Debug + ToBytes,
{
    /// converts all data into confluence avro bytes and send them concurrent to kafka
    #[tracing::instrument(skip_all)]
    async fn write_batch(&self, data: &[O]) -> anyhow::Result<()> {
        let start = Instant::now();

        let send_futures: Vec<_> = data.iter().map(|d| send(self, d)).collect();

        try_join_all(send_futures).await?;
        let end = start.elapsed().as_millis();
        debug!("TIME NEEDED FOR KAFKA SINK: {}", end);
        Ok(())
    }
    fn get_batch_size(&self) -> u32 {
        self.batch_size
    }
}

/// send input as bytes via ToBytes trait, adds kafka header for tracing
/// can be used to send str
#[tracing::instrument(skip(kafka_sink))]
async fn send<T>(kafka_sink: &KafkaByteSink, msg: &T) -> anyhow::Result<OwnedDeliveryResult>
where
    T: Debug + ToBytes + ?Sized,
{
    // setup tracing for kafka
    let headers = generate_tracing_header();

    // send msg
    let result = send_bytes_msg(kafka_sink, headers, msg).await;
    Ok(result)
}
