use crate::kafka_source::general::{
    commit_async, create_metrics, derive_amount_of_required_kafka_consumers,
    extract_context_from_header, subscribe_to_topics, KafkaSource,
};
use crate::source::{Source, SourceError};
use crate::stream::ReadMode;
use anyhow::Error;
use futures::Stream;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::FromBytes;
use rdkafka::{ClientConfig, ClientContext, Message};
use std::fmt::Debug;
use tracing::{error, info};

///
/// handles Msgs from Kafka as plain Bytes and tries to convert them into a specific Datatype via Consumer trait
///
pub struct KafkaByteSource<C: ClientContext + ConsumerContext> {
    consumer_config: ClientConfig,
    context: C,
    pub consumer: StreamConsumer<C>,
    pub topics: Vec<&'static str>,
    pub read_mode: ReadMode,
    pub stop_at_partition_end: bool,
    pub get_otel_context_from_header: bool,
}

impl<C: ClientContext + ConsumerContext + Clone + 'static> Clone for KafkaByteSource<C> {
    fn clone(&self) -> Self {
        KafkaByteSource::new(
            self.consumer_config.clone(),
            self.context.clone(),
            self.topics.clone(),
            self.read_mode.clone(),
            self.stop_at_partition_end,
            self.get_otel_context_from_header,
        )
    }
}

impl<C: ClientContext + ConsumerContext + Clone + 'static> KafkaByteSource<C> {
    pub fn new(
        mut consumer_config: ClientConfig,
        context: C,
        topics: Vec<&'static str>,
        read_mode: ReadMode,
        stop_at_partition_end: bool,
        get_otel_context_from_header: bool,
    ) -> Self {
        let consumer_config = consumer_config
            // mandatory because the consumer stores its state when data was written from the writer and flushes it afterwards
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            // mandatory to be able to stop at the end of the partition
            .set("enable.partition.eof", "true")
            .to_owned();
        let consumer = consumer_config
            .create_with_context(context.clone())
            .expect("Consumer creation failed");

        KafkaByteSource {
            consumer_config,
            context,
            consumer,
            topics,
            read_mode,
            stop_at_partition_end,
            get_otel_context_from_header,
        }
    }
}

impl<C: ClientContext + ConsumerContext + Clone> KafkaSource<C> for KafkaByteSource<C> {
    fn get_consumer(&self) -> &StreamConsumer<C> {
        &self.consumer
    }

    fn get_topics(&self) -> &[&str] {
        &self.topics
    }

    fn get_read_mode(&self) -> &ReadMode {
        &self.read_mode
    }
}

impl<I, C> Source<I> for KafkaByteSource<C>
where
    I: FromBytes + Clone + Debug + Sized,
    <I as FromBytes>::Error: Debug,
    C: ClientContext + ConsumerContext + 'static + Clone,
{
    #[tracing::instrument(skip_all)]
    async fn consume_next_item(&mut self) -> anyhow::Result<Option<I>, SourceError> {
        let consumer = &self.consumer;
        return match consumer.recv().await {
            Err(KafkaError::PartitionEOF(part)) => {
                info!("Consumer reached end of partition: {}", part);
                Err(SourceError::EndOfPartitionError)
            }
            Err(e) => {
                error!("Kafka error: {}", e);
                Err(SourceError::UnexpectedError(Error::from(e)))
            }
            Ok(m) => {
                if self.get_otel_context_from_header {
                    extract_context_from_header(m.headers());
                }

                let payload = match m.payload() {
                    None => Ok(None),
                    Some(p) => match I::from_bytes(p) {
                        Ok(a) => Ok(Some(a.clone())),
                        Err(e) => {
                            error!("Error deserializing bytes: {:?}", e);
                            Err(SourceError::DeserializationError(Error::msg(
                                "Deserialize FromBytes Error",
                            )))
                        }
                    },
                };

                consumer
                    .store_offset_from_message(&m)
                    .map_err(|e| SourceError::CommitError(Error::from(e)))?;
                create_metrics();

                payload
            }
        };
    }

    fn get_read_mode(&mut self) -> &ReadMode {
        &self.read_mode
    }
    #[tracing::instrument(skip_all)]
    async fn initialize(&self) -> anyhow::Result<(), SourceError> {
        subscribe_to_topics(self)
            .await
            .map_err(SourceError::UnexpectedError)
    }

    async fn derive_max_parallel_tasks(&self) -> u32 {
        derive_amount_of_required_kafka_consumers(self).await
    }

    async fn commit(&mut self) -> anyhow::Result<(), SourceError> {
        commit_async(self)
            .await
            .map_err(SourceError::UnexpectedError)
    }

    async fn stream(&mut self) -> impl Stream {
        self.consumer.stream()
    }
    fn stop_at_stream_end(&self) -> bool {
        self.stop_at_partition_end
    }
}
