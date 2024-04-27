use crate::avro::confluence_avro::{get_schema, unwrap_from_confluence_header};
use crate::kafka_source::general::{
    commit_async, create_metrics, derive_amount_of_required_kafka_consumers,
    extract_context_from_header, subscribe_to_topics, KafkaSource,
};
use crate::source::{Source, SourceError};
use crate::stream::ReadMode;
use anyhow::Error;
use apache_avro::{from_avro_datum, from_value, AvroSchema, Schema};
use futures::Stream;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, ClientContext, Message};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use tracing::{debug, error, trace, warn};

///
/// handles Msgs from Kafka as (confluence) avro encoded Bytes and tries to convert them into a specific Datatype via DataHandler trait
/// if the schema is not provided the consumer will try to fetch it from schema registry
///
pub struct KafkaAvroSource<C: ClientContext + ConsumerContext + Clone> {
    consumer_config: ClientConfig,
    context: C,
    pub consumer: StreamConsumer<C>,
    pub schema: Option<Schema>,
    pub topics: Vec<&'static str>,
    pub read_mode: ReadMode,
    pub stop_at_partition_end: bool,
    pub get_otel_context_from_header: bool,
    pub schema_registry_url: String,
}

impl<C: ClientContext + ConsumerContext + Clone + 'static> Clone for KafkaAvroSource<C> {
    fn clone(&self) -> Self {
        KafkaAvroSource::new(
            self.consumer_config.clone(),
            self.context.clone(),
            self.topics.clone(),
            self.read_mode.clone(),
            self.schema.clone(),
            self.stop_at_partition_end,
            self.get_otel_context_from_header,
            &self.schema_registry_url,
        )
    }
}

impl<C: ClientContext + ConsumerContext + Clone + 'static> KafkaAvroSource<C> {
    #[allow(clippy::too_many_arguments)] // todo better config handling
    pub fn new(
        mut consumer_config: ClientConfig,
        context: C,
        topics: Vec<&'static str>,
        read_mode: ReadMode,
        schema: Option<Schema>,
        stop_at_partition_end: bool,
        get_otel_context_from_header: bool,
        schema_registry_url: &str,
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

        KafkaAvroSource {
            consumer_config,
            context,
            consumer,
            schema,
            topics,
            read_mode,
            stop_at_partition_end,
            get_otel_context_from_header,
            schema_registry_url: schema_registry_url.to_string(),
        }
    }
}

impl<C: ClientContext + ConsumerContext + Clone> KafkaSource<C> for KafkaAvroSource<C> {
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

impl<I, C> Source<I> for KafkaAvroSource<C>
where
    I: DeserializeOwned + Debug + Sized + AvroSchema,
    C: ClientContext + ConsumerContext + Clone + 'static,
{
    #[tracing::instrument(level = "trace", skip_all)]
    async fn consume_next_item(&mut self) -> anyhow::Result<Option<I>, SourceError> {
        let consumer = &self.consumer;
        return match consumer.recv().await {
            Err(KafkaError::PartitionEOF(part)) => {
                warn!("Consumer reached end of partition: {}", part);
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

                match m.payload() {
                    Some(avro_data) => {
                        let (id, data) = unwrap_from_confluence_header(avro_data)?;

                        if self.schema.is_none() {
                            debug!("search for schema with id: {}", id);
                            let raw_schema = get_schema(id, &self.schema_registry_url).await?;

                            self.schema =
                                Some(Schema::parse_str(&raw_schema).map_err(anyhow::Error::from)?);
                        }

                        let schema = self
                            .schema
                            .as_ref()
                            .ok_or(anyhow::Error::msg("No schema found in cache"))?;
                        let value = from_avro_datum(schema, &mut &data[..], None)
                            .map_err(|e| SourceError::DeserializationError(Error::from(e)))?;

                        let data = from_value::<I>(&value)
                            .map_err(|e| SourceError::DeserializationError(Error::from(e)))?;

                        trace!("Received Data: {:?}", data);

                        consumer
                            .store_offset_from_message(&m)
                            .map_err(|e| SourceError::CommitError(Error::from(e)))?;

                        create_metrics();

                        Ok(Some(data))
                    }

                    None => Ok(None),
                }
            }
        };
    }

    fn get_read_mode(&mut self) -> &ReadMode {
        &self.read_mode
    }

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

#[derive(Clone)]
pub struct SchemaRegistry {
    pub url: String,
}
