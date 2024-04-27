use crate::avro::confluence_avro::{post_schema, serialize_to_confluence_avro};
use crate::kafka_sink::general::{generate_tracing_header, send_bytes_msg, KafkaSink};
use crate::sink::Sink;
use apache_avro::Schema;
use futures_util::future::try_join_all;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use serde::Serialize;
use std::fmt::Debug;
use std::time::Instant;
use tracing::info;

///
///
/// This sink uses Confluent header and schema registry to send avro byte msgs to kafka
///
///
pub struct KafkaAvroSink {
    producer_config: ClientConfig,
    pub producer: FutureProducer,
    pub topic: String,
    pub batch_size: u32,
    schema: Schema,
    schema_id: u32,
    pub schema_registry_url: String,
}

impl Clone for KafkaAvroSink {
    /// clone manually implemented to make sure that the Producer is exclusive to the Sink.
    /// Otherwise, the underlying Arc is just cloned
    fn clone(&self) -> Self {
        let producer: FutureProducer = self
            .producer_config
            .clone()
            .create()
            .expect("Producer creation error");

        KafkaAvroSink {
            producer_config: self.producer_config.clone(),
            producer,
            topic: self.topic.clone(),
            batch_size: self.batch_size,
            schema: self.schema.clone(),
            schema_id: self.schema_id,
            schema_registry_url: self.schema_registry_url.clone(),
        }
    }
}

impl KafkaAvroSink {
    /// on creation KafkaAvroSink tries to write the defined Schema at the Schema Registry
    pub async fn new(
        topic: &str,
        schema: Schema,
        producer_config: ClientConfig,
        batch_size: u32,
        schema_registry_url: &str,
    ) -> Self {
        let schema_id = post_schema(
            &schema.namespace().unwrap_or("default".to_string()), // todo make this configurable
            &schema.canonical_form(),
            schema_registry_url,
        )
        .await
        .expect("Could not post schema to registry");

        let producer: FutureProducer = producer_config
            .clone()
            .create()
            .expect("Producer creation error");
        KafkaAvroSink {
            producer_config,
            producer,
            topic: topic.to_string(),
            batch_size,
            schema,
            schema_id,
            schema_registry_url: schema_registry_url.to_string(),
        }
    }
}

impl KafkaSink for KafkaAvroSink {
    fn get_producer(&self) -> &FutureProducer {
        &self.producer
    }

    fn get_topic(&self) -> &str {
        &self.topic
    }
}

impl<O> Sink<O> for KafkaAvroSink
where
    O: Serialize + Debug,
{
    /// converts all data into confluence avro bytes and send them concurrent to kafka
    #[tracing::instrument(skip_all)]
    async fn write_batch(&self, data: &[O]) -> anyhow::Result<()> {
        let start = Instant::now();

        let send_futures: Vec<_> = data.iter().map(|d| send_avro(self, d)).collect();

        try_join_all(send_futures).await?;
        let end = start.elapsed().as_millis();
        info!("TIME NEEDED FOR KAFKA SINK: {}", end);
        Ok(())
    }

    fn get_batch_size(&self) -> u32 {
        self.batch_size
    }
}

///  converts input to bytes and uses confluence avro serialization if a schema and id is available in the KafkaSink
/// adds kafka header for tracing
#[tracing::instrument(level = "trace", skip(kafka_sink))]
pub async fn send_avro<T: Serialize + Debug + ?Sized>(
    kafka_sink: &KafkaAvroSink,
    msg: &T,
) -> anyhow::Result<OwnedDeliveryResult> {
    let bytes_msg = serialize_to_confluence_avro(msg, kafka_sink.schema_id, &kafka_sink.schema);

    // setup tracing for kafka
    let headers = generate_tracing_header();

    // send msg
    let result = send_bytes_msg(kafka_sink, headers, &bytes_msg).await;
    Ok(result)
}
