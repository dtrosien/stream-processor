use crate::stream::ReadMode;
use opentelemetry::propagation::Extractor;
use opentelemetry::{global, KeyValue};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, Headers};
use rdkafka::{ClientContext, TopicPartitionList};
use tracing::{error, info, trace, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub trait KafkaSource<C: ClientContext + ConsumerContext> {
    fn get_consumer(&self) -> &StreamConsumer<C>;

    fn get_topics(&self) -> &[&str];

    fn get_read_mode(&self) -> &ReadMode;
}

/// A context can be used to change the behavior of producers and consumers by adding callbacks
/// that will be executed by librdkafka.
/// This particular context sets up custom callbacks to log rebalancing events.
// todo check good settings to provide a good default
#[derive(Clone)]
pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

/// takes the minimum of the amount of partitions the consumer will read from and the max_consumer input value
#[tracing::instrument(skip_all)]
pub async fn derive_amount_of_required_kafka_consumers<C: ClientContext + ConsumerContext>(
    kafka_source: &impl KafkaSource<C>,
) -> u32 {
    let partitions: usize = kafka_source
        .get_topics()
        .iter()
        .map(|topic| {
            kafka_source
                .get_consumer()
                .fetch_metadata(Some(topic), None)
                .map_err(|e| error!("{}", e))
                .expect("Failed to fetch metadata")
                .topics()
                .iter()
                .map(|t| t.partitions().len())
                .sum::<usize>()
        })
        .sum();

    info!("Spawning num consumers: {}", partitions);
    partitions as u32
}

/// check how many partition exiting over all topics and spans that many consumers
#[tracing::instrument(skip_all)]
pub async fn subscribe_to_topics<C: ClientContext + ConsumerContext>(
    kafka_source: &impl KafkaSource<C>,
) -> anyhow::Result<()> {
    let topics = kafka_source.get_topics();

    kafka_source
        .get_consumer()
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    Ok(())
}

/// commits the current consumer state, consumer should NOT have
/// enable.auto.offset.store
/// or enable.auto.commit enabled
pub async fn commit_async<C: ClientContext + ConsumerContext>(
    kafka_source: &impl KafkaSource<C>,
) -> anyhow::Result<()> {
    kafka_source
        .get_consumer()
        .commit_consumer_state(CommitMode::Async)
        .map_err(anyhow::Error::new)
}

/// extract context from kafka header and sets it as parent for current span
pub fn extract_context_from_header(maybe_headers: Option<&BorrowedHeaders>) {
    if let Some(headers) = maybe_headers {
        for header in headers.iter() {
            trace!(" There is a Header {:#?}: {:?}", header.key, header.value);
        }
        // get otel context from header and pass to current span
        let context = global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(headers))
        });

        // set the extracted context to the current span
        Span::current().set_parent(context);
    }
}

///create opentelemetry metrics
pub fn create_metrics() {
    let meter = global::meter("kafka-source");
    let counter = meter.u64_counter("received_kafka_mgs").init();
    counter.add(1, &[KeyValue::new("sourcekey", "sourcevalue")]);
}

/// extract opentelemetry context info from kafka message header
/// see https://dev.to/ciscoemerge/trace-through-a-kafka-cluster-with-rust-and-opentelemetry-2jln
pub struct HeaderExtractor<'a>(pub &'a BorrowedHeaders);

impl<'a> Extractor for HeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        for i in 0..self.0.count() {
            if let Ok(val) = self.0.get_as::<str>(i) {
                if val.key == key {
                    return val.value;
                }
            }
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|kv| kv.key).collect::<Vec<_>>()
    }
}
