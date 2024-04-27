use opentelemetry::global;
use opentelemetry::propagation::Injector;
use rdkafka::message::{Header, Headers, OwnedHeaders, ToBytes};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::{trace, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub trait KafkaSink {
    fn get_producer(&self) -> &FutureProducer;

    fn get_topic(&self) -> &str;
}

/// generates headers required to match kafka mgs from a producer at the receiver for tracing
pub fn generate_tracing_header() -> OwnedHeaders {
    let mut headers = OwnedHeaders::new();
    let context = Span::current().context(); // use this if all messages should be under the same context (trace)
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(&mut headers))
    });
    headers
}

/// sends the actual bytes to the kafka topic and create basic otel metrics
#[tracing::instrument(level = "trace", skip_all)]
pub async fn send_bytes_msg<T, K>(
    kafka_sink: &K,
    headers: OwnedHeaders,
    msg: &T,
) -> OwnedDeliveryResult
where
    T: ToBytes + ?Sized,
    K: KafkaSink,
{
    let delivery_status = kafka_sink
        .get_producer()
        .send(
            FutureRecord::to(kafka_sink.get_topic())
                .payload(msg)
                .key(&format!("Key {}", Uuid::new_v4()))
                .headers(headers),
            Duration::from_secs(0),
        )
        .await;

    trace!("Delivery status for message received");
    delivery_status
}

/// used to inject span context to kafka headers
/// from https://dev.to/ciscoemerge/trace-through-a-kafka-cluster-with-rust-and-opentelemetry-2jln
pub struct HeaderInjector<'a>(pub &'a mut OwnedHeaders);

impl<'a> Injector for HeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        let mut new = OwnedHeaders::new().insert(Header {
            key,
            value: Some(&value),
        });

        for header in self.0.iter() {
            let s = String::from_utf8(header.value.unwrap().to_vec()).unwrap();
            new = new.insert(Header {
                key: header.key,
                value: Some(&s),
            });
        }

        self.0.clone_from(&new);
    }
}
