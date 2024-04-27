use futures_util::future::join_all;
use opentelemetry::global::shutdown_tracer_provider;
use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::ClientConfig;

use crate::transformations::example_transformations::test::{PrintName, SomeTransform};
use apache_avro::AvroSchema;
use std::time::Instant;
use stream_processor::domain::test::{get_test_person, Person};
use stream_processor::kafka_sink::kafka_avro_sink::KafkaAvroSink;
use stream_processor::kafka_source::general::CustomContext;
use stream_processor::kafka_source::kafka_avro_source::KafkaAvroSource;
use stream_processor::object_store_sink::azure_sink::AzureSink;
use stream_processor::sink::Sink;
use stream_processor::stream::{run_streams, ReadMode, StreamCreatorBuilder};
use stream_processor::telemetry::{init_metrics, init_test_tracing};
use tracing::{info, warn};

mod transformations;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_kafka_to_kafka() {
    init_test_tracing();
    let meter_provider = init_metrics("test_metrics".to_string());

    let source_topic = "k2k_in";
    let sink_topic = "k2k_out";
    prepare_test_data(source_topic, sink_topic).await;

    // prepare test
    let consumer_config = get_test_consumer_config();

    let data_source = KafkaAvroSource::new(
        consumer_config.clone(),
        CustomContext,
        vec![source_topic],
        ReadMode::Continuously,
        None, // schema will be filled later when calling the registry
        true,
        false,
        "http://localhost:8081",
    );

    let producer_config = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .to_owned();
    let data_sink = KafkaAvroSink::new(
        sink_topic,
        Person::get_schema(),
        producer_config,
        100000,
        "http://localhost:8081",
    )
    .await;

    let data_transformation = PrintName;

    let creator = StreamCreatorBuilder::new(data_source, data_transformation, data_sink)
        .with_auto_max_tasks()
        .await
        .build();

    let streams = creator.create_streams().await;

    // run test

    run_streams(streams).await.unwrap();

    // this is necessary until global shutdown is released for metric provider
    meter_provider.shutdown().unwrap();

    shutdown_tracer_provider();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_kafka_to_azure() {
    init_test_tracing();
    let meter_provider = init_metrics("test_metrics".to_string());

    let source_topic = "k2a_in";
    let sink_topic = "k2a_out";
    prepare_test_data(source_topic, sink_topic).await;

    // run tests
    let consumer_config = get_test_consumer_config();

    let data_source = KafkaAvroSource::new(
        consumer_config,
        CustomContext,
        vec![source_topic],
        ReadMode::Continuously,
        None, // schema will be filled later when calling the registry
        true,
        false,
        "http://localhost:8081",
    );

    let data_sink = AzureSink::new(100000, "azure_int_test");

    let data_transformation = PrintName;

    let creator = StreamCreatorBuilder::new(data_source, data_transformation, data_sink)
        .with_auto_max_tasks()
        .await
        .build();

    let streams = creator.create_streams().await;

    run_streams(streams).await.unwrap();

    // this is necessary until global shutdown is released for metric provider
    meter_provider.shutdown().unwrap();

    shutdown_tracer_provider();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_kafka_to_kafka_with_mut_transformation() {
    init_test_tracing();
    let meter_provider = init_metrics("test_metrics".to_string());

    let source_topic = "k2k_mut_in";
    let sink_topic = "k2k_mut_out";
    prepare_test_data(source_topic, sink_topic).await;

    // prepare test
    let consumer_config = get_test_consumer_config();

    let data_source = KafkaAvroSource::new(
        consumer_config.clone(),
        CustomContext,
        vec![source_topic],
        ReadMode::Continuously,
        None, // schema will be filled when calling the registry
        true,
        false,
        "http://localhost:8081",
    );

    let producer_config = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .to_owned();
    let data_sink = KafkaAvroSink::new(
        sink_topic,
        Person::get_schema(),
        producer_config,
        100000,
        "http://localhost:8081",
    )
    .await;

    let data_transformation = SomeTransform {
        list: vec!["Hey".to_string()],
    };

    let creator = StreamCreatorBuilder::new(data_source, data_transformation, data_sink)
        .with_auto_max_tasks()
        .await
        .build();

    let streams = creator.create_streams().await;

    // run test

    run_streams(streams).await.unwrap();

    // this is necessary until global shutdown is released for metric provider
    meter_provider.shutdown().unwrap();
    shutdown_tracer_provider();
}

#[tracing::instrument(skip(kafka_sink))]
async fn run_producers(kafka_sink: KafkaAvroSink, num_producers: u32, num_msgs: u32) {
    let start = Instant::now();

    let mut hs = Vec::new();
    for _ in 0..num_producers {
        let kafka_sink = kafka_sink.clone();
        let batch_size = kafka_sink.batch_size;
        let h = tokio::spawn(async move {
            let kafka_sink = kafka_sink.clone();
            let mut futs = Vec::new();
            for n in 0..num_msgs {
                let person = get_test_person();
                futs.push(person);
                if futs.len() as u32 == batch_size || n == num_msgs - 1 {
                    kafka_sink.write_batch(&futs).await.unwrap();
                }
            }
        });
        hs.push(h);
    }
    join_all(hs).await;

    let end = start.elapsed().as_millis();
    info!("Time elapsed for Test producers: {}", end);
}

async fn create_topics(source_topic: &str, sink_topic: &str) {
    let config = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .to_owned();

    let admin_client: AdminClient<DefaultClientContext> = config.create().expect("");

    let topic_in = NewTopic {
        name: source_topic,
        num_partitions: 3,
        replication: TopicReplication::Fixed(1),
        config: vec![],
    };

    let topic_out = NewTopic {
        name: sink_topic,
        num_partitions: 3,
        replication: TopicReplication::Fixed(1),
        config: vec![],
    };

    let results = admin_client
        .create_topics(&[topic_in, topic_out], &Default::default())
        .await
        .expect("Topic creation failed");

    for result in results {
        match result {
            Ok(_) => {}
            Err(e) => {
                warn!("Error: {}, {}", e.0, e.1)
            }
        }
    }
}

async fn prepare_test_data(source_topic: &str, sink_topic: &str) {
    create_topics(source_topic, sink_topic).await;

    // prepare test data data
    let producer_config = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .to_owned();

    let test_sink = KafkaAvroSink::new(
        source_topic,
        Person::get_schema(),
        producer_config,
        100000,
        "http://localhost:8081",
    )
    .await;
    run_producers(test_sink, 3, 10001).await;
}

fn get_test_consumer_config() -> ClientConfig {
    ClientConfig::new()
        .set("group.id", "test")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .to_owned()
}
