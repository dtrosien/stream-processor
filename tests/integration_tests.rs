use crate::mappers::MapperTestImpl;
use crate::transformations::FlattenStructTransformation;
use apache_avro::AvroSchema;
use mockito::Server;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use std::collections::HashMap;
use std::sync::Arc;
use stream_processor::data_sink::dummy_sink::DummySink;
use stream_processor::decoder::avro_sr_decoder::AvroSRDecoder;
use stream_processor::encoder::avro_sr_encoder::AvroSREncoder;
use stream_processor::execution::ExecutionContext;
use stream_processor::stream::Stream;
use stream_processor::test_struct::{FlatA, TestStruct};

pub mod custom_types;
pub mod mappers;
pub mod transformations;
//

//todo implement test trafo and fix mock responses for new flat struct struct(s)
#[test]
fn dummy_to_dummy_with_sr() {
    let mut server = Server::new();
    let _m = server.mock("GET", "/schemas/ids/1?deleted=true")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(r#"{"schema":"{\"type\":\"record\",\"name\":\"StringMessage\",\"namespace\":\"some.namespace\",\"fields\":[{\"name\":\"timestamp_ms\",\"type\":\"long\"},{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"records_binaries\",\"type\":{\"type\":\"array\",\"items\":{\"name\":\"BinaryRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"timestamp_ms\",\"type\":\"long\"},{\"name\":\"binary_type\",\"type\":{\"name\":\"BinaryType\",\"type\":\"enum\",\"symbols\":[\"A\",\"B\",\"C\"]}},{\"name\":\"value\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}}}]}"}"#)
        .create();

    let _a = server.mock("GET", "/subjects/topicA-some.namespace.FlatA/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(r#"{"subject":"FlatA-value","version":1,"id":3,"schema":"{\"type\":\"record\",\"name\":\"FlatA\",\"namespace\":\"some.namespace\",\"fields\":[{\"name\":\"timestamp_ms\",\"type\":\"long\"},{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"value\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}"}"#)
        .create();

    let _b = server.mock("GET", "/subjects/topicA-some.namespace.FlatB/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(r#"{"subject":"FlatB-value","version":1,"id":4,"schema":"{\"type\":\"record\",\"name\":\"FlatB\",\"namespace\":\"some.namespace\",\"fields\":[{\"name\":\"timestamp_ms\",\"type\":\"long\"},{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"value\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}"}"#)
        .create();

    let _c = server.mock("GET", "/subjects/topicA-some.namespace.FlatC/versions/latest")
        .with_status(200)
        .with_header("content-type", "application/vnd.schemaregistry.v1+json")
        .with_body(r#"{"subject":"FlatC-value","version":1,"id":5,"schema":"{\"type\":\"record\",\"name\":\"FlatC\",\"namespace\":\"some.namespace\",\"fields\":[{\"name\":\"timestamp_ms\",\"type\":\"long\"},{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"source\",\"type\":\"string\"},{\"name\":\"value\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}"}"#)
        .create();

    println!("{:?}", TestStruct::get_schema().canonical_form());

    println!("FLAT_A: {:?}", FlatA::get_schema().canonical_form());

    let context = ExecutionContext::new(HashMap::default());
    let sr_settings = SrSettings::new(server.url());
    let decoder = AvroSRDecoder::new(sr_settings.clone());
    let avro_encoder = AvroEncoder::new(sr_settings);

    let stream = context
        .dummy::<TestStruct>(10)
        .deserialize(decoder)
        .transform(
            Some(MapperTestImpl::new()),
            Some(AvroSREncoder::new(avro_encoder)),
            vec![FlattenStructTransformation::new()],
        )
        .write(Arc::new(DummySink {}));

    context.execute_once(stream, false);
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// async fn test_kafka_to_kafka() {
//     init_test_tracing();
//     let meter_provider = init_metrics("test_metrics".to_string());
//
//     let source_topic = "k2k_in";
//     let sink_topic = "k2k_out";
//     prepare_test_data(source_topic, sink_topic).await;
//
//     // prepare test
//     let consumer_config = get_test_consumer_config();
//
//     let data_source = KafkaAvroSource::new(
//         consumer_config.clone(),
//         CustomContext,
//         vec![source_topic],
//         ReadMode::Continuously,
//         None, // schema will be filled later when calling the registry
//         true,
//         false,
//         "http://localhost:8081",
//     );
//
//     let producer_config = ClientConfig::new()
//         .set("bootstrap.servers", "localhost:9092")
//         .set("message.timeout.ms", "5000")
//         .to_owned();
//     let data_sink = KafkaAvroSink::new(
//         sink_topic,
//         Person::get_schema(),
//         producer_config,
//         100000,
//         "http://localhost:8081",
//     )
//     .await;
//
//     let data_transformation = PrintName;
//
//     let creator = StreamCreatorBuilder::new(data_source, data_transformation, data_sink)
//         .with_auto_max_tasks()
//         .await
//         .build();
//
//     let streams = creator.create_streams().await;
//
//     // run test
//
//     run_streams(streams).await.unwrap();
//
//     // this is necessary until global shutdown is released for metric provider
//     meter_provider.shutdown().unwrap();
//
//     shutdown_tracer_provider();
// }
//
// #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// async fn test_kafka_to_azure() {
//     init_test_tracing();
//     let meter_provider = init_metrics("test_metrics".to_string());
//
//     let source_topic = "k2a_in";
//     let sink_topic = "k2a_out";
//     prepare_test_data(source_topic, sink_topic).await;
//
//     // run tests
//     let consumer_config = get_test_consumer_config();
//
//     let data_source = KafkaAvroSource::new(
//         consumer_config,
//         CustomContext,
//         vec![source_topic],
//         ReadMode::Continuously,
//         None, // schema will be filled later when calling the registry
//         true,
//         false,
//         "http://localhost:8081",
//     );
//
//     let data_sink = AzureSink::new(100000, "azure_int_test");
//
//     let data_transformation = PrintName;
//
//     let creator = StreamCreatorBuilder::new(data_source, data_transformation, data_sink)
//         .with_auto_max_tasks()
//         .await
//         .build();
//
//     let streams = creator.create_streams().await;
//
//     run_streams(streams).await.unwrap();
//
//     // this is necessary until global shutdown is released for metric provider
//     meter_provider.shutdown().unwrap();
//
//     shutdown_tracer_provider();
// }
//
// #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
// async fn test_kafka_to_kafka_with_mut_transformation() {
//     init_test_tracing();
//     let meter_provider = init_metrics("test_metrics".to_string());
//
//     let source_topic = "k2k_mut_in";
//     let sink_topic = "k2k_mut_out";
//     prepare_test_data(source_topic, sink_topic).await;
//
//     // prepare test
//     let consumer_config = get_test_consumer_config();
//
//     let data_source = KafkaAvroSource::new(
//         consumer_config.clone(),
//         CustomContext,
//         vec![source_topic],
//         ReadMode::Continuously,
//         None, // schema will be filled when calling the registry
//         true,
//         false,
//         "http://localhost:8081",
//     );
//
//     let producer_config = ClientConfig::new()
//         .set("bootstrap.servers", "localhost:9092")
//         .set("message.timeout.ms", "5000")
//         .to_owned();
//     let data_sink = KafkaAvroSink::new(
//         sink_topic,
//         Person::get_schema(),
//         producer_config,
//         100000,
//         "http://localhost:8081",
//     )
//     .await;
//
//     let data_transformation = SomeTransform {
//         list: vec!["Hey".to_string()],
//     };
//
//     let creator = StreamCreatorBuilder::new(data_source, data_transformation, data_sink)
//         .with_auto_max_tasks()
//         .await
//         .build();
//
//     let streams = creator.create_streams().await;
//
//     // run test
//
//     run_streams(streams).await.unwrap();
//
//     // this is necessary until global shutdown is released for metric provider
//     meter_provider.shutdown().unwrap();
//     shutdown_tracer_provider();
// }
//
// #[tracing::instrument(skip(kafka_sink))]
// async fn run_producers(kafka_sink: KafkaAvroSink, num_producers: u32, num_msgs: u32) {
//     let start = Instant::now();
//
//     let mut hs = Vec::new();
//     for _ in 0..num_producers {
//         let kafka_sink = kafka_sink.clone();
//         let batch_size = kafka_sink.batch_size;
//         let h = tokio::spawn(async move {
//             let kafka_sink = kafka_sink.clone();
//             let mut futs = Vec::new();
//             for n in 0..num_msgs {
//                 let person = get_test_person();
//                 futs.push(person);
//                 if futs.len() as u32 == batch_size || n == num_msgs - 1 {
//                     kafka_sink.write_batch(&futs).await.unwrap();
//                 }
//             }
//         });
//         hs.push(h);
//     }
//     join_all(hs).await;
//
//     let end = start.elapsed().as_millis();
//     info!("Time elapsed for Test producers: {}", end);
// }
//
// async fn create_topics(source_topic: &str, sink_topic: &str) {
//     let config = ClientConfig::new()
//         .set("bootstrap.servers", "localhost:9092")
//         .set("message.timeout.ms", "5000")
//         .to_owned();
//
//     let admin_client: AdminClient<DefaultClientContext> = config.create().expect("");
//
//     let topic_in = NewTopic {
//         name: source_topic,
//         num_partitions: 3,
//         replication: TopicReplication::Fixed(1),
//         config: vec![],
//     };
//
//     let topic_out = NewTopic {
//         name: sink_topic,
//         num_partitions: 3,
//         replication: TopicReplication::Fixed(1),
//         config: vec![],
//     };
//
//     let results = admin_client
//         .create_topics(&[topic_in, topic_out], &Default::default())
//         .await
//         .expect("Topic creation failed");
//
//     for result in results {
//         match result {
//             Ok(_) => {}
//             Err(e) => {
//                 warn!("Error: {}, {}", e.0, e.1)
//             }
//         }
//     }
// }
//
// async fn prepare_test_data(source_topic: &str, sink_topic: &str) {
//     create_topics(source_topic, sink_topic).await;
//
//     // prepare test data data
//     let producer_config = ClientConfig::new()
//         .set("bootstrap.servers", "localhost:9092")
//         .set("message.timeout.ms", "5000")
//         .to_owned();
//
//     let test_sink = KafkaAvroSink::new(
//         source_topic,
//         Person::get_schema(),
//         producer_config,
//         100000,
//         "http://localhost:8081",
//     )
//     .await;
//     run_producers(test_sink, 3, 10001).await;
// }
//
// fn get_test_consumer_config() -> ClientConfig {
//     ClientConfig::new()
//         .set("group.id", "test")
//         .set("bootstrap.servers", "localhost:9092")
//         .set("enable.partition.eof", "false")
//         .set("session.timeout.ms", "6000")
//         .set("statistics.interval.ms", "30000")
//         .set("auto.offset.reset", "smallest")
//         .set_log_level(RDKafkaLogLevel::Debug)
//         .to_owned()
// }
