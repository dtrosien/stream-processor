use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::MixedBatch;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

pub struct KafkaConsumer {
    consumer: BaseConsumer,
    client_config: ClientConfig,
    batch_size: usize,
    topics: Vec<String>,
    timeout: Timeout,
}

impl KafkaConsumer {
    pub fn new(
        mut client_config: ClientConfig,
        batch_size: usize,
        topics: &[&str],
        timeout: Timeout,
    ) -> Arc<Self> {
        let client_config = client_config
            // mandatory because the consumer stores its state when data was written from the writer and flushes it afterwards
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            // mandatory to be able to stop at the end of the partition
            .set("enable.partition.eof", "true")
            .to_owned();

        let consumer: BaseConsumer = client_config
            .create()
            .expect("Failed to create Kafka consumer");

        consumer // todo maybe only subscribe in recreation function
            .subscribe(topics)
            .expect(&format!("Failed to subscribe to topics: {:?}", topics));
        Arc::new(KafkaConsumer {
            consumer,
            client_config,
            batch_size,
            topics: topics
                .into_iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>(),
            timeout,
        })
    }
}

impl DataSource for KafkaConsumer {
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let msg = (0..self.batch_size)
            .into_iter()
            .filter_map(|_| self.consumer.poll(self.timeout))
            .take_while(|kr| {
                match kr {
                    Err(KafkaError::PartitionEOF(part)) => {
                        println!("Consumer reached end of partition: {}", part);
                        info!("Consumer reached end of partition: {}", part);
                        // todo send trigger to stop stream (then the timeout should not be required in real world usage)
                        false // Stop consuming further
                    }
                    Err(e) => {
                        error!("Kafka error: {}", e);
                        panic!("Kafka error: {}", e) // todo proper error handling
                    }
                    Ok(_) => true, // Continue consuming on valid messages
                }
            })
            .filter_map(|kr| {
                kr.ok().and_then(|m| {
                    self.consumer.store_offset_from_message(&m).unwrap();
                    m.payload().map(|pl| Arc::new(pl.to_vec()))
                })
            })
            .collect::<Vec<_>>();

        let batch = Arc::new(Batch::Mixed(MixedBatch::Bytes(msg)));
        let container: Arc<dyn BatchContainer> =
            GenericBatchContainer::new(batch, None, HashMap::default());
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {
        match self.consumer.commit_consumer_state(CommitMode::Sync) {
            Ok(_) => {}
            Err(KafkaError::ConsumerCommit(e)) => {
                info!("{}", e.to_string())
            }
            Err(_) => {
                panic!("Unexpected error while commiting")
            }
        }
    }

    fn get_partitions(&self) -> Vec<String> {
        self.topics
            .iter()
            .flat_map(|topic| {
                self.consumer
                    .fetch_metadata(Some(topic), None)
                    .map_err(|e| error!("{}", e))
                    .expect("Failed to fetch metadata")
                    .topics()
                    .iter()
                    .flat_map(|t| t.partitions())
                    .map(|p| p.id().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    fn recreate_partitioned(&self, partition: String) -> Arc<dyn DataSource> {
        // no need to handle partitions manually, since kafka distributed them evenly to all consumers of one group
        KafkaConsumer::new(
            self.client_config.clone(),
            self.batch_size,
            self.topics
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice(),
            self.timeout.clone(),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::container::Batch;
    use crate::data_source::kafka_consumer::KafkaConsumer;
    use crate::data_source::DataSource;
    use crate::type_definitions::MixedBatch;
    use chrono::Utc;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
    use rdkafka::util::Timeout;
    use rdkafka::{ClientConfig, ClientContext, Message};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tracing::{error, warn};

    #[test]
    fn read_from_kafka() {
        const TOPIC: &str = "test_topic";

        let mock_cluster = MockCluster::new(1).unwrap();
        mock_cluster
            .create_topic(TOPIC, 3, 1)
            .expect("Failed to create topic");

        let num_test_data = 20_usize;
        create_test_data(TOPIC, &mock_cluster.bootstrap_servers(), num_test_data);

        let consumer_config = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .set("group.id", "consumer_test")
            .set("auto.offset.reset", "earliest")
            .to_owned();

        let source = KafkaConsumer::new(
            consumer_config,
            6,
            &[TOPIC],
            Timeout::After(Duration::from_millis(400)),
        );

        let mut all_read_items = 0;
        for i in 0..6 {
            let batch = source.read_batch().next().unwrap().get_batch();

            if let Batch::Mixed(MixedBatch::Bytes(items)) = batch.as_ref() {
                all_read_items += items.len();
                source.commit();
            };
        }

        assert_eq!(all_read_items, num_test_data)
    }

    #[test]
    fn read_partitions() {
        const TOPIC: &str = "test_topic";

        let num_partitions = 33;
        let mock_cluster = MockCluster::new(1).unwrap();
        mock_cluster
            .create_topic(TOPIC, num_partitions, 1)
            .expect("Failed to create topic");

        let consumer_config = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .set("group.id", "consumer_test")
            .set("auto.offset.reset", "earliest")
            .to_owned();

        let source = KafkaConsumer::new(
            consumer_config,
            6,
            &[TOPIC],
            Timeout::After(Duration::from_millis(500)),
        );

        assert_eq!(source.get_partitions().len(), num_partitions as usize)
    }

    #[test]
    fn recreate_from_existing() {
        const TOPIC: &str = "test_topic";

        let mock_cluster = MockCluster::new(1).unwrap();
        mock_cluster
            .create_topic(TOPIC, 3, 1)
            .expect("Failed to create topic");

        let num_test_data = 20_usize;

        create_test_data(TOPIC, &mock_cluster.bootstrap_servers(), num_test_data);

        let consumer_config = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .set("group.id", "consumer_test")
            .set("auto.offset.reset", "earliest")
            .to_owned();

        let source1 = KafkaConsumer::new(
            consumer_config,
            6,
            &[TOPIC],
            Timeout::After(Duration::from_millis(400)),
        ) as Arc<dyn DataSource>;

        let source2 = source1.recreate_partitioned("".to_string());

        let sources = vec![source1, source2];

        let mut all_read_items = 0;

        for i in 0..4 {
            for source in sources.iter() {
                let batch = source.read_batch().next().unwrap().get_batch();
                if let Batch::Mixed(MixedBatch::Bytes(items)) = batch.as_ref() {
                    all_read_items += items.len();
                    source.commit();
                };
            }
        }

        assert_eq!(all_read_items, num_test_data)
    }

    fn create_test_data(topic: &str, bootstrap_servers: &str, num: usize) {
        let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
            .set("message.timeout.ms", "5000")
            .set("bootstrap.servers", bootstrap_servers)
            .create()
            .expect("Producer creation error");

        for i in 0..num {
            let msg = format!("dummy_{i}");
            let i = i.to_string();
            let mut record = BaseRecord::to(topic)
                .key(&i)
                .payload(&msg)
                .timestamp(Utc::now().timestamp_millis());

            loop {
                match producer.send(record) {
                    Ok(()) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rec)) => {
                        // Retry after 500ms
                        warn!("Queue full, retrying...");
                        record = rec;
                        thread::sleep(Duration::from_millis(500));
                    }
                    Err((e, _)) => {
                        error!("Failed to publish on kafka {:?}", e);
                        break;
                    }
                }
            }
        }
    }

    // fn create_topics(source_topic: &str, sink_topic: &str) {
    //     let rt = tokio::runtime::Runtime::new().unwrap();
    //
    //     rt.block_on(async {
    //         // Your async code here
    //         println!("Running async code in a non-async function");
    //         let config = ClientConfig::new()
    //             .set("bootstrap.servers", "localhost:9092")
    //             .set("message.timeout.ms", "5000")
    //             .to_owned();
    //
    //         let admin_client: AdminClient<DefaultClientContext> = config.create().expect("");
    //
    //         let topic_in = NewTopic {
    //             name: source_topic,
    //             num_partitions: 3,
    //             replication: TopicReplication::Fixed(1),
    //             config: vec![],
    //         };
    //
    //         let topic_out = NewTopic {
    //             name: sink_topic,
    //             num_partitions: 3,
    //             replication: TopicReplication::Fixed(1),
    //             config: vec![],
    //         };
    //
    //         let results = admin_client
    //             .create_topics(&[topic_in, topic_out], &Default::default())
    //             .await
    //             .expect("Topic creation failed");
    //
    //         for result in results {
    //             match result {
    //                 Ok(_) => {}
    //                 Err(e) => {
    //                     warn!("Error: {}, {}", e.0, e.1)
    //                 }
    //             }
    //         }
    //     });
    // }
}
