use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_source::DataSource;
use crate::type_definitions::MixedBatch;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::{ClientConfig, Message};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

pub struct KafkaConsumer {
    consumer: BaseConsumer,
    client_config: ClientConfig,
    batch_size: usize,
    topics: Vec<String>,
}

impl KafkaConsumer {
    pub fn new(mut client_config: ClientConfig, batch_size: usize, topics: &[&str]) -> Arc<Self> {
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
        })
    }
}

impl DataSource for KafkaConsumer {
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let msg = self
            .consumer
            .iter()
            .take(self.batch_size)
            .filter_map(|kr| {
                match kr {
                    Err(KafkaError::PartitionEOF(part)) => {
                        info!("Consumer reached end of partition: {}", part);
                        None
                    }
                    Err(e) => {
                        error!("Kafka error: {}", e);
                        panic!("Kafka error: {}", e) // todo proper error handling
                    }
                    Ok(m) => m.payload().and_then(|pl| {
                        self.consumer.store_offset_from_message(&m).unwrap();
                        Some(Arc::new(pl.to_vec()))
                    }),
                }
            })
            .collect::<Vec<_>>();

        let batch = Arc::new(Batch::Mixed(MixedBatch::Bytes(msg)));
        let container: Arc<dyn BatchContainer> =
            GenericBatchContainer::new(batch, None, HashMap::default());
        Box::new(vec![container].into_iter())
    }

    fn commit(&self) {
        self.consumer
            .commit_consumer_state(CommitMode::Sync)
            .unwrap()
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
        )
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_something() {
        // todo use kafka mock cluster here for testing
    }
}
