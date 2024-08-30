pub mod dummy_source;
pub mod kafka_consumer;

use crate::container::BatchContainer;
use rdkafka::Message;
use std::any::Any;
use std::sync::Arc;

pub trait DataSource: Send + Sync {
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>; // todo mal shehen ob batch als standard gut ist. ansonsten halt als single

    fn commit(&self);

    fn get_partitions(&self) -> Vec<String>;

    fn recreate_partitioned(&self, partition: String) -> Arc<dyn DataSource>;
}
