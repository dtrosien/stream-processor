pub mod dummy_sink;
pub mod kafka_producer;

use crate::container::BatchContainer;
use std::sync::Arc;

pub trait DataSink: Send + Sync {
    fn write(
        &self,
        input: Arc<dyn BatchContainer>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;
}
