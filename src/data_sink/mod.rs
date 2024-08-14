pub mod kafka_producer;

use crate::container::BatchContainer;
use std::sync::Arc;

pub trait DataSink {
    fn write(
        &self,
        input: Arc<dyn BatchContainer>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;
}
