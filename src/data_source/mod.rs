mod kafka_consumer;

use crate::container::MsgContainer;
use rdkafka::Message;
use std::any::Any;
use std::sync::Arc;

pub trait DataSource {
    fn read_batch(&self) -> Box<dyn Iterator<Item = Arc<dyn MsgContainer>> + '_>; // todo mal shehen ob batch als standard gut ist. ansonsten halt als single
}
