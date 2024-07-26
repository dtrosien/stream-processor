mod kafka_producer;

use crate::container::MsgContainer;
use std::sync::Arc;

pub trait DataSink {
    fn write(&self, input: Arc<dyn MsgContainer>);
}
