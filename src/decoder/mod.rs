mod avro_sr_decoder;

use crate::container::MsgContainer;
use std::sync::Arc;

pub trait Decoder {
    fn decode(&self, msg: Arc<dyn MsgContainer>) -> Arc<dyn MsgContainer>;
}
