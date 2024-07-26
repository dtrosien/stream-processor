mod avro_sr_encoder;

use crate::container::MsgContainer;
use serde::Serialize;
use std::sync::Arc;

pub trait Encoder {
    fn encode(&self, item: impl Serialize) -> Option<Arc<dyn MsgContainer>>;
}
