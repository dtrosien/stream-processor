pub mod avro_sr_decoder;

use crate::container::BatchContainer;
use std::sync::Arc;

pub trait Decoder {
    fn decode(
        &self,
        msg: Arc<dyn BatchContainer>,
    ) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;
}
