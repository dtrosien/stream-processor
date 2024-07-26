pub mod avro_sr_encoder;

use crate::container::MsgContainer;
use crate::encoder::avro_sr_encoder::AvroSREncoder;
use serde::Serialize;

pub enum Encoder {
    AvroSREncoder(AvroSREncoder),
}
