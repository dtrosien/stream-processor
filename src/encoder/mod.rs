pub mod avro_sr_encoder;

use crate::container::BatchContainer;
use crate::encoder::avro_sr_encoder::AvroSREncoder;
use serde::Serialize;
use std::ops::Deref;
use std::sync::Arc;

// trait is not possible because impl Serialize is required in Encoder.
pub enum Encoder {
    AvroSREncoder(Arc<AvroSREncoder>),
}

impl Encoder {
    pub fn get_avro_rs_encoder(&self) -> Option<Arc<AvroSREncoder>> {
        match self {
            Encoder::AvroSREncoder(encoder) => Some(encoder.clone()),
        }
    }
}
