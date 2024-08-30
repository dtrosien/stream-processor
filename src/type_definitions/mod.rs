use apache_avro::types::Value;
use arrow::array::RecordBatch;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum UniformBatch {
    Bytes(Vec<Arc<Vec<u8>>>),
    RecordBatch(RecordBatch),
    AvroValue(Vec<Arc<Value>>),
    Custom(Vec<Arc<dyn Any + Send + Sync>>),
}

#[derive(Clone, Debug)]
pub enum MixedBatch {
    Bytes(Vec<Arc<Vec<u8>>>),
    Any(Vec<Arc<dyn Any + Send + Sync>>),
}

#[derive(Clone, Debug)]
pub enum ErrorBatch {
    Any(Vec<Arc<dyn Any + Send + Sync>>),
}

pub trait CustomType: Debug + Any {
    fn get_type_name(&self) -> String;
    fn get_type_id(&self) -> u32;
}
