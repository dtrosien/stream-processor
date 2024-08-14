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
    Custom(Vec<Arc<dyn Any>>),
}

#[derive(Clone, Debug)]
pub enum MixedBatch {
    Bytes(Vec<Arc<Vec<u8>>>),
    Any(Vec<Arc<dyn Any>>),
}

#[derive(Clone, Debug)]
pub enum ErrorBatch {
    Any(Vec<Arc<dyn Any>>),
}

pub trait CustomType: Debug + Any {
    fn get_type_name(&self) -> String;
    fn get_type_id(&self) -> u32;
}

// todo move customtypes enum in integration tests

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum CustomTypes {
    StringMessage,
    B,
}

impl CustomType for CustomTypes {
    fn get_type_name(&self) -> String {
        match self {
            CustomTypes::StringMessage => "StringMessage".to_string(),
            CustomTypes::B => "B".to_string(),
        }
    }

    fn get_type_id(&self) -> u32 {
        match self {
            CustomTypes::StringMessage => 0,
            CustomTypes::B => 1,
        }
    }
}
