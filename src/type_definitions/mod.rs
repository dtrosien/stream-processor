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
    Custom(Vec<Arc<dyn CustomType>>),
}

#[derive(Clone, Debug)]
pub enum MixedBatch {
    Bytes(Vec<Arc<Vec<u8>>>),
    Any(Vec<Arc<dyn Any>>),
}

pub trait CustomType: Debug {
    fn get_name(self: Arc<Self>) -> String;
}

// todo wie kann man das in einer API klar machen dass es im Mapper und Converter custom gecoded werden muss
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum CustomTypes {
    A,
    B,
}
