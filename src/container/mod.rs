use crate::type_definitions::{ErrorBatch, MixedBatch, UniformBatch};
use arrow::array::RecordBatch;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub trait BatchContainer {
    fn get_sink(self: Arc<Self>) -> Option<String>;
    fn get_batch(self: Arc<Self>) -> Arc<Batch>;
    fn get_batch_name(self: Arc<Self>) -> Option<String>;

    fn get_meta(self: Arc<Self>) -> Option<HashMap<String, String>>;
}

// todo maybe include batch infos for commit (meta_hashmap can be used for example)

pub struct GenericBatchContainer {
    batch: Arc<Batch>,
    batch_name: Option<String>,
}

pub enum Batch {
    Uniform(UniformBatch),
    Mixed(MixedBatch),
    Error(ErrorBatch),
}

impl GenericBatchContainer {
    pub fn new(batch: Arc<Batch>, batch_name: Option<String>) -> Arc<Self> {
        Arc::new(GenericBatchContainer { batch, batch_name })
    }
}

impl BatchContainer for GenericBatchContainer {
    fn get_sink(self: Arc<Self>) -> Option<String> {
        None
    }

    fn get_batch(self: Arc<Self>) -> Arc<Batch> {
        self.batch.clone()
    }

    fn get_batch_name(self: Arc<Self>) -> Option<String> {
        self.batch_name.clone()
    }

    fn get_meta(self: Arc<Self>) -> Option<HashMap<String, String>> {
        None
    }
}
