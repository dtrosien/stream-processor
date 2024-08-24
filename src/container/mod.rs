use crate::type_definitions::{ErrorBatch, MixedBatch, UniformBatch};
use std::collections::HashMap;
use std::sync::Arc;

pub trait BatchContainer {
    fn get_batch(self: Arc<Self>) -> Arc<Batch>;
    fn get_type_name(self: Arc<Self>) -> Option<String>;

    fn get_meta(self: Arc<Self>, key: &str) -> Option<String>;
}
// todo check from time to time if all required functions are in trait and remove unused ones
// todo maybe include batch infos for commit (meta_hashmap can be used for example)

pub enum Batch {
    Uniform(UniformBatch),
    Mixed(MixedBatch),
    Error(ErrorBatch),
}

pub struct GenericBatchContainer {
    batch: Arc<Batch>,
    batch_type_name: Option<String>,
    metadata: HashMap<String, String>, // todo maybe use vec<string> as value ... e.g handy for multiple topic etc. (if more such cases arise)
}

impl GenericBatchContainer {
    pub fn new(
        batch: Arc<Batch>,
        batch_type_name: Option<String>,
        metadata: HashMap<String, String>,
    ) -> Arc<Self> {
        Arc::new(GenericBatchContainer {
            batch,
            batch_type_name,
            metadata,
        })
    }
}

impl BatchContainer for GenericBatchContainer {
    fn get_batch(self: Arc<Self>) -> Arc<Batch> {
        self.batch.clone()
    }

    fn get_type_name(self: Arc<Self>) -> Option<String> {
        self.batch_type_name.clone()
    }

    fn get_meta(self: Arc<Self>, key: &str) -> Option<String> {
        self.metadata.get(key).cloned()
    }
}

struct ContainerBuilder {
    batch: Arc<Batch>,
    batch_type_name: Option<String>, // todo maybe include in map
    metadata: HashMap<String, String>,
}

impl ContainerBuilder {
    pub fn new(batch: Arc<Batch>) -> Self {
        ContainerBuilder {
            batch,
            batch_type_name: None,
            metadata: HashMap::default(),
        }
    }

    pub fn set_kafka_topic(&mut self, topic_name: &str) -> &mut Self {
        self.metadata
            .insert("kafka.topic".to_string(), topic_name.to_string());
        self
    }

    pub fn set_storage_path(&mut self, topic_name: &str) -> &mut Self {
        self.metadata
            .insert("storage.path".to_string(), topic_name.to_string());
        self
    }

    pub fn set_meta(&mut self, key: &str, value: &str) -> &mut Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn set_batch_type(&mut self, batch_type_name: &str) -> &mut Self {
        self.batch_type_name = Some(batch_type_name.to_string());
        self
    }

    pub fn build(self) -> Arc<GenericBatchContainer> {
        Arc::new(GenericBatchContainer {
            batch: self.batch,
            batch_type_name: self.batch_type_name,
            metadata: self.metadata,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::container::{Batch, ContainerBuilder};
    use crate::type_definitions::MixedBatch;
    use std::sync::Arc;

    #[test]
    fn build_container() {
        let batch = Arc::new(Batch::Mixed(MixedBatch::Any(vec![])));

        // let container = ContainerBuilder::new(batch)
        //     .set_batch_type("Any")
        //     .set_kafka_topic("SomeTopic")
        //     .set_storage_path("some/path")
        //     .build();
    }
}
