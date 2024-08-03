use crate::action_plan::ActionPlan;
use crate::container::{Batch, BatchContainer, GenericBatchContainer};
use crate::data_sink::DataSink;
use crate::type_definitions::{MixedBatch, UniformBatch};
use std::any::Any;
use std::sync::Arc;

pub struct Write {
    input: Arc<dyn ActionPlan>,
    data_sink: Arc<dyn DataSink>,
}

impl Write {
    pub fn new(input: Arc<dyn ActionPlan>, data_sink: Arc<dyn DataSink>) -> Arc<Self> {
        Arc::new(Write { input, data_sink })
    }
}

impl ActionPlan for Write {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let input = self.input.execute();
        let _ = Box::new(input.map(move |container| self.data_sink.write(container)));
        self.commit_batch();

        // todo let write return containers, which keeps error items (maybe a new batch type), these erros can then be handles together or collected etc
        let cc: Arc<dyn BatchContainer> =
            GenericBatchContainer::new(Arc::from(Batch::Mixed(MixedBatch::Any(vec![]))), None);
        Box::new(vec![cc].into_iter())
    }

    fn child(&self) -> Option<Arc<dyn ActionPlan>> {
        Option::from(self.input.clone())
    }

    fn commit_batch(&self) {
        self.child().unwrap().commit_batch()
    }
}
