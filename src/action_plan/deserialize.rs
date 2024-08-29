use crate::action_plan::ActionPlan;
use crate::container::BatchContainer;
use crate::decoder::Decoder;
use std::any::Any;
use std::sync::Arc;

pub struct Deserialize {
    pub input: Arc<dyn ActionPlan>,
    pub decoder: Arc<dyn Decoder>,
}

impl Deserialize {
    pub fn new(input: Arc<dyn ActionPlan>, decoder: Arc<dyn Decoder>) -> Arc<Self> {
        Arc::new(Deserialize { input, decoder })
    }
}
impl ActionPlan for Deserialize {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        let input = self.input.execute();
        Box::new(input.flat_map(move |container| self.decoder.decode(container)))
    }

    fn child(&self) -> Option<Arc<dyn ActionPlan>> {
        Option::from(self.input.clone())
    }

    fn commit_batch(&self) {
        self.child().unwrap().commit_batch()
    }

    fn get_partitions(&self) -> Vec<String> {
        self.child().unwrap().get_partitions()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
