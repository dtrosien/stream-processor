use crate::action_plan::ActionPlan;
use crate::container::MsgContainer;
use crate::data_sink::DataSink;
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
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn MsgContainer>> + '_> {
        let input = self.input.execute();
        let _ = Box::new(input.map(move |container| self.data_sink.write(container)));
        self.commit_batch();
        todo!()
    }

    fn child(&self) -> Option<Arc<dyn ActionPlan>> {
        Option::from(self.input.clone())
    }

    fn commit_batch(&self) {
        self.child().unwrap().commit_batch()
    }
}
