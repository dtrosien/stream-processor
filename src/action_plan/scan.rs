use crate::action_plan::ActionPlan;
use crate::container::MsgContainer;
use crate::data_source::DataSource;
use std::sync::Arc;

pub struct Scan {
    data_source: Arc<dyn DataSource>,
}

impl Scan {
    pub fn new(data_source: Arc<dyn DataSource>) -> Arc<Self> {
        Arc::new(Scan { data_source })
    }
}

impl ActionPlan for Scan {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn MsgContainer>> + '_> {
        self.data_source.read_batch()
    }

    fn child(&self) -> Option<Arc<dyn ActionPlan>> {
        None
    }

    fn commit_batch(&self) {
        self.data_source.commit();
    }
}
