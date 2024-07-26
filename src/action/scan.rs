use crate::action::Action;
use crate::container::MsgContainer;
use crate::data_source::DataSource;
use std::sync::Arc;

pub struct Scan {
    data_source: Arc<dyn DataSource>,
}
impl Action for Scan {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn MsgContainer>> + '_> {
        self.data_source.read_batch()
    }

    fn child(&self) -> Option<Arc<dyn Action>> {
        None
    }
}
