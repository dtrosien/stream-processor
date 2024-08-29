use crate::action_plan::ActionPlan;
use crate::container::BatchContainer;
use crate::data_source::DataSource;
use std::any::Any;
use std::sync::Arc;

pub struct Scan {
    pub data_source: Arc<dyn DataSource>,
    pub partition: Option<String>,
}

impl Scan {
    pub fn new(data_source: Arc<dyn DataSource>) -> Arc<Self> {
        Arc::new(Scan {
            data_source,
            partition: None,
        })
    }

    pub fn new_with_partition(
        data_source: Arc<dyn DataSource>,
        partition: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Scan {
            data_source,
            partition,
        })
    }
}

impl ActionPlan for Scan {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_> {
        self.data_source.read_batch()
    }

    fn child(&self) -> Option<Arc<dyn ActionPlan>> {
        None
    }

    fn commit_batch(&self) {
        self.data_source.commit();
    }
    fn get_partitions(&self) -> Vec<String> {
        self.data_source.get_partitions()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
