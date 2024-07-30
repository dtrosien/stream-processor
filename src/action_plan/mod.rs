use crate::container::BatchContainer;
use crate::data_source::DataSource;
use std::any::Any;
use std::sync::Arc;

pub mod convert;
pub mod deserialize;
pub mod scan;
pub mod transform;
pub mod write;

// todo impl structs which implement thsi trait and build some recursive structure which defines the data pipeline in the end
pub trait ActionPlan {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;

    fn child(&self) -> Option<Arc<dyn ActionPlan>>;

    /// used to be able to commit when necessary
    fn commit_batch(&self);
}
