use crate::container::BatchContainer;
use crate::data_source::DataSource;
use std::any::Any;
use std::sync::Arc;

pub mod deserialize;
pub mod scan;
pub mod transform;
pub mod write;

pub trait ActionPlan {
    fn execute(&self) -> Box<dyn Iterator<Item = Arc<dyn BatchContainer>> + '_>;

    fn child(&self) -> Option<Arc<dyn ActionPlan>>;

    /// used to be able to commit when necessary
    fn commit_batch(&self);

    fn get_partitions(&self) -> Vec<String>;

    fn as_any(&self) -> &dyn Any;
}
