use crate::container::MsgContainer;
use std::sync::Arc;

pub mod convert;
pub mod deserialize;
pub mod scan;
pub mod transform;
pub mod write;

// todo impl structs which implement thsi trait and build some recursive structure which defines the data pipeline in the end
pub trait Action {
    fn execute(&self) -> Arc<dyn MsgContainer>;

    fn child(&self) -> Option<Arc<dyn Action>>;
}
