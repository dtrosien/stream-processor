use crate::action::Action;
use std::sync::Arc;

pub trait Stream {
    /// Apply Serialization
    fn deserialize(self: Arc<Self>) -> Arc<dyn Stream>;

    fn convert(self: Arc<Self>) -> Arc<dyn Stream>;
    fn transform(self: Arc<Self>) -> Arc<dyn Stream>;
    fn write(self: Arc<Self>) -> Arc<dyn Stream>;

    /// Get the Action
    fn action(self: Arc<Self>) -> Arc<dyn Action>;
}
