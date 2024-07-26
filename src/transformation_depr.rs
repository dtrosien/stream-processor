use std::future::Future;

/// Trait defining the transformation logic between input and output data types.
pub trait Transformation<I, O>: Clone {
    /// Transforms input data of type I to output data of type O.
    fn apply(&mut self, data: I) -> impl Future<Output = O>;
}
