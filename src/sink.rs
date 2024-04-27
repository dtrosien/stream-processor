use std::future::Future;

/// Trait which defines how a specific Datatype <O> should be written to a sink
pub trait Sink<O>: Clone {
    /// handles Msgs from Kafka based on the specific implementation and returns them as a concrete type <T>
    fn write_batch(&self, data: &[O]) -> impl Future<Output = anyhow::Result<()>>;

    /// Retrieves the batch size for writing.
    fn get_batch_size(&self) -> u32;
}
