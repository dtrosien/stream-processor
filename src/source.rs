use crate::processor::error_chain_fmt;
use crate::stream::ReadMode;
use futures::Stream;
use std::future::Future;

/// Trait which defines how a specific Datatype <T> is read from a data source
pub trait Source<I>: Clone {
    /// handles Msgs from Kafka based on the specific implementation and returns them as a concrete type <T>
    fn consume_next_item(&mut self)
        -> impl Future<Output = anyhow::Result<Option<I>, SourceError>>;

    /// Retrieves the current reading mode of the consumer.
    fn get_read_mode(&mut self) -> &ReadMode;

    /// ready up consumer
    fn initialize(&self) -> impl Future<Output = anyhow::Result<(), SourceError>>;

    /// Defines the maximum number of parallel tasks that can be handled.
    fn derive_max_parallel_tasks(&self) -> impl Future<Output = u32>;

    /// Commits the current state of the consumer, typically after successfully processing a batch.
    fn commit(&mut self) -> impl Future<Output = anyhow::Result<(), SourceError>>;

    /// Returns a stream of consumed data.
    fn stream(&mut self) -> impl Future<Output = impl Stream>;

    /// Determines if the consumer should stop consuming when the end of the stream is reached.
    fn stop_at_stream_end(&self) -> bool;
}

#[derive(thiserror::Error)]
pub enum SourceError {
    #[error("End of partition encountered")]
    EndOfPartitionError,
    #[error("Something went wrong")]
    UnexpectedError(#[from] anyhow::Error),
    #[error("Initialization failed")]
    InitializationError(#[source] anyhow::Error),
    #[error("Commit failed")]
    CommitError(#[source] anyhow::Error),
    #[error("Deserialization failed")]
    DeserializationError(#[source] anyhow::Error),
}

impl std::fmt::Debug for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(self, f)
    }
}
