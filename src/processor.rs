use crate::sink::Sink;
use crate::source::{Source, SourceError};
use crate::stream::ReadMode;
use crate::transformation::Transformation;
use opentelemetry::{global, KeyValue};
use std::cmp::min;
use std::marker::PhantomData;
use tracing::{info, Instrument, Span};

/// Handles the actual data processing
/// The StreamCreator spawns one Processor per tasks
pub struct Processor<I, O, So, T, Si>
where
    So: Source<I>,
    T: Transformation<I, O>,
    Si: Sink<O>,
{
    source: So,
    transformation: T,
    sink: Si,
    _marker_i: PhantomData<I>,
    _marker_o: PhantomData<O>,
}

impl<I, O, So, T, Si> Processor<I, O, So, T, Si>
where
    So: Source<I>,
    T: Transformation<I, O>,
    Si: Sink<O>,
{
    pub fn new(source: So, transformation: T, sink: Si) -> Self {
        Processor {
            source,
            transformation,
            sink,
            _marker_i: PhantomData,
            _marker_o: PhantomData,
        }
    }

    /// Runs the process loop based on the Read Mode
    ///  Important: in limited mode, each source reads n items
    #[tracing::instrument(skip_all)]
    pub(crate) async fn process(&mut self) -> anyhow::Result<()> {
        self.source.initialize().await?;
        let read_mode = self.source.get_read_mode().clone();
        match read_mode {
            ReadMode::Continuously => {
                let batch_size = self.sink.get_batch_size();
                self.loop_over_next_items(None, batch_size).await?;
            }
            ReadMode::Limited { n } => {
                let batch_size = min(n, self.sink.get_batch_size());
                self.loop_over_next_items(Some(n), batch_size).await?;
            }
            ReadMode::Once => {
                let mut batch: Vec<O> = Vec::new();
                self.process_next_item(1, &mut batch).await?
            }
        }
        Ok(())
    }

    /// calls process_next in a loop, which stops when provided a max_iteration via the Limited Read mode
    /// or if the stop_at_stream_end is enabled
    /// IMPORTANT: Stream end (Partition End) is determined via the SourceError::EndOfPartitionError,
    /// so all sources must trigger this error when their input end is reached
    /// todo figure out a good way to force Source implementations to support this.
    async fn loop_over_next_items(
        &mut self,
        max_iterations: Option<u32>,
        batch_size: u32,
    ) -> anyhow::Result<()> {
        let mut batch: Vec<O> = Vec::new();
        let mut iteration: u32 = 0;
        let stop_at_stream_end = self.source.stop_at_stream_end();
        loop {
            // break loop if max iterations are reached
            if let Some(max_iterations) = max_iterations {
                if max_iterations <= iteration {
                    info!("max iterations reached: {}", iteration);
                    return Ok(());
                }
                iteration += 1;
            }
            // process next item
            match self.process_next_item(batch_size, &mut batch).await {
                Ok(_) => {}
                Err(ProcessorError::SourceError(SourceError::EndOfPartitionError)) => {
                    if stop_at_stream_end {
                        self.source.commit().await?;
                        return Ok(());
                    }
                }
                Err(e) => return Err(anyhow::Error::from(e)),
            };
        }
    }

    ///  reads the next item from the source, transforms it at and add it to the sink batch.
    /// When the sink reaches the max batch size or the source reaches the end of a partition,
    /// the batch gets flushed and the source commits the stored offsets
    ///
    async fn process_next_item(
        &mut self,
        batch_size: u32,
        batch: &mut Vec<O>,
    ) -> anyhow::Result<(), ProcessorError> {
        match self
            .source
            .consume_next_item()
            .instrument(Span::current())
            .await
        {
            Ok(input) => {
                self.transform_and_write(batch_size, batch, input, false)
                    .await
            }
            Err(SourceError::EndOfPartitionError) => {
                self.transform_and_write(batch_size, batch, None, true)
                    .await?;
                Err(ProcessorError::SourceError(
                    SourceError::EndOfPartitionError,
                ))
            }
            Err(e) => Err(ProcessorError::SourceError(e)),
        }
    }

    /// takes input of type <I> which gets transformed from the transformer and then written in batches via the sink
    /// a batch is written when the max batch size is reached or it is the final batch (at end of partition).
    /// This can also happen multiple times when running continuously and new data comes in after some time
    /// after a batch is written, the source commits its stored state
    async fn transform_and_write(
        &mut self,
        batch_size: u32,
        batch: &mut Vec<O>,
        input: Option<I>,
        is_final_batch: bool,
    ) -> anyhow::Result<(), ProcessorError> {
        if let Some(input) = input {
            let out = self.transformation.apply(input).await;
            batch.push(out);
        }

        let current_batch = batch.len() as u32;
        if (current_batch == batch_size) || is_final_batch {
            self.sink.write_batch(batch).await?;
            batch.clear();
            self.source
                .commit()
                .await
                .map_err(ProcessorError::SourceError)?;
            send_batch_metrics(current_batch);
            info!("committed batch: {}", current_batch);
        }
        Ok(())
    }
}

fn send_batch_metrics(batch_size: u32) {
    let meter = global::meter("sink");
    let counter = meter.u64_counter("batch-output").init();
    counter.add(
        batch_size as u64,
        &[KeyValue::new("some_key", "some_value")],
    );
    // todo check what observable counters can do and do not init everytime when calling
}

#[derive(thiserror::Error)]
pub enum ProcessorError {
    #[error("Something went wrong")]
    UnexpectedError(#[from] anyhow::Error),
    #[error("Source failed")]
    SourceError(#[source] SourceError),
}

impl std::fmt::Debug for ProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        error_chain_fmt(self, f)
    }
}

pub fn error_chain_fmt(
    e: &impl std::error::Error,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    writeln!(f, "{}\n", e)?;
    let mut current = e.source();
    while let Some(cause) = current {
        writeln!(f, "Caused by:\n\t{}", cause)?;
        current = cause.source();
    }
    Ok(())
}
