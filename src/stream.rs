use crate::processor::Processor;
use crate::sink::Sink;
use crate::source::Source;
use crate::transformation::Transformation;
use futures_util::future::try_join_all;
use std::future::Future;
use std::marker::PhantomData;
use tracing::info;

/// Builds a StreamCreator composed of multiple processors containing each a source, transformer, and sink.
/// The output type of the source must match the input type of the transformer's apply method,
/// and the output of the apply method must match the input type of the sink.
pub struct StreamCreatorBuilder<I, O, So, T, Si>
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
    max_tasks: u32,
}

impl<I, O, So, T, Si> StreamCreatorBuilder<I, O, So, T, Si>
where
    So: Source<I>,
    T: Transformation<I, O>,
    Si: Sink<O>,
{
    pub fn new(source: So, transformation: T, sink: Si) -> Self {
        StreamCreatorBuilder {
            source,
            transformation,
            sink,
            _marker_i: PhantomData,
            _marker_o: PhantomData,
            max_tasks: 1,
        }
    }

    /// Sets the maximum number of parallel tasks.
    pub fn with_max_tasks(&mut self, max_tasks: u32) -> &mut Self {
        self.max_tasks = max_tasks;
        self
    }

    /// Automatically determines the optimal number of parallel tasks based on the source's capabilities.
    pub async fn with_auto_max_tasks(&mut self) -> &mut Self {
        self.max_tasks =
            StreamCreator::<I, O, So, T, Si>::derive_parallel_tasks(&self.source).await;
        self
    }

    /// Builds the StreamCreator.
    pub fn build(&self) -> StreamCreator<I, O, So, T, Si> {
        StreamCreator {
            source: self.source.to_owned(),
            sink: self.sink.to_owned(),
            transformation: self.transformation.to_owned(),
            _marker_i: PhantomData,
            _marker_o: PhantomData,
            max_tasks: self.max_tasks,
        }
    }
}

/// Defines how often data should be polled from the source
#[derive(Clone)]
pub enum ReadMode {
    Continuously,
    Limited { n: u32 },
    Once,
}

/// The StreamCreator initializes the single processors for each of the streaming tasks
/// and builds the actual streams, which can be awaited via the run_streams function
pub struct StreamCreator<I, O, So, T, Si>
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
    max_tasks: u32,
}

impl<I, O, So, T, Si> StreamCreator<I, O, So, T, Si>
where
    So: Source<I>,
    T: Transformation<I, O>,
    Si: Sink<O>,
{
    /// consumes creator to create processing streams up to the number of max tasks
    pub async fn create_streams(self) -> Vec<impl Future<Output = anyhow::Result<()>>> {
        let mut streams = Vec::new();

        for i in 0..self.max_tasks {
            info!("Prepare stream no. {}", i);
            let source = self.source.clone();
            let sink = self.sink.clone();
            let transformation = self.transformation.clone();
            let mut processor = Processor::new(source, transformation, sink);
            let stream = async move { processor.process().await };
            streams.push(stream)
        }
        streams
    }

    pub async fn derive_parallel_tasks(source: &So) -> u32
    where
        So: Source<I>,
    {
        source.derive_max_parallel_tasks().await
    }
}

/// runs the streams created with the StreamCreator in separate tokio tasks and awaits them concurrently
#[tracing::instrument(skip_all)]
pub async fn run_streams(
    streams: Vec<impl Future<Output = anyhow::Result<()>> + Send + 'static>,
) -> anyhow::Result<()> {
    let mut streaming_tasks = Vec::new();
    for stream in streams {
        let streaming_task = tokio::spawn(async move {
            match stream.await {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        });
        streaming_tasks.push(streaming_task);
    }

    try_join_all(streaming_tasks).await?;
    Ok(())
}
