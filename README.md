# stream-processor

The Streams API is a simple and lightweight Rust library that supports the construction of data streaming pipelines. It
is designed to implement simple streaming applications quickly and efficiently. It allows developers to define data
flows that consume data from a source, apply transformations, and return results to specified sinks. The data is
delivered at least once! The crate uses Opentelemetry to collect metrics and propagate span contexts via Kafka headers
and sending traces to otel-compatible collectors. The sources and sinks are predefined, requiring only configuration
adjustments to suit specific requirements. User logic is encapsulated in the `apply' method of the `Transformation'
trait.

Source -> Transformation -> Sink

## Example

The following example demonstrates setting up a streaming pipeline with Kafka as the data source and Azure as the data
sink:

```rust
// Initialize KafkaSource which does stop when reaching the end of the partitions
let data_source = KafkaAvroSource::new(
consumer_config, // RdKafka consumer config
CustomContext, // RdKafka consumer context
vec![source_topic],
ReadMode::Continuously,
None, // Schema will be filled later when calling the registry
true, // stop at partition end
false, // propagate span context via kafka headers
"http://localhost:8081",
);

// Initialize AzureSink which which writes to Azure in batches of 100k 
let data_sink = AzureSink::new(100000, "azure_int_test");

// simple transformation which prints the name of a Person struct
let data_transformation = PrintName;

// Initialize the StreamCreatorBuilder with the data source, transformation, and sink
let creator = StreamCreatorBuilder::new(data_source, data_transformation, data_sink)
.with_auto_max_tasks()
.await
.build();

// Create streams from the StreamCreator
let streams = creator.create_streams().await;

// Run the streams and handle the results
run_streams(streams).await.unwrap();
```

## Transformations

Transformations in the StreamProcessor API are defined using the `Transformation` trait, which facilitates the
conversion of input data of type `I` to output data of type `O`.

```rust
/// Trait defining the transformation logic between input and output data types.
pub trait Transformation<I, O>: Clone {
    /// Transforms input data of type I to output data of type O.
    fn apply(&mut self, data: I) -> impl Future;
}
```

## TODO List

Below is a list of tasks that need to be addressed in the StreamProcessor API project:

- [ ] Add possibility to write to multiple sinks at once.
- [ ] Add support for 1:n Transformations.
- [ ] Implement additional sources (object_store, odbc).
- [ ] Implement additional sinks (i.e. odbc, s3)
- [ ] Set up github actions.
- [ ] Provide Docker Example.

## Development

Run the init script before running any tests (this starts the kafka cluster, schema registry and azurite with the
required buckets)

    source ./scripts/init_env.sh

To run tests with log output

    RUST_LOG=INFO TEST_LOG=true cargo test

### Details about the setup of Azurite as local Azure emulator

run azurite

    docker run --name azurite -d -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite

create bucket

    docker run --name azure_cli --net=host mcr.microsoft.com/azure-cli az storage container create -n test-bucket --connection-string 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;'

local variables:

    export AZURE_STORAGE_USE_EMULATOR=true
    export AZURE_CONTAINER_NAME=test-bucket
    export AZURE_STORAGE_ACCOUNT_NAME=devstoreaccount1
    export AZURE_STORAGE_ACCESS_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==

even more details:

- https://github.com/apache/arrow-rs/blob/e8b424ab2980be4ed9188b9cb1878b16c4e8ac0e/object_store/CONTRIBUTING.md
- https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage