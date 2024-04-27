use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{ExportConfig, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{runtime, Resource};
use std::sync::OnceLock;
use tokio::task::JoinHandle;
use tracing::subscriber::set_global_default;
use tracing::Subscriber;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

/// Ensures that the `tracing` stack is only initialised once using `once_cell`
pub fn init_test_tracing() {
    static TRACING: OnceLock<()> = OnceLock::new();
    TRACING.get_or_init(|| {
        let default_filter_level = "info".to_string();
        let subscriber_name = "stream_processing_test".to_string();

        if std::env::var("TEST_LOG").is_ok_and(|x| x.to_lowercase().contains("true")) {
            let subscriber = get_open_telemetry_subscriber(
                subscriber_name,
                default_filter_level,
                std::io::stdout,
            );
            init_tracing_with_subscriber(subscriber);
        } else {
            let subscriber =
                get_open_telemetry_subscriber(subscriber_name, default_filter_level, std::io::sink);
            init_tracing_with_subscriber(subscriber);
        }
    });
}

/// Compose multiple layers into a `tracing`'s subscriber.
/// set level via env variable "RUST_LOG"
pub fn get_open_telemetry_subscriber<Sink>(
    name: String,
    env_filter: String,
    sink: Sink,
) -> impl Subscriber + Send + Sync
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    // init context propagation (required for kafka or http)
    global::set_text_map_propagator(TraceContextPropagator::new());

    // layer to output to i.e stout
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(sink)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::CLOSE);

    // layer to write to otlp endpoint
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            opentelemetry_sdk::trace::config()
                .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
                .with_resource(opentelemetry_sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new(
                        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                        name,
                    ),
                ])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("Couldn't create OTLP tracer");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));

    // Create a tracing layer with the configured tracer
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(telemetry_layer)
}

/// inits a meter provider
/// from https://github.com/open-telemetry/opentelemetry-rust/blob/main/opentelemetry-otlp/examples/basic-otlp/src/main.rs
pub fn init_metrics(meter_name: String) -> SdkMeterProvider {
    let provider = opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_export_config(ExportConfig::default()), // for advanced settings
        )
        .with_resource(Resource::new(vec![KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            meter_name,
        )]))
        .build()
        .unwrap();

    global::set_meter_provider(provider.clone());
    provider
}

/// Register a subscriber as global default to process span data.
///
/// It should only be called once!
pub fn init_tracing_with_subscriber(subscriber: impl Subscriber + Send + Sync) {
    // Redirect all `log`'s events to subscriber
    LogTracer::init().expect("Failed to set logger");
    // `set_global_default` can be used by applications to specify what subscriber should be used to process spans.
    set_global_default(subscriber).expect("Failed to set subscriber");
}
