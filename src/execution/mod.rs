use crate::action_plan::scan::Scan;
use crate::data_source::kafka_consumer::KafkaConsumer;
use crate::data_source::DataSource;
use crate::stream::{Stream, StreamImpl};
use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ExecutionContext {
    pub settings: HashMap<String, String>,
    batch_size: usize,
}

impl ExecutionContext {
    pub fn new(settings: HashMap<String, String>) -> Self {
        let batch_size = settings
            .get("csv.batch_size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);
        ExecutionContext {
            settings,
            batch_size,
        }
    }

    pub fn kafka(&self, topic: String, client_config: ClientConfig) -> Arc<StreamImpl> {
        let ds: Arc<dyn DataSource> = KafkaConsumer::new(client_config);
        Arc::new(StreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }
}
