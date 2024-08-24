use crate::action_plan::scan::Scan;
use crate::container::BatchContainer;
use crate::data_source::dummy_source::DummySource;
use crate::data_source::kafka_consumer::KafkaConsumer;
use crate::data_source::DataSource;
use crate::stream::{Stream, StreamImpl};
use apache_avro::AvroSchema;
use fake::{Dummy, Faker};
use log::info;
use rdkafka::ClientConfig;
use serde::Serialize;
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

    pub fn dummy<T: Serialize + Dummy<Faker> + AvroSchema + 'static>(
        &self,
        batch_size: u64,
    ) -> Arc<StreamImpl> {
        let ds: Arc<dyn DataSource> = DummySource::<T, T, T>::new(batch_size);
        Arc::new(StreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }

    pub fn dummy_2x<T1, T2>(&self, batch_size: u64) -> Arc<StreamImpl>
    where
        T1: Serialize + Dummy<Faker> + AvroSchema + 'static,
        T2: Serialize + Dummy<Faker> + AvroSchema + 'static,
    {
        let ds: Arc<dyn DataSource> = DummySource::<T1, T2, T2>::new(batch_size);
        Arc::new(StreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }

    pub fn dummy_3x<T1, T2, T3>(&self, batch_size: u64) -> Arc<StreamImpl>
    where
        T1: Serialize + Dummy<Faker> + AvroSchema + 'static,
        T2: Serialize + Dummy<Faker> + AvroSchema + 'static,
        T3: Serialize + Dummy<Faker> + AvroSchema + 'static,
    {
        let ds: Arc<dyn DataSource> = DummySource::<T1, T2, T2>::new(batch_size);
        Arc::new(StreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }

    /// Execute the logical plan represented by a DataFrame
    pub fn execute_once(&self, stream: Arc<dyn Stream>, optimize: bool) {
        let plan = if optimize {
            todo!()
        } else {
            stream.action_plan()
        };
        let errors = plan.execute().collect::<Vec<_>>();
        // todo collect and log infos of error batches
        info!("Num ErrorBatches: {}", errors.len())
    }
}
