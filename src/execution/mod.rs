use crate::action_plan::scan::Scan;
use crate::container::BatchContainer;
use crate::data_source::dummy_source::{DummySource, TestDummy};
use crate::data_source::kafka_consumer::KafkaConsumer;
use crate::data_source::DataSource;
use crate::data_stream::{DataStream, DataStreamImpl};
use crate::optimizer::Optimizer;
use apache_avro::AvroSchema;
use fake::{Dummy, Faker};
use log::info;
use rayon::prelude::*;
use rdkafka::ClientConfig;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct ExecutionContext {
    pub settings: HashMap<String, String>,
    batch_size: usize,
    streams: HashMap<Partition, Arc<dyn DataStream>>,
}

struct Partition(String);

impl ExecutionContext {
    pub fn new(settings: HashMap<String, String>) -> Self {
        let batch_size = settings
            .get("csv.batch_size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024);
        ExecutionContext {
            settings,
            batch_size,
            streams: HashMap::new(),
        }
    }

    pub fn kafka(&self, topic: String, client_config: ClientConfig) -> Arc<DataStreamImpl> {
        let ds: Arc<dyn DataSource> = KafkaConsumer::new(client_config);
        Arc::new(DataStreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }

    pub fn dummy<T: TestDummy>(&self, batch_size: u64, s_id: u32) -> Arc<DataStreamImpl> {
        let ds: Arc<dyn DataSource> = DummySource::<T, T, T>::new(batch_size, s_id, s_id, s_id);
        Arc::new(DataStreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }

    pub fn dummy_2x<T1, T2>(&self, batch_size: u64, s_id1: u32, s_id2: u32) -> Arc<DataStreamImpl>
    where
        T1: TestDummy,
        T2: TestDummy,
    {
        let ds: Arc<dyn DataSource> =
            DummySource::<T1, T2, T2>::new(batch_size, s_id1, s_id2, s_id2);
        Arc::new(DataStreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }

    pub fn dummy_3x<T1, T2, T3>(
        &self,
        batch_size: u64,
        s_id1: u32,
        s_id2: u32,
        s_id3: u32,
    ) -> Arc<DataStreamImpl>
    where
        T1: TestDummy,
        T2: TestDummy,
        T3: TestDummy,
    {
        let ds: Arc<dyn DataSource> =
            DummySource::<T1, T2, T2>::new(batch_size, s_id1, s_id2, s_id3);
        Arc::new(DataStreamImpl {
            plan: Some(Scan::new(ds)),
        })
    }

    /// Execute the logical plan represented by a DataStream
    pub fn execute_once(&self, stream: Arc<dyn DataStream>, optimize: bool) {
        // get
        let optimize = true;
        let plan = if optimize {
            let plan = stream.action_plan();
            let optimized_plan = Optimizer::optimize(plan);
            Optimizer::split_by_partitions(optimized_plan)
        } else {
            vec![stream.action_plan()]
        };

        let errors = plan
            .par_iter()
            .flat_map(|plan| plan.execute().collect::<Vec<_>>().into_iter().par_bridge())
            .collect::<Vec<_>>();

        info!("Num ErrorBatches: {}", errors.len())

        // todo collect and log infos of error batches

        // todo maybe return error and stats collection here ... better for testing
    }

    // todo add continuous execution etc
}
