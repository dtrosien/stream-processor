use crate::type_definitions::MsgType;
use arrow::array::RecordBatch;
use std::any::Any;
use std::sync::Arc;

pub trait BatchContainer {
    fn get_sink(self: Arc<Self>) -> Option<String>;
    fn get_batch_type(self: Arc<Self>) -> Arc<MsgType>;
    fn get_batch(self: Arc<Self>) -> Arc<Batch>;
    fn get_batch_name(self: Arc<Self>) -> Option<String>;
}

// todo maybe include batch infos for commit
// todo noch ueber legen wo heterogene batches auftreten koennen .. evtl datatypes anpassen damit klar ist was homogen ist. zb das datatype Optional ist und nur wenn gestezt, ist es homogen oder direct im BATCH enum als uebergeorneten typ
// todo I guess most of the types can just be included in the Batch Enum?! Whcih would then make the GenercBatch clearer since MsgType is not needed. Think about how to group best (also keep in mind homogenious and hetro batches)

pub struct GenericBatchContainer {
    batch: Arc<Batch>,
    batch_name: Option<String>,
    batch_type: Arc<MsgType>,
}

pub enum Batch {
    AnyBatch(Vec<Arc<dyn Any>>),
    RecordBatch(RecordBatch),
}

impl GenericBatchContainer {
    pub fn new(batch: Arc<Batch>, batch_name: Option<String>, msg_type: MsgType) -> Arc<Self> {
        Arc::new(GenericBatchContainer {
            batch,
            batch_name,
            batch_type: Arc::new(msg_type),
        })
    }
}

impl BatchContainer for GenericBatchContainer {
    fn get_sink(self: Arc<Self>) -> Option<String> {
        None
    }

    fn get_batch_type(self: Arc<Self>) -> Arc<MsgType> {
        self.batch_type.clone()
    }

    fn get_batch(self: Arc<Self>) -> Arc<Batch> {
        self.batch.clone()
    }

    fn get_batch_name(self: Arc<Self>) -> Option<String> {
        self.batch_name.clone()
    }
}
