use arrow::array::RecordBatch;

pub trait ToArrow {
    fn schema() -> arrow::datatypes::Schema;
    fn create_record_batch(data: &[Self]) -> RecordBatch
    where
        Self: Sized;
}
