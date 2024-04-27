use crate::object_store_sink::general::ToArrow;
use crate::sink::Sink;
use chrono::Utc;
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

// todo migrate to Object_store v0.10 (broke interface for multipart upload and does not integrate with AsyncWriter anymore)

#[derive(Clone)]
pub struct AzureSink {
    object_store: Arc<dyn ObjectStore>,
    pub batch_size: u32,
    pub target_dir: String,
}

impl AzureSink {
    pub fn new(batch_size: u32, target_dir: &str) -> Self {
        let object_store = get_azure_object_store();

        AzureSink {
            object_store,
            batch_size,
            target_dir: target_dir.to_string(),
        }
    }
}

impl<O> Sink<O> for AzureSink
where
    O: ToArrow,
{
    #[tracing::instrument(skip_all)]
    async fn write_batch(&self, data: &[O]) -> anyhow::Result<()> {
        let object_store_path = create_path(&self.target_dir);
        write_parquet(&self.object_store, object_store_path, data).await?;
        Ok(())
    }

    fn get_batch_size(&self) -> u32 {
        self.batch_size
    }
}

fn create_path(root_dir: &str) -> Path {
    let ts = Utc::now().to_string();
    let id = Uuid::new_v4();
    let path_name = format!("{}/batch_{}_{}", root_dir, ts, id);
    Path::from(path_name)
}

/// see readme to set env variables correctly
fn get_azure_object_store() -> Arc<dyn ObjectStore> {
    let store: MicrosoftAzure = MicrosoftAzureBuilder::from_env().build().unwrap();
    Arc::new(store)
}

async fn write_parquet<O>(
    object_store: &Arc<dyn ObjectStore>,
    path: Path,
    data: &[O],
) -> anyhow::Result<()>
where
    O: ToArrow,
{
    let batch = O::create_record_batch(data);
    debug!("Rows in record batch: {}", batch.num_rows());
    let (_id, object_store_writer) = object_store.put_multipart(&path).await?;

    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = AsyncArrowWriter::try_new(object_store_writer, batch.schema(), Some(props))?;

    writer.write(&batch).await?;

    // writer must be closed to write footer
    writer.close().await?;
    Ok(())
}

#[cfg(test)]
mod test {

    use crate::domain::test::Person;
    use crate::object_store_sink::azure_sink::{get_azure_object_store, write_parquet};
    use bytes::Bytes;
    use object_store::path::Path;
    use object_store::ObjectStore;

    #[tokio::test]
    async fn test_azure_object_store() {
        let object_store = get_azure_object_store();
        let path = Path::from("test");
        let bytes = Bytes::from_static(b"hello");
        object_store.put(&path, bytes).await.unwrap();
    }

    #[tokio::test]
    async fn test_azure_parquet_upload() {
        let object_store = get_azure_object_store();
        let object_store_path = Path::from("azure_parquet_test/sub_dir");
        let person = Person {
            id: 0,
            name: "Muster".to_string(),
            address: None,
        };

        write_parquet(&object_store, object_store_path.clone(), &vec![person])
            .await
            .unwrap();

        let size_on_object_store = object_store
            .get(&object_store_path)
            .await
            .unwrap()
            .meta
            .size;

        assert_eq!(800, size_on_object_store);
    }
}
