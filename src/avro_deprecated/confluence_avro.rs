use anyhow::Error;
use apache_avro::{to_avro_datum, to_value, AvroSchema, Schema};
use reqwest::Response;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, error};

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
struct Test {
    a: i64,
    b: String,
}

/// register a new schema at the schema registry
pub async fn post_schema(
    subject: &str,
    schema: &str,
    schema_registry_url: &str,
) -> Result<u32, Error> {
    let client = reqwest::Client::new();

    let url = format!("{}/subjects/{}/versions", schema_registry_url, subject);

    // schema registry requires a json starting with a "schema" node
    let schema_registration_payload = json!({
        "schema": schema
    })
    .to_string();

    debug!("try to post schema");
    let res = client
        .post(url)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .body(schema_registration_payload)
        .send()
        .await?;

    if res.status().is_success() {
        let response = res.text().await?;
        let parsed: serde_json::Value = serde_json::from_str(&response)?;
        let schema_id = parsed["id"]
            .as_u64()
            .map(|id| id as u32)
            .expect("Id must be in registry response");
        Ok(schema_id)
    } else {
        Err(Error::from(res.error_for_status().unwrap_err()))
    }
}

/// reads schema from registry via the unique schema id
pub async fn get_schema(id: u32, registry_url: &str) -> anyhow::Result<String> {
    let client = reqwest::Client::new();
    let url = format!("{}/schemas/ids/{}", registry_url, id);

    let res: Response = client.get(url).send().await?;

    // Ensure the request was successful and convert the response body to a String
    if res.status().is_success() {
        let response = res.text().await?;
        debug!("schema received {}", &response);

        // Parse the JSON response from the Schema Registry
        let parsed: serde_json::Value = serde_json::from_str(&response)?;

        // Extract the schema string. Ensure you handle potential errors here, such as missing or malformed data.
        let schema_str = parsed["schema"]
            .as_str()
            .ok_or(anyhow::Error::msg("Schema not found"))?;

        Ok(schema_str.to_owned())
    } else {
        Err(Error::from(res.error_for_status().unwrap_err()))
    }
}

/// gets schema id from schema registry for a subject of a specific version or latest if not provided
#[allow(dead_code)]
async fn get_schema_id(
    subject: &str,
    version: Option<u32>,
    schema_registry_url: &str,
) -> Result<Option<u32>, Error> {
    let client = reqwest::Client::new();

    let version = version
        .map(|v| v.to_string())
        .unwrap_or("latest".to_string());

    let url = format!(
        "{}/subjects/{}/versions/{}",
        schema_registry_url, subject, version
    );

    let res: Response = client.get(url).send().await?;
    if res.status().is_success() {
        let response = res.text().await?;
        let parsed: serde_json::Value = serde_json::from_str(&response)?;
        let schema_id = parsed["id"].as_u64().map(|id| id as u32);
        Ok(schema_id)
    } else {
        Err(Error::from(res.error_for_status().unwrap_err()))
    }
}

/// removes the 5 bytes from the beginning of the avro and returns the plain binary data
/// together with the u32 schema id from the confluent header
#[tracing::instrument(level = "trace", skip_all)]
pub fn unwrap_from_confluence_header(avro_data: &[u8]) -> Result<(u32, Vec<u8>), Error> {
    // slice of avro data
    let avro_data_slice = &avro_data[5..];
    //slice of confluent bytes
    let confluent_header = &avro_data[..5];

    // Check if magic byte matches
    if confluent_header[0] != 0u8 {
        let msg = format!(
            "Magic byte does not match first bit must be 0 instead of: {}",
            confluent_header[0]
        );
        error!(msg);
        return Err(Error::msg(msg));
    }

    let schema_id = u32::from_be_bytes(confluent_header[1..5].try_into()?);

    Ok((schema_id, avro_data_slice.to_vec()))
}

/// adds confluent header with magic byte and four mor bytes for an u32 schema id
#[tracing::instrument(level = "trace", skip_all)]
fn wrap_in_confluence_header(avro_data: Vec<u8>, schema_id: u32) -> Vec<u8> {
    // Magic byte
    let magic_byte = 0u8;

    // Convert schema ID to 4 bytes in big endian format.
    let schema_id_bytes = schema_id.to_be_bytes();

    // Create a new Vec<u8> and append the magic byte, schema ID, and Avro data.
    let mut message = Vec::<u8>::new();
    message.push(magic_byte); // Append the magic byte.
    message.extend_from_slice(&schema_id_bytes); // Append schema ID bytes.
    message.extend(avro_data); // Append Avro binary data.

    // `message` now contains the serialized data with the magic byte and schema ID.
    debug!("{:?}", message);
    message
}

/// serialize without avro header and add a confluent header with schema id
#[tracing::instrument(level = "trace", skip_all)]
pub fn serialize_to_confluence_avro(
    data: impl Serialize,
    schema_id: u32,
    schema: &Schema,
) -> Vec<u8> {
    let avro_binary = to_avro_datum(schema, to_value(data).unwrap()).unwrap();
    wrap_in_confluence_header(avro_binary, schema_id)
}

#[cfg(test)]
mod test {

    use crate::avro::confluence_avro::{
        get_schema, get_schema_id, post_schema, serialize_to_confluence_avro,
        unwrap_from_confluence_header, Test,
    };
    use anyhow::Error;
    use apache_avro::types::Value;
    use apache_avro::{from_avro_datum, from_value, AvroSchema, Schema};
    use reqwest::Response;
    use serde::de::DeserializeOwned;
    use tracing::{debug, info};

    #[tokio::test]
    async fn write_and_read_from_schema_registry() {
        let schema = r#"{"name":"Test","type":"record","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}"#;

        println!("{}", &Test::get_schema().canonical_form());
        post_schema("manual_schema-value", schema, "http://localhost:8081")
            .await
            .unwrap();

        let schema_id = get_schema_id("manual_schema-value", None, "http://localhost:8081")
            .await
            .unwrap()
            .unwrap();

        get_schema(schema_id, "http://localhost:8081")
            .await
            .unwrap();
        delete_schema("manual_schema-value").await.unwrap();
    }

    #[tokio::test]
    async fn serialize_with_confluent_header_and_deserialize_with_schema_registry() {
        // register schema
        let id_from_post = post_schema(
            "test_schema-value",
            &Test::get_schema().canonical_form(),
            "http://localhost:8081",
        )
        .await
        .unwrap();

        let test = Test {
            a: 0,
            b: "test data".to_string(),
        };

        let schema_id = get_schema_id("test_schema-value", None, "http://localhost:8081")
            .await
            .unwrap()
            .unwrap();

        let avro_data = serialize_to_confluence_avro(&test, schema_id, &Test::get_schema());

        let again_test = deserialize_avro_binary_to_actual_type::<Test>(&avro_data).await;

        assert_eq!(test.b, again_test.b);
        assert_eq!(id_from_post, schema_id);
        // delete_schema().await.unwrap();
    }

    async fn deserialize_to_generic_avro_value(avro_data: &[u8]) -> Value {
        let (id, data) = unwrap_from_confluence_header(avro_data).unwrap();

        debug!("search for schema wit id: {}", id);

        let raw_schema = get_schema(id, "http://localhost:8081").await.unwrap();

        let schema = Schema::parse_str(&raw_schema).unwrap();
        let value = from_avro_datum(&schema, &mut &data[..], None).unwrap();
        value
    }

    #[tracing::instrument(skip_all)]
    pub async fn deserialize_avro_binary_to_actual_type<T: AvroSchema + DeserializeOwned>(
        avro_data: &[u8],
    ) -> T {
        let value = deserialize_to_generic_avro_value(avro_data).await;
        from_value::<T>(&value).expect("couldn't convert from value")
    }

    /// deletes schema from schema registry (all versions)
    async fn delete_schema(subject: &str) -> Result<(), Error> {
        let client = reqwest::Client::new();
        let url = format!("http://localhost:8081/subjects/{}", subject);

        let res: Response = client.delete(url).send().await?;
        if res.status().is_success() {
            info!("schema deleted");
            Ok(())
        } else {
            Err(Error::from(res.error_for_status().unwrap_err()))
        }
    }
}
