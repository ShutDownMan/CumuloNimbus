use anyhow::Result;
use sqlx::{postgres::PgPool, Acquire};
use std::sync::Arc;
use tracing::{info, debug, error};
use sqlx::Row;
use sqlx::types::chrono::DateTime;

extern crate intercom;

use intercom::schemas;

#[derive(sqlx::Type)]
#[sqlx(type_name = "DataSeriesType")] // This should match the name of the enum type in PostgreSQL
#[sqlx(rename_all = "lowercase")] // This ensures that the Rust enum variants are converted to lowercase strings
enum DataSeriesType {
    Numeric,
    Text,
    Boolean,
    Arbitrary,
    Jsonb,
}

pub struct DataKeeper {
    db_pool: Arc<PgPool>,
    service_bus: Arc<intercom::ServiceBus>,
    tokio_handle: tokio::runtime::Handle,
}

impl DataKeeper {
    pub fn new(
        db_pool: Arc<PgPool>,
        service_bus: Arc<intercom::ServiceBus>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        Ok(DataKeeper {
            db_pool,
            service_bus,
            tokio_handle,
        })
    }

    pub async fn handle_messages(&self) -> Result<()> {
        let db_pool_clone = self.db_pool.clone();
        let service_bus_clone = self.service_bus.clone();
        let tokio_handle = self.tokio_handle.clone();

        self.service_bus.subscribe("persistor.input", intercom::MessageType::PersistDataSeries, Box::new(move |reader, metadata| {
            debug!("Handling persist data series message");
            let db_pool = db_pool_clone.clone();
            // let service_bus = service_bus_clone.clone();

            tokio_handle.block_on(async move {
                Self::handle_persist_data_series(db_pool, reader, metadata).await
            })
        }))?;

        self.service_bus.subscribe("persistor.input", intercom::MessageType::FetchDataSeries, Box::new(move |reader, metadata| {
            debug!("Handling fetch data series message");

            Self::handle_fetch_data_series(reader, metadata)
        }))?;

        self.service_bus.listen_forever().await?;

        Ok(())
    }

    async fn handle_persist_data_series(
        db_pool: Arc<PgPool>,
        reader: intercom::CapnpReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<schemas::persistor_capnp::persist_data_series::Reader>()?;
        let data_series_id = root.get_id()?;
        let data_series_type = root.get_type()?;
        let data_series_values = root.get_values()?;

        info!("Persisting data series with id: {:?}", data_series_id);
        debug!("Type: {:?}", data_series_type);

        persist_dataseries_from_message(db_pool, reader, metadata).await
    }

    fn handle_fetch_data_series(
        reader: intercom::CapnpReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<schemas::persistor_capnp::fetch_data_series::Reader>()?;
        debug!("Id: {:?}", root.get_id());

        Ok(())
    }
}

pub async fn persist_dataseries_from_message(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpReader,
    metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<schemas::persistor_capnp::persist_data_series::Reader>()?;
    let data_series_id = root.get_id()?;
    let data_series_type = root.get_type()?;
    let data_series_values = root.get_values()?;

    let mut executor = db_pool.acquire().await?;
    let mut transaction = executor.begin().await?;

    // CREATE TYPE DataSeriesType AS ENUM ('numeric', 'text', 'boolean', 'arbitrary', 'jsonb');
    let numeric_type = match data_series_type {
        schemas::persistor_capnp::persist_data_series::DataType::Numerical => DataSeriesType::Numeric,
        schemas::persistor_capnp::persist_data_series::DataType::Text => DataSeriesType::Text,
        schemas::persistor_capnp::persist_data_series::DataType::Boolean => DataSeriesType::Boolean,
        schemas::persistor_capnp::persist_data_series::DataType::Arbitrary => DataSeriesType::Arbitrary,
        // schemas::persistor_capnp::persist_data_series::DataType::Jsonb => DataSeriesType::Jsonb,
    };

    // TODO: do away with this query
    let res = sqlx::query(r#"
        INSERT INTO DataSeries (external_id, created_at, type)
        VALUES ($1, NOW(), $2)
        ON CONFLICT (external_id) DO UPDATE SET updated_at = NOW()
        RETURNING id;
    "#,)
    .bind(uuid::Uuid::parse_str(&data_series_id)?)
    .bind(numeric_type)
    .fetch_one(&mut *transaction)
    .await?;

    let data_series_id: i32 = res.try_get("id")?;
    info!("Data series {:} with {:} points to persist", data_series_id, data_series_values.len());

    // zip timestamps and data together
    let mut dataseries_values = vec![];
    for data_point in data_series_values.iter() {
        let timestamp = data_point.get_timestamp();
        let data = data_point.get_data();

        let timestamp = DateTime::from_timestamp(timestamp / 1_000_000_000, (timestamp % 1_000_000_000) as u32);

        dataseries_values.push((timestamp, data));
    }
    // filter out None values
    dataseries_values.retain(|(timestamp, _)| timestamp.is_some());

    // Type => Vec<(DateTime, Data)>
    let mut datapoints_dict = std::collections::HashMap::new();
    for (timestamp, data) in dataseries_values {
        let timestamp = timestamp.unwrap();
        match data.which()? {
            schemas::persistor_capnp::persist_data_series::data_point::data::Numerical(data) => {
                datapoints_dict.entry("numeric").or_insert_with(Vec::new).push((timestamp, data));
            },
            _ => {
                todo!("Implement other data types")
            }
        }
    }

    for (data_type, datapoints) in datapoints_dict {
        let dataseries_ids = vec![data_series_id; datapoints.len()];
        let timestamps = datapoints.iter().map(|(timestamp, _)| *timestamp).collect::<Vec<_>>();
        let values = datapoints.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let query = match data_type {
            "numeric" => {
                sqlx::query(r#"
                    INSERT INTO DataPointNumeric (dataseries_id, timestamp, value)
                    SELECT * FROM UNNEST($1::int[], $2::timestamptz[], $3::float8[])
                    ON CONFLICT (dataseries_id, timestamp) DO NOTHING;
                "#)
                .bind(dataseries_ids)
                .bind(timestamps)
                .bind(values)
                .execute(&mut *transaction)
            },
            _ => {
                todo!("Implement other data types")
            }
        };

        query.await?;
    }

    info!("Persisted data series with id: {:?}", data_series_id);

    transaction.commit().await?;

    debug!("Committed transaction");

    Ok(())
}
