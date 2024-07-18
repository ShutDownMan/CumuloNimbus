use anyhow::Result;
use sqlx::{postgres::PgPool, Acquire};
use std::sync::Arc;
use tracing::{info, debug};
use sqlx::Row;
use sqlx::types::chrono::DateTime;

extern crate intercom;

use intercom::schemas;
use crate::intercom::HasTypeId;

type PersistNumericDataSeriesReader<'a> = intercom::schemas::persistor_capnp::persist_data_series::Reader::<'a, schemas::dataseries_capnp::numeric_data_series::Owned>;
type FetchDataSeriesReader<'a> = intercom::schemas::persistor_capnp::fetch_data_series::Reader<'a>;

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
        let _service_bus_clone = self.service_bus.clone();
        let tokio_handle = self.tokio_handle.clone();

        let persist_dataseries_type_id = PersistNumericDataSeriesReader::TYPE_ID;
        self.service_bus.subscribe("persistor.input", persist_dataseries_type_id, Box::new(move |reader, metadata| {
            debug!("Handling persist data series message");
            let db_pool = db_pool_clone.clone();
            let _service_bus = _service_bus_clone.clone();

            tokio_handle.block_on(async move {
                Self::handle_persist_data_series(db_pool, reader, metadata).await
            })
        }))?;

        let fetch_dataseries_type_id = FetchDataSeriesReader::TYPE_ID;
        self.service_bus.subscribe("persistor.input", fetch_dataseries_type_id, Box::new(move |reader, metadata| {
            debug!("Handling fetch data series message");

            Self::handle_fetch_data_series(reader, metadata)
        }))?;

        self.service_bus.listen_forever().await?;

        Ok(())
    }

    // struct PersistDataSeries(T) {
    //     dataseries @0 :T;
    //     options @1 :PersistDataSeriesOptions;
    // }

    async fn handle_persist_data_series(
        db_pool: Arc<PgPool>,
        reader: intercom::CapnpReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<PersistNumericDataSeriesReader>()?;
        let dataseries: schemas::dataseries_capnp::numeric_data_series::Reader = root.get_dataseries()?;
        let datataseries_metadata = dataseries.get_metadata()?;
        let dataseries_id = datataseries_metadata.get_id()?;

        info!("Persisting data series with id: {:?}", dataseries_id);

        persist_dataseries_from_message(db_pool, reader, metadata).await
    }

    fn handle_fetch_data_series(
        reader: intercom::CapnpReader,
        metadata: intercom::LapinAMQPProperties
    ) -> Result<()> {
        let root = reader.get_root::<schemas::persistor_capnp::fetch_data_series::Reader>()?;
        debug!("Id: {:?}", root.get_id());
        debug!("Metadata: {:?}", metadata);

        Ok(())
    }
}

pub async fn persist_dataseries_from_message(
    db_pool: Arc<PgPool>,
    reader: intercom::CapnpReader,
    _metadata: intercom::LapinAMQPProperties
) -> Result<()> {
    let root = reader.get_root::<PersistNumericDataSeriesReader>()?;
    let dataseries: schemas::dataseries_capnp::numeric_data_series::Reader = root.get_dataseries()?;
    let datataseries_metadata = dataseries.get_metadata()?;
    let dataseries_id = datataseries_metadata.get_id()?;
    let dataseries_data = dataseries.get_values()?;

    let mut executor = db_pool.acquire().await?;
    let mut transaction = executor.begin().await?;

    // TODO: do away with this query
    let res = sqlx::query(r#"
        INSERT INTO DataSeries (external_id, created_at)
        VALUES ($1, NOW())
        ON CONFLICT (external_id) DO UPDATE SET updated_at = NOW()
        RETURNING id;
    "#,)
    .bind(uuid::Uuid::parse_str(&dataseries_id)?)
    .fetch_one(&mut *transaction)
    .await?;

    let dataseries_id: i32 = res.try_get("id")?;
    info!("Data series {:} with {:} points to persist", dataseries_id, dataseries_data.len());

    // zip timestamps and data together
    let mut zipped_values = vec![];
    for data_point in dataseries_data.iter() {
        let timestamp = data_point.get_timestamp();
        let data = data_point.get_value();

        let timestamp = DateTime::from_timestamp(timestamp / 1_000_000_000, (timestamp % 1_000_000_000) as u32);

        zipped_values.push((timestamp, data));
    }
    // filter out None values
    zipped_values.retain(|(timestamp, _)| timestamp.is_some());

    // let dataseries_ids = vec![dataseries_id; zipped_values.len()];
    // let timestamps = zipped_values.iter().map(|(timestamp, _)| *timestamp).collect::<Vec<_>>();
    // let values = zipped_values.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    // let query = sqlx::query(r#"
    //     INSERT INTO DataPointNumeric (dataseries_id, timestamp, value)
    //     SELECT * FROM UNNEST($1::int[], $2::timestamptz[], $3::float8[])
    //     ON CONFLICT (dataseries_id, timestamp) DO NOTHING;
    // "#)
    // .bind(dataseries_ids)
    // .bind(timestamps)
    // .bind(values)
    // .execute(&mut *transaction);

    // query.await?;

    info!("Persisted data series with id: {:?}", dataseries_id);

    transaction.commit().await?;

    debug!("Committed transaction");

    Ok(())
}
