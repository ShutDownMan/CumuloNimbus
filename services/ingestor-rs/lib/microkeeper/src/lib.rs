use anyhow::{bail, Result};
use tracing::{debug, error, info, warn};
use sqlx::{Row, sqlite::SqlitePool, Acquire};
use std::sync::Arc;
use uuid::Uuid;

extern crate intercom;
use crate::intercom::HasTypeId;

type DataPointBuilder<'a, T> = intercom::schemas::dataseries_capnp::data_point::Builder<'a, T>;
type PersistDataSeriesBuilder<'a, T> = intercom::schemas::persistor_capnp::persist_data_series::Builder<'a, T>;
type PersistNumericDataSeriesBuilder<'a> = PersistDataSeriesBuilder<'a, intercom::schemas::dataseries_capnp::numeric_data_point::Owned>;

#[derive(Debug, thiserror::Error)]
pub struct DatabaseLockError;

impl std::fmt::Display for DatabaseLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Database lock error")
    }
}

#[derive(Clone, Debug)]
pub struct DataSeries<T>
where T: DataPointType {
    pub metadata: DataSeriesMetadata,
    pub values: Vec<DataPoint<T>>,
}

#[derive(Clone, Debug)]
pub struct DataSeriesMetadata {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub data_type: intercom::schemas::dataseries_capnp::DataType,
}

#[derive(Clone, Debug)]
pub struct DataPoint<T>
where T: DataPointType {
    pub id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub value: T,
}

pub trait DataPointType: Sized + Clone + std::fmt::Debug {
    fn encode(&self) -> String;
    fn decode<'r, DB: sqlx::Database>(value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self>
    where &'r str: sqlx::Decode<'r, DB>;

    fn try_get_numerical(&self) -> Result<f64>;
    fn try_get_text(&self) -> Result<String>;
}

#[derive(Clone, Debug)]
pub struct NumericDataPoint {
    pub numeric: f64,
}

impl DataPointType for NumericDataPoint {
    fn encode(&self) -> String {
        self.numeric.to_string()
    }

    fn decode<'r, DB: sqlx::Database>(value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self>
    where &'r str: sqlx::Decode<'r, DB> {
        let value = <&str as sqlx::Decode<DB>>::decode(value)
            .map_err(|e| anyhow::anyhow!("failed to decode numeric value: {:?}", e))?;

        Ok(NumericDataPoint {
            numeric: value.parse()?,
        })
    }

    fn try_get_numerical(&self) -> Result<f64> {
        Ok(self.numeric)
    }

    fn try_get_text(&self) -> Result<String> {
        bail!("numeric datapoint should not be converted to text value");
    }
}

#[derive(Clone, Debug)]
pub struct TextDataPoint {
    pub text: String,
}

impl DataPointType for TextDataPoint {
    fn encode(&self) -> String {
        format!("'{}'", self.text)
    }

    fn decode<'r, DB: sqlx::Database>(value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef) -> Result<Self>
    where &'r str: sqlx::Decode<'r, DB> {
        let value = <&str as sqlx::Decode<DB>>::decode(value)
            .map_err(|e| anyhow::anyhow!("failed to decode text value: {:?}", e))?;

        Ok(TextDataPoint {
            text: value.to_string(),
        })
    }

    fn try_get_numerical(&self) -> Result<f64> {
        bail!("text datapoint should not be converted to numerical value");
    }

    fn try_get_text(&self) -> Result<String> {
        Ok(self.text.clone())
    }
}

pub trait DataPointSetter {
    fn set(&mut self, datapoint: &DataPoint<impl DataPointType>) -> Result<()>;
}

impl DataPointSetter for DataPointBuilder<'_, intercom::schemas::dataseries_capnp::numeric_data_point::Owned> {
    fn set(&mut self, datapoint: &DataPoint<impl DataPointType>) -> Result<()> {
        let timestamp = datapoint.timestamp.timestamp_nanos_opt();
        let value = datapoint.value.try_get_numerical()?;

        if timestamp.is_none() {
            bail!("invalid timestamp");
        }

        self.reborrow().set_timestamp(timestamp.unwrap());
        self.reborrow().init_value().set_numeric(value);

        Ok(())
    }
}

pub async fn save_dataseries(sqlite_pool: Arc<SqlitePool>, dataseries: &DataSeries<impl DataPointType>, expiration: chrono::Duration) -> Result<Vec<i64>> {
    // this time we will save as much as possible in a single statement
    info!("saving dataseries of id {:?}", dataseries.metadata.id);

    if dataseries.values.is_empty() {
        info!("no datapoints to save");
        return Ok(Vec::new());
    }

    // save the dataseries to the database
    info!("fetching database connection to save dataseries {:?}", dataseries.metadata.id);
    // block until we get a connection
    let mut executor = sqlite_pool.acquire().await?;
    let mut transaction = executor.begin().await?;
    info!("saving dataseries to database");
    let res = sqlx::query(r#"
        INSERT INTO DataSeries (external_id, created_at)
        VALUES ($1, $2)
        ON CONFLICT (external_id) DO UPDATE SET updated_at = $2
        RETURNING id;
    "#,)
    .bind(dataseries.metadata.id.to_string())
    .bind(chrono::Utc::now().timestamp())
    .fetch_one(&mut *transaction)
    .await?;
    let db_dataseries_id: i32 = res.try_get("id")?;
    debug!("dataseries_id: {:?}", db_dataseries_id);

    info!("saving datapoints of dataseries {:?} to database", dataseries.metadata.id);
    let mut datapoint_ids = Vec::new();

    for data_point in dataseries.values.iter() {
        debug!("saving datapoint {:?}", data_point);
        // encode the datapoint value
        let mut new_datapoint_ids = sqlx::query(r#"
            INSERT INTO DataPoint (dataseries_id, timestamp, value, created_at, expiration)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (dataseries_id, timestamp) DO UPDATE SET value = EXCLUDED.value
            RETURNING id;
        "#,)
        .bind(db_dataseries_id)
        .bind(data_point.timestamp.timestamp_nanos_opt().unwrap_or(0))
        .bind(data_point.value.encode())
        .bind(chrono::Utc::now().timestamp())
        .bind(chrono::Utc::now().timestamp() + expiration.num_seconds())
        .fetch_all(&mut *transaction)
        .await?;

        debug!("new_datapoint_ids: {:?}", new_datapoint_ids.len());
        while let Some(row) = new_datapoint_ids.pop() {
            let datapoint_id = row.try_get("id");
            let datapoint_id: i64 = match datapoint_id {
                Ok(id) => id,
                Err(e) => {
                    error!("failed to save datapoint: {:?}", e);
                    continue;
                }
            };
            datapoint_ids.push(datapoint_id);
        }
    }

    info!("committing transaction");
    transaction.commit().await?;

    Ok(datapoint_ids)
}

pub async fn get_pending_dataseries_ids(sqlite_pool: Arc<SqlitePool>) -> Result<Vec<uuid::Uuid>> {
    // get the pending dataseries from the database
    info!("fetching database connection to get pending dataseries");
    // block until we get a connection
    let executor = sqlite_pool.try_acquire();
    if let None = executor {
        warn!("could not acquire database connection");
        return Err(DatabaseLockError.into());
    }
    let mut executor = executor.unwrap();
    let mut transaction = executor.begin().await?;

    // get the pending dataseries
    let res = sqlx::query(r#"
        SELECT ds.external_id
        FROM DataSeries ds
        INNER JOIN DataPoint dp ON ds.id = dp.dataseries_id
        WHERE dp.sent_at IS NULL
        GROUP BY ds.external_id
    "#)
    .fetch_all(&mut *transaction)
    .await?;

    debug!("fetched {} dataseries", res.len());

    let dataseries_ids: Vec<String> = res.iter()
        .map(|row| row.get("external_id"))
        .collect();

    // commit the transaction
    transaction.commit().await?;

    Ok(dataseries_ids.iter().map(|id| uuid::Uuid::parse_str(id).unwrap()).collect())
}

pub async fn send_pending_numeric_datapoints(sqlite_pool: Arc<SqlitePool>, service_bus: Arc<intercom::ServiceBus>, dataseries_id: &uuid::Uuid, realtime: bool) -> Result<()> {
    loop {
        // get the pending datapoints for the dataseries
        let pending_datapoints = get_pending_datapoints::<NumericDataPoint>(sqlite_pool.clone(), dataseries_id).await?;
        // if there are no pending datapoints, we break the loop
        if pending_datapoints.values.is_empty() {
            info!("no pending datapoints found for dataseries {:?}", dataseries_id);
            break;
        }

        // send the datapoints to the service bus
        publish_dataseries(service_bus.clone(), &pending_datapoints,
            "persistor.input", intercom::MessagePriority::Omega, !realtime).await?;

        let datapoint_ids: Vec<i64> = pending_datapoints.values.iter()
            .map(|datapoint| datapoint.id)
            .collect();
        // mark the datapoints as sent in the database
        mark_datapoints_as_sent(sqlite_pool.clone(), &datapoint_ids).await?;
    }

    Ok(())
}

async fn get_pending_datapoints<T>(sqlite_pool: Arc<SqlitePool>, dataseries_id: &uuid::Uuid) -> Result<DataSeries<impl DataPointType>>
where T: DataPointType {
    // get the pending datapoints for the dataseries from the database
    info!("fetching database connection to get pending datapoints");
    tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
    let executor = sqlite_pool.try_acquire();
    if let None = executor {
        warn!("could not acquire database connection");
        return Err(DatabaseLockError.into());
    }
    let mut executor = executor.unwrap();
    let mut transaction = executor.begin().await?;

    // get the pending datapoints
    let mut rows = sqlx::query(r#"
        SELECT dp.*
        FROM DataPoint dp
        JOIN DataSeries ds ON dp.dataseries_id = ds.id
        WHERE ds.external_id = ? AND dp.sent_at IS NULL
        LIMIT ?
    "#)
    .bind(&dataseries_id.to_string())
    .bind(1000)
    .fetch_all(&mut *transaction)
    .await?;

    let count = rows.len();
    debug!("fetched {} datapoints", count);

    let datapoints = get_datapoints_from_rows::<T>(&mut rows).await?;

    transaction.commit().await?;

    Ok(DataSeries {
        metadata: DataSeriesMetadata {
            id: *dataseries_id,
            name: String::new(),
            description: String::new(),
            data_type: intercom::schemas::dataseries_capnp::DataType::Numeric,
        },
        values: datapoints,
    })
}

async fn get_datapoints_from_rows<'r, T>(rows: &mut Vec<sqlx::sqlite::SqliteRow>) -> Result<Vec<DataPoint<T>>>
where T: DataPointType {
    // TODO: discover T from datapoint type
    let mut datapoints = Vec::new();

    for row in rows.drain(..) {
        let id: i64 = row.try_get("id")?;
        let nano_timestamp: i64 = row.try_get("timestamp")?;
        let value = T::decode::<sqlx::Sqlite>(row.try_get_raw("value")?)?;

        let timestamp = chrono::DateTime::from_timestamp(nano_timestamp / 1_000_000_000, (nano_timestamp % 1_000_000_000) as u32);
        // if invalid timestamp, skip
        match timestamp {
            Some(timestamp) => {
                datapoints.push(DataPoint {
                    id: id,
                    timestamp: timestamp,
                    value: value
                });
            },
            None => {
                error!("invalid timestamp: {:?}", nano_timestamp);
                continue;
            }
        };
    }
    Ok(datapoints)
}

pub async fn publish_numeric_dataseries(service_bus: Arc<intercom::ServiceBus>, dataseries: &DataSeries<NumericDataPoint>, topic: &str,
    priority: intercom::MessagePriority, compress: bool) -> Result<()> {
    if dataseries.values.is_empty() {
        info!("no datapoints to publish for dataseries {:?}", dataseries.metadata.id);
        return Ok(());
    }
    info!("publishing dataseries to service bus");

    let dataseries_id = dataseries.metadata.id.to_string();

    info!("publishing {} datapoints for dataseries {:?}", dataseries.values.len(), dataseries_id);

    info!("transforming dataseries into bytearray with capnp");
    let buffer: Vec<u8> = {
        let count = dataseries.values.len();
        let mut message = intercom::CapnpBuilder::new_default();
        let capnp_persist_dataseries = message.init_root::<PersistNumericDataSeriesBuilder>();
        let mut capnp_dataseries = capnp_persist_dataseries.init_dataseries();

        {
            let mut capnp_dataseries_metadata = capnp_dataseries.reborrow().init_metadata();
            capnp_dataseries_metadata.set_id(&dataseries_id);
            capnp_dataseries_metadata.set_name(&dataseries.metadata.name);
            capnp_dataseries_metadata.set_description(&dataseries.metadata.description);
            capnp_dataseries_metadata.set_data_type(dataseries.metadata.data_type);
        }

        let mut capnp_dataseries_data = capnp_dataseries.reborrow().init_values(count as u32);

        for (i, datapoint) in dataseries.values.iter().enumerate() {
            let timestamp = datapoint.timestamp.timestamp_nanos_opt();
            let value = datapoint.value.numeric;

            if timestamp.is_none() {
                error!("invalid datapoint: {:?}", datapoint);
                continue;
            }

            let mut datapoint_builder = capnp_dataseries_data.reborrow().get(i as u32);
            datapoint_builder.set_timestamp(timestamp.unwrap());

            let mut value_builder = datapoint_builder.reborrow().init_value();
            value_builder.set_numeric(value);
        }

        let mut buffer = Vec::new();
        intercom::serialize_packed::write_message(&mut buffer, &message)?;

        buffer
    };

    info!("attempting to send dataseries to service bus");
    let type_id = PersistNumericDataSeriesBuilder::TYPE_ID;
    let publish_status = service_bus
        .intercom_publish("amq.direct", topic,
        type_id, &buffer, compress, priority.into())
        .await;
    if let Err(e) = publish_status {
        error!("failed to send dataseries to service bus: {:?}", e);
        return Err(e);
    }
    info!("dataseries {} sent to service bus", dataseries_id);

    Ok(())
}

pub async fn publish_dataseries<T>(service_bus: Arc<intercom::ServiceBus>, dataseries: &DataSeries<impl DataPointType>, topic: &str,
    priority: intercom::MessagePriority, compress: bool) -> Result<()>
where T: intercom::traits::Owned, for<'a> DataPointBuilder<'a, T>: DataPointSetter {
    if dataseries.values.is_empty() {
        info!("no datapoints to publish for dataseries {:?}", dataseries.metadata.id);
        return Ok(());
    }
    info!("publishing dataseries to service bus");

    let dataseries_id = dataseries.metadata.id.to_string();

    info!("publishing {} datapoints for dataseries {:?}", dataseries.values.len(), dataseries_id);

    info!("transforming dataseries into bytearray with capnp");
    let buffer: Vec<u8> = {
        let count = dataseries.values.len();
        let mut message = intercom::CapnpBuilder::new_default();
        let capnp_persist_dataseries = message.init_root::<PersistDataSeriesBuilder<T>>();
        let mut capnp_dataseries = capnp_persist_dataseries.init_dataseries();

        {
            let mut capnp_dataseries_metadata = capnp_dataseries.reborrow().init_metadata();
            capnp_dataseries_metadata.set_id(&dataseries_id);
            capnp_dataseries_metadata.set_name(&dataseries.metadata.name);
            capnp_dataseries_metadata.set_description(&dataseries.metadata.description);
            capnp_dataseries_metadata.set_data_type(dataseries.metadata.data_type);
        }

        let mut capnp_dataseries_data = capnp_dataseries.reborrow().init_values(count as u32);

        for (i, datapoint) in dataseries.values.iter().enumerate() {
            let mut datapoint_builder = capnp_dataseries_data.reborrow().get(i as u32);
            datapoint_builder.set(datapoint)?;
        }

        let mut buffer = Vec::new();
        intercom::serialize_packed::write_message(&mut buffer, &message)?;

        buffer
    };

    info!("attempting to send dataseries to service bus");
    let type_id = PersistDataSeriesBuilder::<T>::TYPE_ID;
    let publish_status = service_bus
        .intercom_publish("amq.direct", topic,
        type_id, &buffer, compress, priority.into())
        .await;
    if let Err(e) = publish_status {
        error!("failed to send dataseries to service bus: {:?}", e);
        return Err(e);
    }
    info!("dataseries {} sent to service bus", dataseries_id);

    Ok(())
}

pub async fn mark_datapoints_as_sent(sqlite_pool: Arc<SqlitePool>, datapoint_ids: &Vec<i64>) -> Result<()> {
    if datapoint_ids.is_empty() {
        info!("no datapoints to mark as sent");
        return Ok(());
    }

    info!("marking {} datapoints as sent", datapoint_ids.len());

    info!("fetching database connection to mark datapoints as sent");
    tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
    let mut executor = sqlite_pool.acquire().await?;
    let mut transaction = executor.begin().await?;

    let query = format!(r#"
        UPDATE DataPoint
        SET sent_at = {}
        WHERE id IN ({});
        "#,
        chrono::Utc::now().timestamp(),
        datapoint_ids.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ")
    );

    sqlx::query(&query)
        .execute(&mut *transaction)
        .await?;

    transaction.commit().await?;

    info!("{} datapoints marked as sent", datapoint_ids.len());

    Ok(())
}

pub async fn delete_expired_datapoints(sqlite_pool: Arc<SqlitePool>) -> Result<()> {
    // info!("deleting expired datapoints");

    info!("fetching database connection to delete expired datapoints");
    let mut executor = sqlite_pool.acquire().await?;
    let mut transaction = executor.begin().await?;

    let query = sqlx::query(r#"
        DELETE FROM DataPoint
        WHERE expiration < $1;
    "#)
    .bind(chrono::Utc::now().timestamp())
    .execute(&mut *transaction)
    .await?;

    info!("deleted {} expired datapoints", query.rows_affected());

    transaction.commit().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
