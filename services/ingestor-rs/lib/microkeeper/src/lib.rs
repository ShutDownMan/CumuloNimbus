use anyhow::Result;
use tracing::{debug, error, info, warn};
use sqlx::{Row, sqlite::SqlitePool, Acquire};
use std::sync::Arc;
use uuid::Uuid;

extern crate intercom;

#[derive(Debug, thiserror::Error)]
pub struct DatabaseLockError;

impl std::fmt::Display for DatabaseLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Database lock error")
    }
}

#[derive(Clone, Debug)]
pub struct DataPointNumeric {
    pub id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub value: f64,
}

#[derive(Clone, Debug)]
pub struct DataSeriesNumeric {
    pub dataseries_id: Uuid,
    pub values: Vec<DataPointNumeric>,
}

pub enum DataSeriesValues {
    Numeric(Vec<DataPointNumeric>),
    // Categorical(Vec<DataPointCategorical>),
}

pub struct DataSeries {
    pub dataseries_id: Uuid,
    pub values: DataSeriesValues,
}

pub async fn save_dataseries(sqlite_pool: Arc<SqlitePool>, dataseries: &DataSeriesNumeric) -> Result<Vec<i64>> {
    // this time we will save as much as possible in a single statement
    info!("saving dataseries of id {:?}", dataseries.dataseries_id);

    if dataseries.values.is_empty() {
        info!("no datapoints to save");
        return Ok(Vec::new());
    }

    // save the dataseries to the database
    info!("fetching database connection");
    // block until we get a connection
    let mut executor = sqlite_pool.acquire().await?;
    let mut transaction = executor.begin().await?;
    info!("saving dataseries to database");
    let res = sqlx::query(r#"
        INSERT INTO DataSeries (external_id, created_at)
        VALUES (?, DATE('now'))
        ON CONFLICT (external_id) DO UPDATE SET updated_at = DATE('now')
        RETURNING id;
    "#,)
    .bind(dataseries.dataseries_id.to_string())
    .fetch_one(&mut *transaction)
    .await?;
    let dataseries_id: i32 = res.try_get("id")?;
    debug!("dataseries_id: {:?}", dataseries_id);

    info!("saving datapoints to database");
    let mut datapoint_ids = Vec::new();

    for data_point in dataseries.values.iter() {
        debug!("saving datapoint {:?}", data_point);
        let mut new_datapoint_ids = sqlx::query(r#"
            INSERT INTO DataPoint (dataseries_id, timestamp, value, created_at)
            VALUES (?, ?, ?, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))
            ON CONFLICT (dataseries_id, timestamp) DO UPDATE SET value = EXCLUDED.value
            RETURNING id;
        "#,)
        .bind(dataseries_id)
        .bind(data_point.timestamp.timestamp_nanos_opt().unwrap_or(0))
        .bind(data_point.value)
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

pub async fn get_pending_dataseries(sqlite_pool: Arc<SqlitePool>) -> Result<Vec<uuid::Uuid>> {
    // get the pending dataseries from the database
    info!("fetching database connection");
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

pub async fn send_pending_datapoints(sqlite_pool: Arc<SqlitePool>, service_bus: Arc<intercom::ServiceBus>, dataseries_id: &uuid::Uuid) -> Result<()> {
    loop {
        // get the pending datapoints for the dataseries
        tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
        let pending_datapoints = get_pending_datapoints(sqlite_pool.clone(), dataseries_id).await?;
        // if there are no pending datapoints, we break the loop
        if pending_datapoints.values.is_empty() {
            info!("no pending datapoints found for dataseries {:?}", dataseries_id);
            break;
        }

        // send the datapoints to the service bus
        publish_dataseries(service_bus.clone(), &pending_datapoints,
            "persistor.imput", intercom::MessagePriority::Omega).await?;

        let datapoint_ids: Vec<i64> = pending_datapoints.values.iter()
            .map(|datapoint| datapoint.id)
            .collect();
        // mark the datapoints as sent in the database
        tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
        mark_datapoints_as_sent(sqlite_pool.clone(), &datapoint_ids).await?;
    }

    Ok(())
}

async fn get_pending_datapoints(sqlite_pool: Arc<SqlitePool>, dataseries_id: &uuid::Uuid) -> Result<DataSeriesNumeric> {
    // get the pending datapoints for the dataseries from the database
    info!("fetching database connection");
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
    .bind(dataseries_id.to_string())
    .bind(1000)
    .fetch_all(&mut *transaction)
    .await?;

    let mut datapoint_ids: Vec<i64> = Vec::new();
    datapoint_ids.reserve(rows.len());
    let mut datapoints: Vec<DataPointNumeric> = Vec::new();
    datapoints.reserve(rows.len());

    info!("transforming rows into datapoints");
    for row in rows.drain(..) {
        // unwrap or skip
        let datapoint_id: i64 = row.try_get("id")?;
        let id: i64 = row.try_get("id")?;
        let nano_timestamp: i64 = row.try_get("timestamp")?;
        let value: f64 = row.try_get("value")?;

        let timestamp = chrono::DateTime::from_timestamp(nano_timestamp / 1_000_000_000, (nano_timestamp % 1_000_000_000) as u32);
        // if invalid timestamp, skip
        match timestamp {
            Some(timestamp) => datapoints.push(DataPointNumeric { id, timestamp, value }),
            None => {
                error!("invalid timestamp: {:?}", nano_timestamp);
                continue;
            }
        };

        datapoint_ids.push(datapoint_id);
    }

    info!("datapoints count: {}", datapoints.len());
    let dataseries = DataSeriesNumeric {
        dataseries_id: dataseries_id.clone(),
        values: datapoints,
    };

    // commit the transaction
    transaction.commit().await?;

    Ok(dataseries)
}

pub async fn publish_dataseries(service_bus: Arc<intercom::ServiceBus>, dataseries: &DataSeriesNumeric, topic: &str,
    priority: intercom::MessagePriority
) -> Result<()> {
    info!("publishing dataseries to service bus");

    if dataseries.values.is_empty() {
        info!("no datapoints to publish for dataseries {:?}", dataseries.dataseries_id);
        return Ok(());
    }

    let dataseries_id = dataseries.dataseries_id.to_string();

    info!("publishing {} datapoints for dataseries {:?}", dataseries.values.len(), dataseries_id);

    info!("transforming dataseries into bytearray with capnp");
    let buffer: Vec<u8> = {
        let mut message = intercom::CapnpBuilder::new_default();
        let mut capnp_dataseries =
            message.init_root::<intercom::schemas::persistor_capnp::persist_data_series::Builder>();
        capnp_dataseries.set_id(&dataseries_id);
        capnp_dataseries
            .set_type(intercom::schemas::persistor_capnp::persist_data_series::DataType::Numerical);
        let mut datapoints_builder = capnp_dataseries.init_values(
            dataseries.values.len() as u32);
        for (i, datapoint) in dataseries.values.iter().enumerate() {
            let mut datapoint_builder = datapoints_builder.reborrow().get(i as u32);
            // nano accuracy timestamp
            datapoint_builder.set_timestamp(datapoint.timestamp.timestamp_nanos_opt().unwrap_or(0));
            datapoint_builder.init_data().set_numerical(datapoint.value);
        }

        let mut buffer = Vec::new();
        intercom::serialize_packed::write_message(&mut buffer, &message)?;

        buffer
    };

    info!("attempting to send dataseries to service bus");
    let publish_status = service_bus
        .intercom_publish("amq.direct", topic,
        intercom::MessageType::PersistDataSeries, &buffer, true, priority.into())
        .await;
    if let Err(e) = publish_status {
        error!("failed to send dataseries to service bus: {:?}", e);
        return Err(e);
    }
    info!("dataseries sent to service bus");

    Ok(())
}

pub async fn mark_datapoints_as_sent(sqlite_pool: Arc<SqlitePool>, datapoint_ids: &Vec<i64>) -> Result<()> {
    // info!("marking datapoints as sent");

    info!("fetching database connection");
    let mut executor = sqlite_pool.acquire().await?;
    let mut transaction = executor.begin().await?;

    let query = format!(
        r#"
        UPDATE DataPoint
        SET sent_at = CURRENT_TIMESTAMP
        WHERE id IN ({});
        "#,
        datapoint_ids.iter().map(|id| id.to_string()).collect::<Vec<String>>().join(", ")
    );

    sqlx::query(&query)
        .execute(&mut *transaction)
        .await?;

    transaction.commit().await?;

    // info!("datapoints marked as sent");

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
