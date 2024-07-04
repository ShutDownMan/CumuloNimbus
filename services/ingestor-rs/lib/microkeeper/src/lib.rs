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
pub struct DataSeries {
    pub dataseries_id: Uuid,
    pub values: Vec<DataPoint>,
}

#[derive(Clone, Debug)]
pub struct DataPoint {
    pub id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub value: DataPointValue,
}

#[derive(Clone, Debug)]
pub enum DataPointValue {
    Numeric(f64),
    Text(String),
    Boolean(bool),
    Arbitrary(Vec<u8>),
    // Jsonb(serde_json::Value),
}

trait DataPointValueTrait {
    fn try_get_numerical(&self) -> Result<f64>;
    // fn try_get_text(&self) -> Result<&String>;
    // fn try_get_boolean(&self) -> Result<bool>;
    // fn try_get_arbitrary(&self) -> Result<&Vec<u8>>;
    // fn try_get_jsonb(&self) -> Result<serde_json::Value>;
}

impl DataPointValueTrait for DataPointValue {
    fn try_get_numerical(&self) -> Result<f64> {
        match self {
            DataPointValue::Numeric(value) => Ok(*value),
            _ => Err(anyhow::anyhow!("DataPointValue is not Numeric")),
        }
    }

    // fn try_get_text(&self) -> Result<&String> {
    //     match self {
    //         DataPointValue::Text(value) => Ok(value),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Text")),
    //     }
    // }

    // fn try_get_boolean(&self) -> Result<bool> {
    //     match self {
    //         DataPointValue::Boolean(value) => Ok(*value),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Boolean")),
    //     }
    // }

    // fn try_get_arbitrary(&self) -> Result<&Vec<u8>> {
    //     match self {
    //         DataPointValue::Arbitrary(value) => Ok(value),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Arbitrary")),
    //     }
    // }

    // fn try_get_jsonb(&self) -> Result<serde_json::Value> {
    //     match self {
    //         DataPointValue::Jsonb(value) => Ok(value.clone()),
    //         _ => Err(anyhow::anyhow!("DataPointValue is not Jsonb")),
    //     }
    // }
}

pub async fn save_dataseries(sqlite_pool: Arc<SqlitePool>, dataseries: &DataSeries, expiration: chrono::Duration) -> Result<Vec<i64>> {
    // this time we will save as much as possible in a single statement
    info!("saving dataseries of id {:?}", dataseries.dataseries_id);

    if dataseries.values.is_empty() {
        info!("no datapoints to save");
        return Ok(Vec::new());
    }

    // save the dataseries to the database
    info!("fetching database connection to save dataseries {:?}", dataseries.dataseries_id);
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
    .bind(dataseries.dataseries_id.to_string())
    .bind(chrono::Utc::now().timestamp())
    .fetch_one(&mut *transaction)
    .await?;
    let dataseries_id: i32 = res.try_get("id")?;
    debug!("dataseries_id: {:?}", dataseries_id);

    info!("saving datapoints of dataseries {:?} to database", dataseries.dataseries_id);
    let mut datapoint_ids = Vec::new();

    for data_point in dataseries.values.iter() {
        debug!("saving datapoint {:?}", data_point);
        let mut new_datapoint_ids = sqlx::query(r#"
            INSERT INTO DataPoint (dataseries_id, timestamp, value, created_at, expiration)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (dataseries_id, timestamp) DO UPDATE SET value = EXCLUDED.value
            RETURNING id;
        "#,)
        .bind(dataseries_id)
        .bind(data_point.timestamp.timestamp_nanos_opt().unwrap_or(0))
        .bind(data_point.value.try_get_numerical()?)
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

pub async fn get_pending_dataseries(sqlite_pool: Arc<SqlitePool>) -> Result<Vec<uuid::Uuid>> {
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

pub async fn send_pending_datapoints(sqlite_pool: Arc<SqlitePool>, service_bus: Arc<intercom::ServiceBus>, dataseries_id: &uuid::Uuid, realtime: bool) -> Result<()> {
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
            "persistor.imput", intercom::MessagePriority::Omega, !realtime).await?;

        let datapoint_ids: Vec<i64> = pending_datapoints.values.iter()
            .map(|datapoint| datapoint.id)
            .collect();
        // mark the datapoints as sent in the database
        tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
        mark_datapoints_as_sent(sqlite_pool.clone(), &datapoint_ids).await?;
    }

    Ok(())
}

async fn get_pending_datapoints(sqlite_pool: Arc<SqlitePool>, dataseries_id: &uuid::Uuid) -> Result<DataSeries> {
    // get the pending datapoints for the dataseries from the database
    info!("fetching database connection to get pending datapoints");
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
    let mut datapoints: Vec<DataPoint> = Vec::new();
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
            Some(timestamp) => datapoints.push(DataPoint { id, timestamp, value: DataPointValue::Numeric(value) }),
            None => {
                error!("invalid timestamp: {:?}", nano_timestamp);
                continue;
            }
        };

        datapoint_ids.push(datapoint_id);
    }

    info!("datapoints count: {}", datapoints.len());
    let dataseries = DataSeries {
        dataseries_id: dataseries_id.clone(),
        values: datapoints,
    };

    // commit the transaction
    transaction.commit().await?;

    Ok(dataseries)
}

pub async fn publish_dataseries(service_bus: Arc<intercom::ServiceBus>, dataseries: &DataSeries, topic: &str,
    priority: intercom::MessagePriority, compress: bool
) -> Result<()> {
    if dataseries.values.is_empty() {
        info!("no datapoints to publish for dataseries {:?}", dataseries.dataseries_id);
        return Ok(());
    }
    info!("publishing dataseries to service bus");

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
            datapoint_builder.init_data().set_numerical(datapoint.value.try_get_numerical()?);
        }

        let mut buffer = Vec::new();
        intercom::serialize_packed::write_message(&mut buffer, &message)?;

        buffer
    };

    info!("attempting to send dataseries to service bus");
    let publish_status = service_bus
        .intercom_publish("amq.direct", topic,
        intercom::MessageType::PersistDataSeries, &buffer, compress, priority.into())
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
    let mut executor = sqlite_pool.acquire().await?;
    let mut transaction = executor.begin().await?;

    let query = format!(
        r#"
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
