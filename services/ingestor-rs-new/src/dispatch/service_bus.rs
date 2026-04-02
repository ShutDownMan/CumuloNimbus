//! Service bus dispatcher
//!
//! This module implements dispatching to a service bus.

use async_trait::async_trait;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing::{debug, error, info};

use super::{strategy::DispatchStrategy, Dispatcher};
use crate::config;
use crate::domain::{DataSeries, NumericDataPoint};
use crate::error::{IngestorError, IngestorResult};

/// Dispatcher that sends data to a service bus
pub struct ServiceBusDispatcher {
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
    tokio_handle: Handle,
    config: config::DispatchConfig,
}

impl ServiceBusDispatcher {
    /// Create a new service bus dispatcher
    pub fn new(
        sqlite_pool: Arc<SqlitePool>,
        service_bus: Arc<intercom::ServiceBus>,
        tokio_handle: Handle,
        config: config::DispatchConfig,
    ) -> Self {
        Self {
            sqlite_pool,
            service_bus,
            tokio_handle,
            config,
        }
    }

    /// Store data points in temporary storage
    async fn store_in_temp_storage(
        &self,
        dataseries: &DataSeries<NumericDataPoint>,
    ) -> IngestorResult<Vec<i64>> {
        let mut datapoint_ids = Vec::with_capacity(dataseries.values.len());

        // Begin a transaction
        let mut tx =
            self.sqlite_pool.begin().await.map_err(|e| {
                IngestorError::Database(format!("Failed to begin transaction: {}", e))
            })?;

        for datapoint in &dataseries.values {
            let timestamp = datapoint.timestamp().timestamp_millis();
            let value = *datapoint.value();

            let id = sqlx::query!(
                r#"
                INSERT INTO datapoints (dataseries_id, timestamp, value, sent)
                VALUES (?, ?, ?, 0)
                RETURNING id
                "#,
                dataseries.metadata.id,
                timestamp,
                value
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| IngestorError::Database(format!("Failed to insert datapoint: {}", e)))?
            .id;

            datapoint_ids.push(id);
        }

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| IngestorError::Database(format!("Failed to commit transaction: {}", e)))?;

        Ok(datapoint_ids)
    }

    /// Mark data points as sent
    async fn mark_as_sent(&self, datapoint_ids: &[i64]) -> IngestorResult<()> {
        // Convert to a comma-separated list
        let ids = datapoint_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Mark as sent in a single query
        sqlx::query(&format!(
            "UPDATE datapoints SET sent = 1 WHERE id IN ({})",
            ids
        ))
        .execute(&*self.sqlite_pool)
        .await
        .map_err(|e| {
            IngestorError::Database(format!("Failed to mark datapoints as sent: {}", e))
        })?;

        Ok(())
    }

    /// Dispatch based on the configured strategy
    async fn dispatch_with_strategy(
        &self,
        dataseries: &DataSeries<NumericDataPoint>,
    ) -> IngestorResult<bool> {
        match &self.config.strategy {
            DispatchStrategy::Realtime => self.dispatch_realtime(dataseries).await,
            DispatchStrategy::Batched { max_batch, trigger } => {
                self.dispatch_batched(dataseries, *max_batch, trigger).await
            }
        }
    }

    /// Dispatch in real-time mode
    async fn dispatch_realtime(
        &self,
        dataseries: &DataSeries<NumericDataPoint>,
    ) -> IngestorResult<bool> {
        // Convert to intercom DataSeries
        let intercom_dataseries = self.convert_to_intercom_dataseries(dataseries)?;

        // Publish to service bus
        self.service_bus
            .publish(
                "persist-dataseries",
                &intercom_dataseries,
                intercom::MessagePriority::Normal,
                true,
            )
            .await
            .map_err(|e| {
                IngestorError::ServiceBus(format!("Failed to publish to service bus: {}", e))
            })?;

        Ok(true)
    }

    /// Dispatch in batched mode
    async fn dispatch_batched(
        &self,
        dataseries: &DataSeries<NumericDataPoint>,
        _max_batch: usize,
        _trigger: &config::DispatchTrigger,
    ) -> IngestorResult<bool> {
        // For now, implement a simple batched strategy
        // In a real implementation, this would collect data points until the trigger fires

        // Store in temporary storage first
        info!(
            "Storing data series with id {} in temporary storage",
            dataseries.metadata.id
        );

        // Convert to intercom DataSeries
        let intercom_dataseries = self.convert_to_intercom_dataseries(dataseries)?;

        // Publish to service bus
        self.service_bus
            .publish(
                "persist-dataseries",
                &intercom_dataseries,
                intercom::MessagePriority::Normal,
                false,
            )
            .await
            .map_err(|e| {
                IngestorError::ServiceBus(format!("Failed to publish to service bus: {}", e))
            })?;

        Ok(true)
    }

    /// Convert our domain DataSeries to an intercom DataSeries
    fn convert_to_intercom_dataseries(
        &self,
        dataseries: &DataSeries<NumericDataPoint>,
    ) -> IngestorResult<intercom::NumericDataSeries> {
        // Create intercom DataSeriesMetadata
        let metadata = intercom::DataSeriesMetadata {
            id: dataseries.metadata.id.clone(),
            name: dataseries.metadata.name.clone(),
            description: dataseries.metadata.description.clone(),
            data_type: match dataseries.metadata.data_type {
                crate::domain::DataType::Numeric => intercom::DataSeriesDataType::Numeric,
                crate::domain::DataType::Text => intercom::DataSeriesDataType::Text,
            },
        };

        // Create intercom DataPoints
        let values = dataseries
            .values
            .iter()
            .map(|point| intercom::DataPoint {
                timestamp: point.timestamp().timestamp_millis(),
                value: intercom::NumericDataPoint {
                    numeric: *point.value(),
                },
            })
            .collect();

        // Create intercom DataSeries
        let intercom_dataseries = intercom::DataSeries { metadata, values };

        Ok(intercom_dataseries)
    }
}

#[async_trait]
impl Dispatcher for ServiceBusDispatcher {
    async fn dispatch(&self, dataseries: &DataSeries<NumericDataPoint>) -> IngestorResult<()> {
        // Use the configured dispatch strategy
        let dispatch_success = self.dispatch_with_strategy(dataseries).await?;

        // Store in temporary storage if configured
        if let Some(duration) = self.config.temporary_storage_duration {
            let datapoint_ids = self.store_in_temp_storage(dataseries).await?;

            // If successfully published, mark as sent
            if dispatch_success {
                self.mark_as_sent(&datapoint_ids).await?;
            }

            // TODO: Add cleanup job for expired data
        }

        if !dispatch_success {
            error!("Failed to dispatch or save dataseries");
            return Err(IngestorError::ServiceBus(
                "Failed to dispatch data series".to_string(),
            ));
        }

        Ok(())
    }
}
