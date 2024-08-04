use anyhow::Result;
use tracing::{debug, error, warn, info};
use sqlx::sqlite::SqlitePool;
use std::sync::Arc;
use uuid::Uuid;
use std::time::Duration as StdDuration;

extern crate intercom;

pub struct HousekeeperConfig {
    pub work_interval: StdDuration,
    pub idle_interval: StdDuration,
    pub patience_falloff_rate: f64,
    pub patience_recovery_rate: f64,
    pub patience_min_threshold: f64,
}

pub struct Housekeeper {
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
}

#[derive(Clone, Debug)]
pub struct DataPointNumeric {
    pub id: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub value: f64,
}

#[derive(Debug)]
pub struct DataSeriesNumeric {
    pub dataseries_id: Uuid,
    pub values: Vec<DataPointNumeric>,
}

impl Housekeeper {
    pub fn new(
        sqlite_pool: Arc<SqlitePool>,
        service_bus: Arc<intercom::ServiceBus>,
    ) -> Result<Self> {
        // create the houseKeeper
        Ok(Housekeeper {
            sqlite_pool,
            service_bus,
        })
    }

    pub async fn run(&self, config: HousekeeperConfig) -> Result<()> {
        info!("housekeeper started");
        // start the housekeeper loop
        self.housekeeper_loop(config).await
    }

    async fn housekeeper_loop(&self, config: HousekeeperConfig) -> Result<()> {
        let mut patience = 1.0;
        let mut working = false;

        loop {
            tokio::time::sleep(Self::calculate_sleep_duration(&config, patience, working)).await;
            // check if there are any pending dataseries
            info!("checking for pending dataseries");
            debug!("patience: {}", patience);
            debug!("working: {}", working);
            tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
            let pending_dataseries = microkeeper::get_pending_dataseries_ids(self.sqlite_pool.clone()).await;
            // if we could not lock, decrease the patience and sleep for a while
            let pending_dataseries = match pending_dataseries {
                Ok(pending_dataseries) => pending_dataseries,
                Err(e) => {
                    match e.downcast_ref::<microkeeper::DatabaseLockError>() {
                        Some(_) => {
                            warn!("could not lock the database");
                            patience = (patience * config.patience_falloff_rate).clamp(0.0, 1.0);
                            // if no patience left, skip the rest of the loop
                            if patience < config.patience_min_threshold {
                                continue;
                            }
                        }
                        None => {
                            error!("error getting pending dataseries");
                        }
                    }
                    continue;
                }
            };

            info!("pending dataseries found: {:?}", pending_dataseries);

            // if there are no pending dataseries, we skip the rest of the loop
            if pending_dataseries.is_empty() {
                info!("no pending dataseries found");
                working = false;
                patience = 1.0;
            } else {
                working = true;

                // dispatch the dataseries
                for dataseries_id in pending_dataseries.iter() {
                    // loop until we run out of patience
                    while patience > config.patience_min_threshold {
                        // send the pending datapoints
                        tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
                        let send_result = microkeeper::send_pending_numeric_datapoints(
                            self.sqlite_pool.clone(), self.service_bus.clone(), dataseries_id, false).await;

                        match send_result {
                            Ok(_) => {
                                info!("datapoints for dataseries {:?} sent successfully", dataseries_id);
                                // recover patience
                                patience = (patience + config.patience_recovery_rate).clamp(0.0, 1.0);
                                break;
                            }
                            Err(e) => {
                                match e.downcast_ref::<microkeeper::DatabaseLockError>() {
                                    Some(_) => {
                                        warn!("could not lock the database");
                                        // decrease patience
                                        patience = (patience * config.patience_falloff_rate).clamp(0.0, 1.0);
                                        tokio::time::sleep(Self::calculate_sleep_duration(&config, patience, working)).await;
                                        continue;
                                    }
                                    None => {
                                        error!("error sending datapoints");
                                        break;
                                    }
                                }
                            }
                        };
                    }

                    debug!("patience: {}", patience);
                    if patience == 0.0 {
                        warn!("ran out of patience!");
                        break;
                    }
                }
            }

            // delete expired datapoints
            loop {
                tokio::task::yield_now().await; // gives sqlx a chance to release the previous connection
                let expired_datapoints = microkeeper::delete_expired_datapoints(self.sqlite_pool.clone()).await;
                match expired_datapoints {
                    Ok(_) => {
                        info!("expired datapoints deleted successfully");
                        break;
                    }
                    Err(e) => {
                        match e.downcast_ref::<microkeeper::DatabaseLockError>() {
                            Some(_) => {
                                warn!("could not lock the database");
                                tokio::time::sleep(Self::calculate_sleep_duration(&config, patience, working)).await;
                                continue;
                            }
                            None => {
                                error!("error deleting expired datapoints");
                                break;
                            }
                        }
                    }
                }
            }

            working = false;
            patience = 1.0;
        }
    }

    fn calculate_sleep_duration(config: &HousekeeperConfig, patience: f64, working: bool) -> StdDuration {
        // calculate the sleep duration based on the patience and the presence of pending dataseries
        if working {
            // working interval with patience
            let interval_milis = (config.work_interval.as_millis() as f64) * patience;
            StdDuration::from_millis(interval_milis as u64)
        } else {
            // idle interval with patience
            let interval_milis = (config.idle_interval.as_millis() as f64) * patience;
            StdDuration::from_millis(interval_milis as u64)
        }
    }
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
