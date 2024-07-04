use anyhow::{Context, Result};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

extern crate dispatcher;

#[derive(Clone, Debug)]
pub struct MqttConnectionOptions {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    pub keep_alive: std::time::Duration,
}

#[derive(Clone, Debug)]
pub struct MqttConnectorConfig {
    pub mqtt_options: MqttConnectionOptions,
    pub mqtt_subscribe_topic: String,
}

#[derive(Clone, Debug)]
pub struct MqttConverterConfig {
    pub sensor_dataseries_mapping: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct MqttDispatchConfig {
    // pub dispatch_strategy: dispatcher::DispatchStrategy,
    pub dispatch_config: dispatcher::DispatcherConfig,
}

pub struct MqttIngestor {
    mqtt_connector: MqttConnectorConfig,
    mqtt_converter: MqttConverterConfig,
    mqtt_dispatcher: MqttDispatchConfig,

    mqtt_client: Option<AsyncClient>,
    mqtt_eventloop: Option<rumqttc::EventLoop>,

    pub collector: Option<tokio::task::JoinHandle<()>>,

    dispatcher: Arc<dispatcher::Dispatcher>,
}

impl MqttIngestor {
    // create a new mqtt ingestor instance by passing in the configuration
    pub fn new(
        mqtt_connector: MqttConnectorConfig,
        mqtt_converter: MqttConverterConfig,
        mqtt_dispatcher: MqttDispatchConfig,
        dispatcher: Arc<dispatcher::Dispatcher>,
    ) -> Result<Self> {
        Ok(MqttIngestor {
            mqtt_connector,
            mqtt_converter,
            mqtt_dispatcher,
            mqtt_client: None,
            mqtt_eventloop: None,
            collector: None,
            dispatcher,
        })
    }

    pub async fn initialize(&mut self) -> Result<()> {
        // subscribe to the mqtt topics
        info!("mqtt_main connecting");
        let mut mqtt_options = MqttOptions::new(
            self.mqtt_connector.mqtt_options.client_id.clone(),
            self.mqtt_connector.mqtt_options.host.clone(),
            self.mqtt_connector.mqtt_options.port,
        );
        mqtt_options.set_keep_alive(self.mqtt_connector.mqtt_options.keep_alive);

        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);

        self.mqtt_client = Some(client);
        self.mqtt_eventloop = Some(eventloop);

        // subscribe to the mqtt topics
        info!("mqtt_main subscribing");
        if let Some(mqtt_client) = self.mqtt_client.as_mut() {
            mqtt_client
                .subscribe(
                    self.mqtt_connector.mqtt_subscribe_topic.clone(),
                    QoS::AtMostOnce,
                )
                .await
                .context("mqtt subscribe failed")?;
            info!("mqtt_main subscribed");
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        // start a thread that polls the mqtt eventloop and dispatches the data
        // to the dispatcher
        let mqtt_eventloop = self
            .mqtt_eventloop
            .take()
            .context("mqtt eventloop not initialized")?;
        let mqtt_converter = self.mqtt_converter.clone();

        let dispatcher = self.dispatcher.clone();
        let dispatch_config = self.mqtt_dispatcher.dispatch_config.clone();

        // spawn collector thread
        self.collector = Some(tokio::spawn(async move {
            if let Err(err) = Self::collect(
                mqtt_eventloop,
                mqtt_converter,
                dispatcher,
                dispatch_config,
            )
            .await
            {
                error!("mqtt collect failed: {}", err);
            }
        }));

        Ok(())
    }

    pub async fn collect(
        mut mqtt_eventloop: rumqttc::EventLoop,
        mqtt_converter: MqttConverterConfig,
        dispatcher: Arc<dispatcher::Dispatcher>,
        dispatch_config: dispatcher::DispatcherConfig,
    ) -> Result<()> {
        info!("mqtt collect started");
        let dispatch_strategy = &dispatch_config.dispatch_strategy;

        let mut dataseries_buffer: HashMap<Uuid, Vec<dispatcher::DataPoint>> = HashMap::new();

        let infiniterval = tokio::time::interval(std::time::Duration::from_secs(u64::MAX));
        let mut interval = match dispatch_strategy {
            dispatcher::DispatchStrategy::Realtime => infiniterval,
            dispatcher::DispatchStrategy::Batched { trigger, .. } => match trigger {
                dispatcher::DispatchTrigger::Interval { interval } => {
                    tokio::time::interval(*interval)
                }
                _ => infiniterval,
            },
        };

        info!("mqtt collect loop started");
        loop {
            let notification = mqtt_eventloop.poll();

            // select between the mqtt eventloop and the interval
            tokio::select! {
                // biased;
                notification = notification => {
                    // let notification = notification.context("mqtt eventloop poll failed")?;

                    match notification {
                        Ok(notification) => Self::handle_notification(
                            notification,
                            &mqtt_converter,
                            &mut dataseries_buffer,
                            &dispatcher,
                            &dispatch_config,
                        ).await.context("mqtt handle notification failed")?,
                        Err(err) => warn!("notification result failed {}", err)
                    }
                },
                _ = interval.tick() => Self::handle_interval_tick(&mut dataseries_buffer, &dispatcher, &dispatch_config).await
                    .context("mqtt handle interval tick failed")?,
                else => {
                    warn!("no branch selected in mqtt eventloop poll");
                }
            }
        }
    }

    async fn handle_notification(
        notification: rumqttc::Event,
        mqtt_converter: &MqttConverterConfig,
        dataseries_buffer: &mut HashMap<Uuid, Vec<dispatcher::DataPoint>>,
        dispatcher: &Arc<dispatcher::Dispatcher>,
        dispatch_config: &dispatcher::DispatcherConfig,
    ) -> Result<()> {
        if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) = notification {
            // check if the topic path is in the mapping
            if let Some(dataseries_id) =
                mqtt_converter.sensor_dataseries_mapping.get(&publish.topic)
            {
                // convert the payload to a f64
                let payload = match std::str::from_utf8(&publish.payload) {
                    Ok(payload) => payload,
                    Err(err) => {
                        error!("mqtt payload is not valid utf8: {}", err);
                        return Err(err.into());
                    }
                };
                // convert the payload to a f64
                let value = match payload.parse::<f64>() {
                    Ok(value) => value,
                    Err(err) => {
                        error!("mqtt payload is not a valid f64: {}", err);
                        return Err(err.into());
                    }
                };
                // convert the dataseries id to a uuid
                let dataseries_uuid = match Uuid::parse_str(dataseries_id) {
                    Ok(dataseries_uuid) => dataseries_uuid,
                    Err(err) => {
                        error!("mqtt dataseries id is not a valid uuid: {}", err);
                        return Err(err.into());
                    }
                };

                // debug!(
                //     "adding datapoint to dataseries {}",
                //     dataseries_uuid.to_string()
                // );
                // check if the dataseries is not already in the buffer
                dataseries_buffer
                    .entry(dataseries_uuid)
                    .or_insert_with(Vec::new);

                // add the datapoint to the buffer
                dataseries_buffer
                    .get_mut(&dataseries_uuid)
                    .unwrap()
                    .push(dispatcher::DataPoint {
                        id: 0,
                        timestamp: chrono::Utc::now(),
                        value: dispatcher::DataPointValue::Numeric(value),
                    });

                if let Some(dataseries_buffer) = dataseries_buffer.get_mut(&dataseries_uuid) {
                    debug!(
                        "dataseries {} buffer length: {}",
                        dataseries_uuid,
                        dataseries_buffer.len()
                    );
                    let should_dispatch_persist = check_dispatch_trigger(
                        dataseries_buffer,
                        dispatch_config,
                        &dataseries_uuid,
                    )
                    .await;

                    if should_dispatch_persist {
                        // create a dataseries
                        let dataseries = dispatcher::DataSeries {
                            dataseries_id: dataseries_uuid,
                            values: dataseries_buffer.clone(),
                        };

                        // dispatch the dataseries
                        let dispatcher = dispatcher.clone();
                        let dispatch_config = dispatch_config.clone();
                        // run in another real thread to avoid deadlocking
                        tokio::task::spawn_blocking(move || {
                            if let Err(err) = dispatcher.dispatch_to_persistor(&dataseries, &dispatch_config) {
                                error!("mqtt dispatch failed when handling notification: {}", err);
                            }
                        });

                        // clear the buffer
                        dataseries_buffer.clear();
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_interval_tick(
        dataseries_buffer: &mut HashMap<Uuid, Vec<dispatcher::DataPoint>>,
        dispatcher: &Arc<dispatcher::Dispatcher>,
        dispatch_config: &dispatcher::DispatcherConfig,
    ) -> Result<()> {
        info!("mqtt interval tick");
        // for each dataseries in the buffer
        if dataseries_buffer.is_empty() {
            info!("dataseries buffer is empty");
            return Ok(());
        }
        for (dataseries_uuid, dataseries_buffer) in dataseries_buffer.iter_mut() {
            debug!("mqtt dispatching {} values for dataseries {}", dataseries_buffer.len(), dataseries_uuid);

            // create a dataseries
            let dataseries = dispatcher::DataSeries {
                dataseries_id: *dataseries_uuid,
                values: dataseries_buffer.clone(),
            };

            // dispatch the dataseries
            let dispatcher = dispatcher.clone();
            let dispatch_config = dispatch_config.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(err) = dispatcher.dispatch_to_persistor(&dataseries, &dispatch_config) {
                    error!("mqtt dispatch failed: {}", err);
                }
            });

            debug!("mqtt dispatched dataseries: {}", dataseries_uuid);

            // clear the buffer
            dataseries_buffer.clear();
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        // stop the thread that polls the mqtt eventloop
        if let Some(collector) = self.collector.take() {
            // stop the thread
            collector.abort();
        }

        // unsubscribe from the mqtt topics
        if let Some(mqtt_client) = self.mqtt_client.take() {
            mqtt_client
                .unsubscribe(self.mqtt_connector.mqtt_subscribe_topic.clone())
                .await?;
        }

        Ok(())
    }
}

async fn check_dispatch_trigger(
    dataseries_buffer: &Vec<dispatcher::DataPoint>,
    dispatch_config: &dispatcher::DispatcherConfig,
    dataseries_uuid: &Uuid,
) -> bool {
    let dispatch_strategy = &dispatch_config.dispatch_strategy;
    let mut should_dispatch = false;
    // debug!("checking dispatch trigger for dataseries {}", dataseries_uuid);
    // check if dispatch strategy is set to batch
    match dispatch_strategy {
        dispatcher::DispatchStrategy::Batched { max_batch, trigger } => {
            // check if the buffer is full
            if dataseries_buffer.len() >= *max_batch {
                should_dispatch = true;
                debug!("should dispatch batched dataseries {}", dataseries_uuid);
            }

            // check if holdoff time is reached
            if let dispatcher::DispatchTrigger::Holdoff { holdoff } = trigger {
                if let Some(first_datapoint) = dataseries_buffer.first() {
                    let last_datapoint_timestamp = first_datapoint.timestamp;
                    let now = chrono::Utc::now();
                    debug!(
                        "now: {}, last datapoint timestamp: {}, holdoff: {:#?}",
                        now, last_datapoint_timestamp, holdoff
                    );
                    debug!("time until holdoff: {}", now - last_datapoint_timestamp);
                    if now - last_datapoint_timestamp > *holdoff {
                        should_dispatch = true;
                        debug!("should dispatch holdoff dataseries {}", dataseries_uuid);
                    }
                }
            }
        }
        dispatcher::DispatchStrategy::Realtime => {
            should_dispatch = true;
            debug!("should dispatch realtime dataseries {}", dataseries_uuid);
        }
    }

    should_dispatch
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[test]
    // fn it_works() {
    //     let result = 2 + 2;
    //     assert_eq!(result, 4);
    // }
}
