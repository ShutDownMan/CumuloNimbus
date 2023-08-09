use anyhow::Result;
use std::sync::Arc;
use tracing::error;

extern crate dispatcher;
extern crate mqtt_ingestor;
extern crate service_bus;

#[tokio::main]
async fn main() -> Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let service_bus = init_service_bus().await?;

    service_bus.queue_declare("ingestor").await?;

    let mqtt_ingestor = init_mqtt_ingestor(service_bus).await?;

    match mqtt_ingestor.collector {
        Some(collector) => collector.await?,
        None => error!("mqtt_ingestor.collector is None"),
    }

    Ok(())
}

async fn init_service_bus() -> Result<service_bus::ServiceBus> {
    let service_bus = service_bus::ServiceBus::new().await;
    if let Err(e) = service_bus {
        error!("service bus initialize failed: {:?}", e);
        return Err(e);
    }
    let service_bus = service_bus.unwrap();

    Ok(service_bus)
}

async fn init_mqtt_ingestor(
    service_bus: service_bus::ServiceBus,
) -> Result<mqtt_ingestor::MqttIngestor> {
    let mqtt_connector_config = mqtt_ingestor::MqttConnectorConfig {
        mqtt_options: mqtt_ingestor::MqttConnectionOptions {
            client_id: "rumqtt-async".to_string(),
            host: "localhost".to_string(),
            port: 1883,
            keep_alive: std::time::Duration::from_secs(5),
        },
        mqtt_subscribe_topic: "agrometeo/stations/#".to_string(),
    };

    let mut _sensor_dataseries_mapping = std::collections::HashMap::from([
        (
            "agrometeo/stations/1/1/1".to_string(),
            "94442585-0168-4688-8532-31e20520a41f".to_string(),
        ),
        (
            "agrometeo/stations/2/1/1".to_string(),
            "7a16f2f5-482d-45eb-9807-62143fc58d46".to_string(),
        ),
        (
            "agrometeo/stations/3/1/1".to_string(),
            "cb8f1dfe-747d-441d-be43-1428924633e3".to_string(),
        ),
        (
            "agrometeo/stations/4/1/1".to_string(),
            "8f599e75-581d-4250-a456-c678cdf907dd".to_string(),
        ),
    ]);

    let mqtt_converter_config = mqtt_ingestor::MqttConverterConfig {
        sensor_dataseries_mapping: _sensor_dataseries_mapping,
    };
    let mqtt_dispatcher_config = mqtt_ingestor::MqttDispatchConfig {
        dispatch_strategy: dispatcher::DispatchStrategy::Batched {
            trigger: dispatcher::DispatchTriggerType::Interval {
                interval: std::time::Duration::from_secs(5),
            },
            max_batch: 100,
        },
    };
    let service_bus = Arc::new(service_bus);
    let dispatcher = dispatcher::Dispatcher::new(service_bus).await;
    if let Err(e) = dispatcher {
        error!("dispatcher initialize failed: {:?}", e);
        return Err(e);
    }
    let dispatcher = Arc::new(dispatcher.unwrap());

    let mut mqtt_ingestor = mqtt_ingestor::MqttIngestor::new(
        mqtt_connector_config,
        mqtt_converter_config,
        mqtt_dispatcher_config,
        dispatcher,
    );

    let _ = mqtt_ingestor.initialize().await;

    let _ = mqtt_ingestor.start().await;

    Ok(mqtt_ingestor)
}
