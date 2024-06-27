use anyhow::Result;
use sqlx::sqlite::SqlitePool;
use sqlx::{migrate::MigrateDatabase, migrate::Migrator, Sqlite};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

extern crate dispatcher;
extern crate mqtt_ingestor;
extern crate intercom;

const DB_URL: &str = "sqlite://./db/ingestor.db";

/**
* Main function to start the ingestor service.
* this service is responsible for collecting data from the MQTT broker, persisting it locally to the database and
* dispatching it to the service bus for further processing/analysis.
*/
#[tokio::main]
async fn main() -> Result<()> {
    info!("Starting Ingestor Service");
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();

    let sqlite_pool = Arc::new(init_sqlite_pool().await?);

    let service_bus = Arc::new(init_service_bus().await?);

    let mqtt_ingestor = init_mqtt_ingestor(sqlite_pool, service_bus).await?;

    match mqtt_ingestor.collector {
        Some(collector) => collector.await?,
        None => error!("mqtt_ingestor.collector is None"),
    }

    Ok(())
}

async fn init_sqlite_pool() -> Result<SqlitePool> {
    // create the database if it doesn't exist
    info!("checking if database exists");
    if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
        info!("creating database {}", DB_URL);
        match Sqlite::create_database(DB_URL).await {
            Ok(_) => info!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        info!("database already exists");
    }

    // initialize the database
    info!("initializing database");
    let sqlite_pool = SqlitePool::connect(DB_URL)
        .await
        .expect("failed to connect to sqlite");

    // run migrations
    info!("running migrations");
    let migrator = Migrator::new(Path::new("./migrations")).await?;
    migrator.run(&sqlite_pool).await?;
    info!("migrations complete");

    Ok(sqlite_pool)
}

async fn init_service_bus() -> Result<intercom::ServiceBus> {
    let service_bus = intercom::ServiceBus::new().await;
    if let Err(e) = service_bus {
        error!("service bus initialize failed: {:?}", e);
        return Err(e);
    }
    let service_bus = service_bus.unwrap();

    Ok(service_bus)
}

async fn init_mqtt_ingestor(
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
) -> Result<mqtt_ingestor::MqttIngestor> {
    let mqtt_connector_config = mqtt_ingestor::MqttConnectorConfig {
        mqtt_options: mqtt_ingestor::MqttConnectionOptions {
            client_id: "rumqtt-async".to_string(),
            host: "localhost".to_string(),
            port: 1883,
            keep_alive: std::time::Duration::from_secs(60),
        },
        mqtt_subscribe_topic: "agrometeo/stations/#".to_string(),
    };

    let sensor_dataseries_mapping = std::collections::HashMap::from([
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
        (
            "agrometeo/stations/5/1/1".to_string(),
            "a8442585-0168-4688-8532-31e20520a41f".to_string(),
        ),
        (
            "agrometeo/stations/6/1/1".to_string(),
            "af16f2f5-482d-45eb-9807-62143fc58d46".to_string(),
        ),
        (
            "agrometeo/stations/7/1/1".to_string(),
            "448f1dfe-747d-441d-be43-1428924633e3".to_string(),
        ),
        (
            "agrometeo/stations/8/1/1".to_string(),
            "57599e75-581d-4250-a456-c678cdf907dd".to_string(),
        ),
    ]);

    let mqtt_converter_config = mqtt_ingestor::MqttConverterConfig {
        sensor_dataseries_mapping,
    };
    // let mqtt_dispatcher_config = mqtt_ingestor::MqttDispatchConfig {
    //     dispatch_strategy: dispatcher::DispatchStrategy::Realtime
    // };
    let mqtt_dispatcher_config = mqtt_ingestor::MqttDispatchConfig {
        dispatch_strategy: dispatcher::DispatchStrategy::Batched {
            trigger: dispatcher::DispatchTriggerType::Interval {
                interval: std::time::Duration::from_secs(30),
            },
            max_batch: 1000,
        },
    };
    let dispatcher = dispatcher::Dispatcher::new(sqlite_pool.clone(), service_bus.clone()).await;
    if let Err(e) = dispatcher {
        error!("dispatcher initialize failed: {:?}", e);
        return Err(e);
    }
    let dispatcher = Arc::new(Mutex::new(dispatcher.unwrap()));

    let mut mqtt_ingestor = mqtt_ingestor::MqttIngestor::new(
        mqtt_connector_config,
        mqtt_converter_config,
        mqtt_dispatcher_config,
        dispatcher.clone(),
    )
    .unwrap();

    let _ = mqtt_ingestor.initialize().await;

    let _ = mqtt_ingestor.start().await;

    Ok(mqtt_ingestor)
}
