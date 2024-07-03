use anyhow::Result;
use sqlx::sqlite::{SqlitePoolOptions, SqlitePool};
use sqlx::{migrate::MigrateDatabase, migrate::Migrator, Sqlite};
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info, debug};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;
use sha1::{Sha1, Digest};

extern crate dispatcher;
extern crate mqtt_ingestor;
extern crate housekeeper;
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
        .with_line_number(true)
        .init();

    let sqlite_pool = Arc::new(init_sqlite_pool().await?);

    let service_bus = Arc::new(init_service_bus().await?);

    let housekeeper = init_housekeeper(sqlite_pool.clone(), service_bus.clone()).await?;

    let dispatcher = init_dispatcher(sqlite_pool.clone(), service_bus.clone()).await?;

    let housekeeper_config = housekeeper::HousekeeperConfig {
        idle_interval: std::time::Duration::from_millis(15000),
        work_interval: std::time::Duration::from_millis(2500),
        patience_falloff_rate: 0.5,
        patience_recovery_rate: 0.1,
        patience_min_threshold: 0.01,
    };

    // spawn and detach the housekeeper task
    tokio::runtime::Handle::current().spawn(async move {
        housekeeper.run(housekeeper_config).await.unwrap();
    });

    let mqtt_ingestor = init_mqtt_ingestor(dispatcher.clone()).await?;

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
    let sqlite_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect(DB_URL)
        .await?;

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

async fn init_housekeeper(
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
) -> Result<housekeeper::Housekeeper> {
    let housekeeper = housekeeper::Housekeeper::new(sqlite_pool.clone(),
        service_bus.clone());
    if let Err(e) = housekeeper {
        error!("housekeeper initialize failed: {:?}", e);
        return Err(e);
    }
    let housekeeper = housekeeper.unwrap();

    Ok(housekeeper)
}

async fn init_dispatcher(
    sqlite_pool: Arc<SqlitePool>,
    service_bus: Arc<intercom::ServiceBus>,
) -> Result<Arc<dispatcher::Dispatcher>> {
    let dispatcher = dispatcher::Dispatcher::new(sqlite_pool.clone(), service_bus.clone(),
        tokio::runtime::Handle::current());
    if let Err(e) = dispatcher {
        error!("dispatcher initialize failed: {:?}", e);
        return Err(e);
    }
    let dispatcher = dispatcher.unwrap();

    Ok(Arc::new(dispatcher))
}

async fn init_mqtt_ingestor(dispatcher: Arc<dispatcher::Dispatcher>) -> Result<mqtt_ingestor::MqttIngestor> {
    let mqtt_connector_config = mqtt_ingestor::MqttConnectorConfig {
        mqtt_options: mqtt_ingestor::MqttConnectionOptions {
            client_id: "rumqtt-async".to_string(),
            host: "localhost".to_string(),
            port: 1883,
            keep_alive: std::time::Duration::from_secs(60),
        },
        mqtt_subscribe_topic: "agrometeo/stations/#".to_string(),
    };

    let station_ids = 8;
    let sensor_ids = 6;
    let magnitude_ids = 2;

    let mut sensor_dataseries_mapping = std::collections::HashMap::new();

    for station_id in 1..=station_ids {
        for sensor_id in 1..=sensor_ids {
            for magnitude_id in 1..=magnitude_ids {
                let topic = format!("agrometeo/stations/{}/{}/{}", station_id, sensor_id, magnitude_id);
                let dataseries_id = generate_uuid_from_seed(&topic).to_string();
                sensor_dataseries_mapping.insert(topic, dataseries_id);
            }
        }
    }

    let mqtt_converter_config = mqtt_ingestor::MqttConverterConfig {
        sensor_dataseries_mapping,
    };
    // let mqtt_dispatcher_config = mqtt_ingestor::MqttDispatchConfig {
    //     dispatch_config: dispatcher::DispatcherConfig {
    //         dispatch_strategy: dispatcher::DispatchStrategy::Realtime,
    //         temporary_storage: None,
    //     }
    // };
    let mqtt_dispatcher_config = mqtt_ingestor::MqttDispatchConfig {
        dispatch_config: dispatcher::DispatcherConfig {
            dispatch_strategy: dispatcher::DispatchStrategy::Batched {
                trigger: dispatcher::DispatchTrigger::Interval {
                    interval: std::time::Duration::from_secs(30),
                },
                max_batch: 2300,
            },
            // 1 week storage
            temporary_storage: Some(chrono::Duration::weeks(1)),
        },
    };

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


fn generate_uuid_from_seed(seed: &str) -> Uuid {
    // Create a SHA-1 hash from the seed
    let mut hasher = Sha1::new();
    hasher.update(seed.as_bytes());
    let hash = hasher.finalize();
    // Convert the hash to a string and take the first 16 characters
    let hash: String = format!("{:x}", hash);
    let hash = &hash[0..32];

    debug!("hash: {}", hash);

    // Generate a UUID v5 from the hash
    Uuid::parse_str(&hash).unwrap()
}