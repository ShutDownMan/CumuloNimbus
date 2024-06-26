use anyhow::Result;
use datakeeper::DataKeeper;
use sqlx::postgres::{PgPoolOptions, PgPool};
use sqlx::{migrate::MigrateDatabase, migrate::Migrator, Postgres};
use std::path::Path;
use std::sync::Arc;
use tracing::{error, debug, info};
use tracing_subscriber::EnvFilter;
use std::env;

extern crate intercom;
extern crate datakeeper;

/**
* Main function to start the persistor service.
* this service is responsible for persisting data to the database and
* dispatching it to the service bus when queried.
*/
#[tokio::main]
async fn main() -> Result<()> {
    info!("Starting Persistor Service");
    init_log();

    let timescale_pool = Arc::new(init_timescale_pool().await?);

    let service_bus = Arc::new(init_service_bus().await?);
    service_bus
        .simple_queue_declare("persistor.input", "persist-dataseries")
        .await?;

    let data_keeper = init_data_keeper(timescale_pool, service_bus).await?;

    data_keeper.handle_messages().await?;

    Ok(())
}

fn init_log() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();
}

async fn init_timescale_pool() -> Result<PgPool> {
    debug!("Fetching database URL from env");
    let db_url = env::var("PERSISTOR_DB_URL")?;
    let max_connections = env::var("PERSISTOR_DB_MAX_CONNECTIONS")
        .unwrap_or("5".into())
        .parse::<u32>()?;

    debug!("{:#?}", db_url);

    info!("checking if database exists");
    if !Postgres::database_exists(db_url.as_str()).await.unwrap_or(false) {
        info!("creating database {}", db_url);
        match Postgres::create_database(db_url.as_str()).await {
            Ok(_) => info!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        info!("database already exists");
    }

    info!("initializing database");
    let timescale_pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(db_url.as_str())
        .await
        .expect("failed to connect to timescale");

    info!("running migrations");
    let migrator = Migrator::new(Path::new("./migrations")).await?;
    migrator.run(&timescale_pool).await?;
    info!("migrations complete");

    Ok(timescale_pool)
}

async fn init_service_bus() -> Result<intercom::ServiceBus> {
    debug!("Instantiating service bus");
    let service_bus = intercom::ServiceBus::new().await;
    if let Err(e) = service_bus {
        error!("service bus initialize failed: {:?}", e);
        return Err(e);
    }
    // TODO: remove unwrap and treat connection errors with retries
    let service_bus = service_bus.unwrap();

    Ok(service_bus)
}

async fn init_data_keeper(
    postgres_pool: Arc<PgPool>,
    service_bus: Arc<intercom::ServiceBus>,
) -> Result<datakeeper::DataKeeper> {
    DataKeeper::new(postgres_pool, service_bus)
}
