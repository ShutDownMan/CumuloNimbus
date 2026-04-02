# CumuloNimbus: Comprehensive Telemetry Solution

## Project Overview

CumuloNimbus is a comprehensive solution for telemetry needs spanning data gathering, persistence, monitoring, and reporting. It provides a robust infrastructure for collecting, processing, and analyzing time-series data from various sources, particularly focused on meteorological and environmental data.

## System Architecture

The system follows a microservices architecture with several components interconnected through message brokers. The main components are:

### Data Flow Architecture

1. **Data Ingestion** → **Data Processing** → **Data Storage** → **Data Visualization**

### Core Components

#### 1. Data Ingestion (Ingestor)
- Collects data from MQTT broker
- Persists data locally in SQLite database 
- Dispatches data to the service bus for further processing

#### 2. Data Persistence (Persistor)
- Handles long-term storage of time-series data
- Uses TimescaleDB (PostgreSQL extension optimized for time-series data)
- Provides data retrieval services 

#### 3. Data Processing (Baker)
- Processes data according to defined recipes
- Executes computations on data series
- Supports custom data transformations

#### 4. Data Simulation (Agrometeo Simulator)
- Generates synthetic meteorological data for testing
- Publishes data to MQTT broker

#### 5. Visualization (Grafana)
- Provides dashboards for data visualization
- Integrates with the persisted time-series data

## Technology Stack

### Languages
- **Rust**: Core services (Ingestor, Persistor, Baker)
- **Python**: Data simulation (Agrometeo Simulator)
- **JavaScript**: Web UI components

### Databases
- **TimescaleDB**: Time-series data storage (PostgreSQL extension)
- **SQLite**: Local storage for the Ingestor service

### Message Brokers
- **MQTT (Mosquitto)**: IoT data ingestion
- **RabbitMQ**: Inter-service communication

### Containerization
- **Docker**: Service containerization
- **Docker Compose**: Service orchestration

### Monitoring & Visualization
- **Grafana**: Data visualization dashboards

## Key Components Details

### 1. Agrometeo Simulator (Python)
A Python service that simulates agrometeorology sensor data. It generates random data using Perlin noise to create realistic environmental measurements and publishes them to an MQTT broker.

Features:
- Creates simulated data for multiple virtual stations, sensors, and magnitudes
- Configurable publishing interval
- Uses MQTT protocol for communication

### 2. Ingestor Service (Rust)
Collects data from the MQTT broker, processes it, and forwards it to other services. It has several subcomponents:

Components:
- **MQTT Ingestor**: Connects to MQTT broker and collects messages
- **Dispatcher**: Routes data to appropriate destinations
- **Housekeeper**: Manages system maintenance tasks

Features:
- Local SQLite storage for data buffering
- Configurable dispatch strategies (realtime/batched)
- UUID-based data series identification

### 3. Persistor Service (Rust)
Manages long-term storage of time-series data in TimescaleDB.

Features:
- Handles database migrations
- Provides data storage APIs
- Integration with the service bus for data retrieval

### 4. Baker Service (Rust)
Processes and transforms data series according to recipes. It computes new data series based on existing ones using defined formulas.

Features:
- Recipe-based data processing
- WASM-based computation engine
- Support for complex data transformations

### 5. Intercom System
Facilitates communication between services using RabbitMQ as a message broker. It defines message schemas using Cap'n Proto for efficient binary serialization.

Message Types:
- DataSeries: Represents time-series data
- PersistDataSeries: Commands to store data
- ComputeDataSeries: Instructions for data processing

## Data Processing Language

CumuloNimbus appears to include a domain-specific language for data processing, as seen in the `example.tsky` file. This language provides features for:

- Time-series data manipulation
- Data interpolation strategies (LOCF - Last Observation Carried Forward, LERP - Linear Interpolation)
- Mathematical operations on data series
- Data mirroring and referencing

Example syntax:
```
function main(dataseries_a, dataseries_b):
    A = locf dataseries_a;
    B = lerp dataseries_b;
    return A + B mirroring (dataseries_a, dataseries_b);
```

## Deployment

CumuloNimbus is containerized using Docker, with service definitions in the `docker-compose.yml` file. The system requires configuration through environment variables before deployment.

Services deployed include:
- Mosquitto (MQTT broker)
- Agrometeo Simulator
- TimescaleDB (as dataseries-db)
- PostgreSQL Admin
- RabbitMQ (as intercom)
- Grafana

## Development Status

The project appears to be a functioning telemetry solution with components for data collection, processing, storage, and visualization. The modular architecture allows for scalability and flexibility in handling various types of time-series data.

## Future Enhancement Opportunities

1. **Scalability Improvements**: Implement horizontal scaling for high-volume data ingestion
2. **Security Enhancements**: Add authentication and encryption for data transmission
3. **Additional Data Sources**: Integrate with more data sources beyond MQTT
4. **Advanced Analytics**: Implement machine learning models for predictive analytics
5. **Web Interface**: Develop a dedicated web UI for system configuration and monitoring
6. **Alert System**: Add configurable alerts for abnormal data patterns

## Conclusion

CumuloNimbus provides a comprehensive solution for telemetry needs, particularly well-suited for environmental and IoT data applications. Its modular architecture, use of modern technologies, and emphasis on time-series data processing make it a robust framework for data collection, analysis, and visualization.
