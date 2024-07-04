-- Timescale DB setup migration

-- CREATE EXTENSION system_stats;

-- Enum with the possible types of data series
CREATE TYPE DataSeriesType AS ENUM ('numeric', 'text', 'boolean', 'arbitrary', 'jsonb');

-- Create the table to store the data series, externally identified by a UUID and
-- internally identified by a serial id to save space in the indexes
CREATE TABLE DataSeries (
    -- Serial Id
    id SERIAL PRIMARY KEY,
    -- external UUID of the data series
    external_id UUID UNIQUE NOT NULL,
    -- Optional name of the data series
    name TEXT,
    -- Optional description of the data series
    description TEXT,
    -- Type of the data series
    type DataSeriesType NOT NULL,
    -- Created and updated at timestamp
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ
);

-- Table to store the data points of the data series
CREATE TABLE DataPointNumeric (
    -- Reference to the data series
    dataseries_id INT REFERENCES DataSeries(id),
    -- Timestamp of the data point
    timestamp TIMESTAMPTZ(5) NOT NULL,
    -- Value of the data point
    value DOUBLE PRECISION NOT NULL,
    -- Unique constraint to avoid duplicates
    UNIQUE (dataseries_id, timestamp)
);

-- Create the hypertable
SELECT create_hypertable('DataPointNumeric', by_range('timestamp'));
-- Create the index to speed up the queries
CREATE INDEX idx_datapoint_numeric_dataseries_id_timestamp ON DataPointNumeric (dataseries_id, timestamp);

-- Table to store the data points of the data series
CREATE TABLE DataPointText (
    -- Reference to the data series
    dataseries_id INT REFERENCES DataSeries(id),
    -- Timestamp of the data point
    timestamp TIMESTAMPTZ(5) NOT NULL,
    -- Value of the data point
    value TEXT NOT NULL,
    -- Unique constraint to avoid duplicates
    UNIQUE (dataseries_id, timestamp)
);

-- Create the hypertable
SELECT create_hypertable('DataPointText', by_range('timestamp'));
-- Create the index to speed up the queries
CREATE INDEX idx_datapoint_text_dataseries_id_timestamp ON DataPointText (dataseries_id, timestamp);

-- Table to store the data points of the data series
CREATE TABLE DataPointBoolean (
    -- Reference to the data series
    dataseries_id INT REFERENCES DataSeries(id),
    -- Timestamp of the data point
    timestamp TIMESTAMPTZ(5) NOT NULL,
    -- Value of the data point
    value BOOLEAN NOT NULL,
    -- Unique constraint to avoid duplicates
    UNIQUE (dataseries_id, timestamp)
);

-- Create the hypertable
SELECT create_hypertable('DataPointBoolean', by_range('timestamp'));
-- Create the index to speed up the queries
CREATE INDEX idx_datapoint_boolean_dataseries_id_timestamp ON DataPointBoolean (dataseries_id, timestamp);


-- Table to store the data points of the data series
CREATE TABLE DataPointArbitrary (
    -- Reference to the data series
    dataseries_id INT REFERENCES DataSeries(id),
    -- Timestamp of the data point
    timestamp TIMESTAMPTZ(5) NOT NULL,
    -- Value of the data point, BLOB
    value BYTEA NOT NULL,
    -- Unique constraint to avoid duplicates
    UNIQUE (dataseries_id, timestamp)
);

-- Create the hypertable
SELECT create_hypertable('DataPointArbitrary', by_range('timestamp'));
-- Create the index to speed up the queries
CREATE INDEX idx_datapoint_arbitrary_dataseries_id_timestamp ON DataPointArbitrary (dataseries_id, timestamp);

-- Table to store the data points of the data series
CREATE TABLE DataPointJsonb (
    -- Reference to the data series
    dataseries_id INT REFERENCES DataSeries(id),
    -- Timestamp of the data point
    timestamp TIMESTAMPTZ(5) NOT NULL,
    -- Value of the data point, JSONB
    value JSONB NOT NULL,
    -- Unique constraint to avoid duplicates
    UNIQUE (dataseries_id, timestamp)
);

-- Create the hypertable
SELECT create_hypertable('DataPointJsonb', by_range('timestamp'));
-- Create the index to speed up the queries
CREATE INDEX idx_datapoint_jsonb_dataseries_id_timestamp ON DataPointJsonb (dataseries_id, timestamp);
