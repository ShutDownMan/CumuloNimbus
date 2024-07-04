-- sqlite migration for database setup

CREATE TABLE DataSeries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT UNIQUE NOT NULL,
    -- milliseconds timestamp
    created_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- milliseconds timestamp
    updated_at INTEGER
) STRICT;

CREATE INDEX idx_dataseries_external_id ON DataSeries (external_id);

CREATE TABLE DataPoint (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataseries_id INTEGER NOT NULL,
    -- nanosecond timestamp
    timestamp INTEGER NOT NULL,
    -- value can be of any type
    value ANY NOT NULL,
    -- milliseconds timestamp
    created_at INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- milliseconds timestamp
    sent_at INTEGER DEFAULT NULL,
    -- milliseconds timestamp
    expiration INTEGER DEFAULT NULL,

    UNIQUE (dataseries_id, timestamp),
    FOREIGN KEY (dataseries_id) REFERENCES DataSeries(id)
) STRICT;

CREATE INDEX idx_datapoint_dataseries_id_sent_at ON DataPoint (dataseries_id, sent_at);

-- fetch all: sqlite3 db/ingestor.db "SELECT * FROM DataSeries;"
-- fetch all: sqlite3 db/ingestor.db "SELECT * FROM DataPoint;"
