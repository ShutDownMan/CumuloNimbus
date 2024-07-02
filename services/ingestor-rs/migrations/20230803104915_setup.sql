-- sqlite migration for database setup

CREATE TABLE DataSeries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_dataseries_external_id ON DataSeries (external_id);

CREATE TABLE DataPoint (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataseries_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value REAL NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP DEFAULT NULL,

    UNIQUE (dataseries_id, timestamp),
    FOREIGN KEY (dataseries_id) REFERENCES DataSeries(id)
);

CREATE INDEX idx_datapoint_dataseries_id_sent_at ON DataPoint (dataseries_id, sent_at);

-- fetch all: sqlite3 db/ingestor.db "SELECT * FROM DataSeries;"
-- fetch all: sqlite3 db/ingestor.db "SELECT * FROM DataPoint;"
