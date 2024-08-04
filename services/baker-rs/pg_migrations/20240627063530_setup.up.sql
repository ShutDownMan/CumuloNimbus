-- PostgreSQL setup migration

-- CREATE EXTENSION system_stats;

CREATE ENUM TemporalStrategy AS ENUM ('mirror', 'fixedInterval');

-- Recipe table for DataSeries computation
CREATE TABLE Recipe (
    id SERIAL PRIMARY KEY,
    external_id UUID NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,

    simplified_expression TEXT,
    wasm_expression TEXT NOT NULL,

    temporal_strategy TemporalStrategy NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (external_id)
);

CREATE INDEX Recipe_external_id ON Recipe (external_id);

CREATE TABLE RecipeTemporalStrategyMirror (
    recipe_id INT NOT NULL,
    mirrored_data_series_external_id UUID NOT NULL,

    FOREIGN KEY (recipe_id) REFERENCES Recipe(id),
    FOREIGN KEY (mirrored_data_series_external_id) REFERENCES Recipe(external_id)
);

CREATE ENUM Interval AS ENUM ('ISO8601', 'cron');

CREATE TABLE RecipeTemporalStrategyFixedInterval (
    recipe_id INT NOT NULL,
    interval Interval NOT NULL,
    iso8601_interval TEXT,
    cron TEXT,

    FOREIGN KEY (recipe_id) REFERENCES Recipe(id)
);

CREATE TABLE RecipeDataSeriesDependency (
    recipe_id INT NOT NULL,
    data_series_external_id UUID NOT NULL,

    alias TEXT NOT NULL,

    FOREIGN KEY (recipe_id) REFERENCES Recipe(id),
    FOREIGN KEY (data_series_external_id) REFERENCES DataSeries(external_id)
);
