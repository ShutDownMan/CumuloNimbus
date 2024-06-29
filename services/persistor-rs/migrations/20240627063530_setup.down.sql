-- Down Migration

-- Drop hypertable for DataPointJsonb
SELECT drop_chunks('DataPointJsonb');
DROP TABLE IF EXISTS DataPointJsonb;

-- Drop hypertable for DataPointArbitrary
SELECT drop_chunks('DataPointArbitrary');
DROP TABLE IF EXISTS DataPointArbitrary;

-- Drop hypertable for DataPointBoolean
SELECT drop_chunks('DataPointBoolean');
DROP TABLE IF EXISTS DataPointBoolean;

-- Drop hypertable for DataPointText
SELECT drop_chunks('DataPointText');
DROP TABLE IF EXISTS DataPointText;

-- Drop hypertable for DataPointNumeric
SELECT drop_chunks('DataPointNumeric');
DROP TABLE IF EXISTS DataPointNumeric;

-- Drop the enum type for data series
DROP TYPE IF EXISTS DataSeriesType;

-- Drop the table storing data series
DROP TABLE IF EXISTS DataSeries;
