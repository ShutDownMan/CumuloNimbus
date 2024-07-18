-- Down Migration

DROP TABLE RecipeTemporalStrategyFixedInterval;
DROP TYPE Interval;
DROP TABLE RecipeTemporalStrategyMirror;

DROP INDEX Recipe_external_id;
DROP TABLE Recipe;
DROP TYPE TemporalStrategy;

DROP TABLE RecipeDataSeriesDependency;