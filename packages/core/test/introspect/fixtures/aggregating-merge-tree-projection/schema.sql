CREATE TABLE default.aggregate_state_samples (
  `bucket_date` Date,
  `account_id` UInt64,
  `segment` LowCardinality(String)
)
ENGINE = AggregatingMergeTree
PARTITION BY bucket_date
ORDER BY (account_id, bucket_date);

CREATE VIEW default.aggregate_state_labels AS
SELECT
  account_id,
  lower(segment) AS segment_lower
FROM default.aggregate_state_samples;
