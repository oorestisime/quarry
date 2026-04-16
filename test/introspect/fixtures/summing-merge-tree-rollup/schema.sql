CREATE TABLE default.daily_metrics (
  `bucket_date` Date,
  `account_id` UInt64,
  `clicks` UInt32,
  `revenue` Float64
)
ENGINE = SummingMergeTree(clicks)
PARTITION BY bucket_date
ORDER BY (account_id, bucket_date)
SETTINGS index_granularity = 4096;

CREATE VIEW default.daily_metric_months AS
SELECT
  account_id,
  toYYYYMM(bucket_date) AS bucket_yyyymm
FROM default.daily_metrics;
