CREATE TABLE default.revenue_rollups (
  `event_date` Date,
  `user_id` UInt64,
  `amount_cents` Int64
)
ENGINE = SharedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', amount_cents)
PARTITION BY event_date
ORDER BY (user_id, event_date);

CREATE VIEW default.revenue_rollup_months AS
SELECT
  user_id,
  toYYYYMM(event_date) AS event_yyyymm
FROM default.revenue_rollups;
