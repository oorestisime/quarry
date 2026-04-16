CREATE TABLE default.user_session_events (
  `user_id` UInt64,
  `event_timestamp` DateTime,
  `city` LowCardinality(String),
  `user_agent` String CODEC(ZSTD(1)),
  `ip` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (user_id, event_timestamp, ip)
TTL event_timestamp + toIntervalYear(2)
SETTINGS index_granularity = 8192;

CREATE VIEW default.user_session_event_rollup AS
SELECT
  user_id,
  toYYYYMM(event_timestamp) AS event_yyyymm,
  lower(city) AS city_lower
FROM default.user_session_events;
