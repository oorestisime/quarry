CREATE TABLE default.event_versions (
  `id` UInt32,
  `event_date` Date,
  `created_at` DateTime64(3),
  `status` String,
  `version` UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY event_date
ORDER BY (id, created_at)
TTL created_at + toIntervalDay(30)
SETTINGS index_granularity = 8192;

CREATE VIEW default.event_version_labels AS
SELECT
  id,
  toString(id) AS id_text,
  lower(status) AS status_lower,
  formatDateTime(created_at, '%Y-%m-%d') AS created_day
FROM default.event_versions;
