CREATE TABLE analytics.events (
  tenant_id UInt32,
  user_id UInt64,
  event_type LowCardinality(String),
  created_at DateTime,
  duration_ms UInt32 DEFAULT 0
) ENGINE = MergeTree ORDER BY (tenant_id, created_at, event_type);

INSERT INTO analytics.events VALUES
  (1, 1001, 'signup', '2026-08-01 10:00:00', 100),
  (1, 1002, 'signup', '2026-08-02 10:00:00', 200),
  (1, 1001, 'purchase', '2026-08-03 10:00:00', 300),
  (1, 1001, 'purchase', '2026-08-04 10:00:00', 400),
  (2, 2001, 'signup', '2026-08-01 10:00:00', 999);
