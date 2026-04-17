CREATE TABLE default.user_notifications (
  `id` UInt32,
  `created_at` DateTime64(3) DEFAULT now64(3),
  `event_date` Date MATERIALIZED toDate(created_at),
  `event_label` String ALIAS toString(event_date)
)
ENGINE = MergeTree
ORDER BY id;
