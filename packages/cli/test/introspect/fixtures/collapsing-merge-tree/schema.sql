CREATE TABLE default.activity_log (
  `id` UInt32,
  `sign` Int8,
  `category` FixedString(4),
  `created_at` DateTime
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (id, created_at);

CREATE VIEW default.activity_log_labels AS
SELECT
  id,
  toString(id) AS id_text
FROM default.activity_log;
