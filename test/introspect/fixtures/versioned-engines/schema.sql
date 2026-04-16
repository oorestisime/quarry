CREATE TABLE default.leaderboard_user_dim (
  `user_id` UInt64,
  `first_name` String,
  `refreshed_at` DateTime64(3),
  `is_deleted` UInt8
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', refreshed_at, is_deleted)
PRIMARY KEY user_id
ORDER BY user_id
SETTINGS index_granularity = 8192;

CREATE TABLE default.activity_deltas (
  `id` UInt32,
  `sign` Int8,
  `version` DateTime64(3)
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY id;

CREATE VIEW default.active_leaderboard_users AS
SELECT
  user_id,
  lower(first_name) AS first_name_lower
FROM default.leaderboard_user_dim;
