CREATE TABLE default.intro_users (
  `id` UInt32,
  `email` String,
  `created_at` DateTime64(3)
)
ENGINE = MergeTree
ORDER BY id;

CREATE VIEW default.intro_users_all AS
SELECT *
FROM default.intro_users;

CREATE VIEW default.intro_users_daily AS
SELECT
  created_at,
  count() AS total_users
FROM default.intro_users
GROUP BY created_at;
