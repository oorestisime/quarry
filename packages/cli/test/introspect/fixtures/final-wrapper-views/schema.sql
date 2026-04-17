CREATE TABLE default.public_users (
  `id` Int32,
  `_peerdb_is_deleted` UInt8,
  `student_id` Nullable(Int32)
)
ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', id)
ORDER BY id;

CREATE VIEW default.final_users (`id` Int32, `_peerdb_is_deleted` UInt8, `student_id` Nullable(Int32)) AS
SELECT *
FROM default.public_users FINAL
WHERE _peerdb_is_deleted = 0;

CREATE VIEW default.final_user_students (`id` Int32, `_peerdb_is_deleted` UInt8, `student_id` Nullable(Int32)) AS
SELECT *
FROM default.public_users FINAL
WHERE student_id IS NOT NULL;
