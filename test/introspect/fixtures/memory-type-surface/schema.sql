CREATE TABLE default.network_profiles (
  `profile_id` UUID,
  `has_opt_in` Bool,
  `signup_date` Date,
  `archive_date` Date32,
  `nickname` Nullable(String) CODEC(ZSTD(3)),
  `ip_v4` IPv4,
  `ip_v6` IPv6,
  `small_u16` UInt16,
  `score_f32` Float32,
  `score_f64` Float64,
  `signed_i16` Int16,
  `signed_i32` Int32,
  `signed_i64` Int64,
  `postal_code` FixedString(2),
  `tags` Array(LowCardinality(String)),
  `retry_codes` Array(Nullable(UInt16))
)
ENGINE = Memory;

CREATE VIEW default.network_profile_labels AS
SELECT
  profile_id,
  toString(small_u16) AS small_u16_text,
  lower(nickname) AS nickname_lower
FROM default.network_profiles;
