SELECT
  t.id,
  toInt32(t.id) AS id_i32,
  toInt64(t.id) AS id_i64,
  toUInt32(t.id) AS id_u32,
  toUInt64(t.big_user_id) AS big_user_id_u64,
  toFloat32(t.id) AS id_f32,
  toFloat64(t.amount) AS amount_f64,
  toDate(t.created_at) AS created_date,
  toDateTime(t.created_at) AS created_at_dt,
  toDateTime64(t.created_at, 3) AS created_at_dt64,
  toString(t.id) AS id_text,
  toDecimal64(t.amount, 2) AS amount_d64,
  toDecimal128(t.amount, 2) AS amount_d128
FROM typed_samples AS t
WHERE toUInt32(t.id) > {p0:Int64}
ORDER BY t.id ASC
