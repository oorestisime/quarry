SELECT
  t.id,
  if(t.status = {p0:String}, t.label, {p1:String}) AS status_label,
  least(t.id, {p2:UInt32}) AS least_val,
  greatest(t.id, {p3:UInt32}) AS greatest_val,
  ceil(t.amount) AS ceil_amount,
  floor(t.amount) AS floor_amount,
  toUInt8(t.id) AS id_u8,
  toYear(t.created_at) AS created_year,
  toMonth(t.created_at) AS created_month
FROM typed_samples AS t
WHERE toUInt8(t.id) > {p4:Int64}
ORDER BY t.id ASC
