SELECT
  t.id,
  toStartOfMonth(t.created_at) AS month_start,
  toStartOfYear(t.created_at) AS year_start,
  formatDateTime(t.created_at, '%Y-%m-%d') AS created_date_text,
  dateDiff('day', toDate(t.created_at), {p0:Date}) AS days_until_cutoff,
  addDays(t.created_at, {p1:Int64}) AS plus_five_days,
  subtractHours(t.created_at, {p2:Int64}) AS minus_two_hours,
  toYYYYMM(t.created_at) AS created_yyyymm,
  toYYYYMMDD(t.created_at) AS created_yyyymmdd
FROM typed_samples AS t
ORDER BY t.id ASC
