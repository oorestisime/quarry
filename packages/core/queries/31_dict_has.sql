SELECT
  u.id,
  dictHas('partner_rates', u.id) AS has_rate
FROM users AS u
WHERE u.id IN {p0:Array(Int64)}
ORDER BY u.id ASC
