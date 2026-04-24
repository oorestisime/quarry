SELECT
  u.id,
  dictGetOrDefault('partner_rates', 'currency', u.id, {p0:String}) AS currency
FROM users AS u
WHERE u.id IN {p1:Array(Int64)}
ORDER BY u.id ASC
