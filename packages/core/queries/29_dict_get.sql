SELECT
  u.id,
  dictGet('partner_rates', 'rate_cents', u.id) AS rate_cents
FROM users AS u
WHERE u.id IN {p0:Array(Int64)}
ORDER BY u.id ASC
