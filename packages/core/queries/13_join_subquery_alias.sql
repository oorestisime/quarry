SELECT
  u.email,
  signups.event_type
FROM users AS u
INNER JOIN (
  SELECT
    e.user_id,
    e.event_type
  FROM event_logs AS e
  WHERE e.event_type = {p0:String}
) AS signups ON u.id = signups.user_id
ORDER BY u.id ASC
LIMIT 10
