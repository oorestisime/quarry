SELECT
  u.id,
  u.email,
  e.event_type
FROM users AS u
INNER JOIN event_logs AS e FINAL ON u.id = e.user_id
WHERE e.event_type = {p0:String}
ORDER BY e.created_at DESC
LIMIT 20
