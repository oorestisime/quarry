SELECT
  u.id,
  u.email,
  e.event_type
FROM users AS u
INNER JOIN event_logs AS e ON u.id = e.user_id
WHERE u.id = e.user_id
ORDER BY u.id ASC
LIMIT 20
