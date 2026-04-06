SELECT
  u.id,
  u.email
FROM users AS u
WHERE u.id IN (
  SELECT e.user_id
  FROM event_logs AS e
  WHERE e.event_type = {p0:String}
)
ORDER BY u.id ASC
