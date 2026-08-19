SELECT
  e.user_id,
  e.event_type
FROM event_logs AS e
ORDER BY e.user_id ASC, e.created_at DESC
LIMIT 1 BY e.user_id
LIMIT 10
