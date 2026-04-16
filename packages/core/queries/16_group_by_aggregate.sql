SELECT
  e.user_id,
  count() AS event_count
FROM event_logs AS e
WHERE e.event_type IN {p0:Array(String)}
GROUP BY e.user_id
ORDER BY event_count DESC, e.user_id ASC
LIMIT 25
