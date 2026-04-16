SELECT
  e.user_id,
  e.created_at
FROM event_logs AS e FINAL
PREWHERE e.event_date >= {p0:Date}
WHERE e.event_type = {p1:String}
ORDER BY e.created_at DESC
LIMIT 100
SETTINGS max_threads = 8
