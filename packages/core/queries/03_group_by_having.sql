SELECT
  e.user_id,
  count() AS event_count
FROM event_logs AS e
WHERE e.event_type IN {p0:Array(String)}
GROUP BY e.user_id
HAVING count() > {p1:Int64}
ORDER BY event_count DESC
LIMIT 25
