SELECT
  e.user_id,
  count() AS event_count
FROM event_logs AS e
GROUP BY e.user_id
HAVING count() > (
  SELECT count() AS threshold_count
  FROM users AS u
  WHERE u.id = {p0:Int64}
)
ORDER BY e.user_id ASC
