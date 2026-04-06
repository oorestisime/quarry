WITH active_users AS (
  SELECT e.user_id
  FROM event_logs AS e
  WHERE e.event_type = {p0:String}
  GROUP BY e.user_id
), user_counts AS (
  SELECT
    e.user_id,
    count() AS event_count
  FROM event_logs AS e
  GROUP BY e.user_id
)
SELECT
  u.id,
  u.email,
  uc.event_count
FROM users AS u
LEFT JOIN active_users AS au ON au.user_id = u.id
LEFT JOIN user_counts AS uc ON uc.user_id = u.id
WHERE au.user_id = u.id
ORDER BY u.id ASC
LIMIT 100
