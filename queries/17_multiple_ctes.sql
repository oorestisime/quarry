WITH active_users AS (
  SELECT e.user_id
  FROM event_logs AS e
  WHERE e.event_type = {p0:String}
  GROUP BY e.user_id
), active_user_emails AS (
  SELECT u.id, u.email
  FROM active_users AS au
  INNER JOIN users AS u ON u.id = au.user_id
)
SELECT
  aue.id,
  aue.email
FROM active_user_emails AS aue
ORDER BY aue.id ASC
LIMIT 100
