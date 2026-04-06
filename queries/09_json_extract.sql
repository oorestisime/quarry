SELECT
  e.user_id,
  JSONExtractString(e.properties, 'source') AS source
FROM event_logs AS e
WHERE JSONExtractString(e.properties, 'source') = {p0:String}
ORDER BY e.created_at DESC
LIMIT 50
