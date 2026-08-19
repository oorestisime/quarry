SELECT
  e.event_type,
  count() AS event_count
FROM event_logs AS e
GROUP BY e.event_type WITH TOTALS
ORDER BY e.event_type ASC
