SELECT * FROM event_logs AS e WHERE e.event_type = {p0:String} ORDER BY e.created_at DESC LIMIT 2
