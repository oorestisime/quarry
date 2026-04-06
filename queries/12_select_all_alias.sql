SELECT u.* FROM users AS u INNER JOIN event_logs AS e ON u.id = e.user_id WHERE e.event_type = {p0:String} ORDER BY e.created_at DESC LIMIT 20
