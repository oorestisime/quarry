SELECT
  a.id,
  a.email
FROM users AS a
INNER JOIN users AS b ON a.id = b.id AND a.email = b.email
WHERE a.status = {p0:String}
ORDER BY a.id ASC
LIMIT 2
