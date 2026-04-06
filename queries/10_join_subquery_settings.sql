SELECT
  u.id,
  u.email,
  downloads.inquiries_count
FROM users AS u
LEFT JOIN (
  SELECT
    d.user_id,
    count() AS inquiries_count
  FROM inquiry_downloads AS d FINAL
  PREWHERE d.created_at >= {p0:DateTime}
  GROUP BY d.user_id
) AS downloads ON downloads.user_id = u.id
WHERE u.status = {p1:String}
ORDER BY downloads.inquiries_count DESC, u.id ASC
LIMIT 20
OFFSET 40
SETTINGS join_algorithm = 'grace_hash'
