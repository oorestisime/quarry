SELECT
  t.id,
  t.tags AS tags
FROM typed_samples AS t
LEFT ARRAY JOIN t.tags
ORDER BY t.id ASC, t.tags ASC
