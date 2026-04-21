SELECT
  t.id,
  t.label
FROM typed_samples AS t
WHERE t.status = {p0:String}
  AND has(t.tags, {p1:String})
ORDER BY t.id ASC
