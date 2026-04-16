SELECT
  t.id,
  has(t.tags, {p0:String}) AS has_trial,
  hasAny(t.tags, {p1:Array(String)}) AS has_overlap,
  hasAll(t.tags, {p2:Array(String)}) AS has_required,
  length(t.tags) AS tag_count,
  empty(t.tags) AS is_empty,
  notEmpty(t.tags) AS is_not_empty
FROM typed_samples AS t
ORDER BY t.id ASC
