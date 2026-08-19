SELECT
  t.id,
  t.tags AS tags,
  t.scores AS scores
FROM typed_samples AS t
ARRAY JOIN t.tags
ARRAY JOIN t.scores
ORDER BY t.tags ASC, t.scores ASC
