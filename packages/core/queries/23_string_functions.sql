SELECT
  t.id,
  like(t.label, {p0:String}) AS has_ph,
  ilike(t.label, {p1:String}) AS has_al_insensitive,
  empty(t.label) AS label_is_empty,
  notEmpty(t.label) AS label_is_not_empty,
  concat(t.label, {p2:String}, toString(t.id)) AS label_key,
  lower(t.label) AS label_lower,
  upper(t.label) AS label_upper,
  substring(t.label, {p3:Int64}, {p4:Int64}) AS label_slice,
  trimBoth(concat({p5:String}, t.label, {p6:String})) AS label_trimmed,
  trimLeft(concat({p7:String}, t.label, {p8:String})) AS label_left_trimmed,
  trimRight(concat({p9:String}, t.label, {p10:String})) AS label_right_trimmed
FROM typed_samples AS t
WHERE notEmpty(t.label)
ORDER BY t.id ASC
