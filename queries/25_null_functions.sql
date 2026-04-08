SELECT
  t.id,
  isNull(t.nickname) AS nickname_is_null,
  isNotNull(t.nickname) AS nickname_is_not_null,
  nullIf(t.label, {p0:String}) AS maybe_label,
  coalesce(t.nickname, t.label) AS display_name,
  coalesce(t.nickname, {p1:String}) AS display_name_with_literal,
  ifNull(t.nickname, {p2:String}) AS nickname_or_default
FROM typed_samples AS t
ORDER BY t.id ASC
