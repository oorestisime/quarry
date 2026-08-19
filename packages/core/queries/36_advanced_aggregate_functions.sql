SELECT
  argMin(t.label, t.id) AS first_label,
  argMax(t.label, t.id) AS last_label,
  quantile(0.95)(t.amount) AS amount_p95
FROM typed_samples AS t
