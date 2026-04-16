import {
  Date as CHDate,
  defineSchema,
  LowCardinality,
  String as CHString,
  table,
  UInt64,
  view,
} from "@oorestisime/quarry";

export const schema = defineSchema({
  aggregate_state_samples: table.aggregatingMergeTree(
    {
      bucket_date: CHDate(),
      account_id: UInt64(),
      segment: LowCardinality(CHString()),
    },
    {
      orderBy: ["account_id", "bucket_date"],
      partitionBy: ["bucket_date"],
    },
  ),
}).views((db) => ({
  aggregate_state_labels: view.as(
    db
      .selectFrom("aggregate_state_samples")
      .selectExpr((eb) => ["account_id", eb.fn.lower("segment").as("segment_lower")]),
  ),
}));
