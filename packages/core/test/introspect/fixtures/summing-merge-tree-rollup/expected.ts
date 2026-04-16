import {
  Date as CHDate,
  defineSchema,
  Float64,
  table,
  UInt32,
  UInt64,
  view,
} from "@oorestisime/quarry";

export const schema = defineSchema({
  daily_metrics: table.summingMergeTree(
    {
      bucket_date: CHDate(),
      account_id: UInt64(),
      clicks: UInt32(),
      revenue: Float64(),
    },
    {
      orderBy: ["account_id", "bucket_date"],
      partitionBy: ["bucket_date"],
      settings: {
        index_granularity: 4096,
      },
      sumColumns: ["clicks"],
    },
  ),
}).views((db) => ({
  daily_metric_months: view.as(
    db
      .selectFrom("daily_metrics")
      .selectExpr((eb) => ["account_id", eb.fn.toYYYYMM("bucket_date").as("bucket_yyyymm")]),
  ),
}));
