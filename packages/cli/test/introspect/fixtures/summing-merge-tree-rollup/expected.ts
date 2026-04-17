import {
  Date as CHDate,
  defineSchema,
  Float64,
  table,
  type SchemaBuilder,
  UInt32,
  UInt64,
  view,
} from "@oorestisime/quarry";

const tables = {
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
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  daily_metric_months: view.as(
    db
      .selectFrom("daily_metrics")
      .selectExpr((eb) => ["account_id", eb.fn.toYYYYMM("bucket_date").as("bucket_yyyymm")]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
