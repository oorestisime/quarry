import {
  Date as CHDate,
  defineSchema,
  LowCardinality,
  String as CHString,
  table,
  type SchemaBuilder,
  UInt64,
  view,
} from "@oorestisime/quarry";

const tables = {
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
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  aggregate_state_labels: view.as(
    db
      .selectFrom("aggregate_state_samples")
      .selectExpr((eb) => ["account_id", eb.fn.lower("segment").as("segment_lower")]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
