import {
  Date as CHDate,
  defineSchema,
  Int64,
  table,
  type SchemaBuilder,
  UInt64,
  view,
} from "@oorestisime/quarry";

const tables = {
  revenue_rollups: table.sharedSummingMergeTree(
    {
      event_date: CHDate(),
      user_id: UInt64(),
      amount_cents: Int64(),
    },
    {
      orderBy: ["user_id", "event_date"],
      partitionBy: ["event_date"],
      sumColumns: ["amount_cents"],
    },
  ),
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  revenue_rollup_months: view.as(
    db
      .selectFrom("revenue_rollups")
      .selectExpr((eb) => ["user_id", eb.fn.toYYYYMM("event_date").as("event_yyyymm")]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
