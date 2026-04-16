import { Date as CHDate, defineSchema, Int64, table, UInt64, view } from "@oorestisime/quarry";

export const schema = defineSchema({
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
}).views((db) => ({
  revenue_rollup_months: view.as(
    db
      .selectFrom("revenue_rollups")
      .selectExpr((eb) => ["user_id", eb.fn.toYYYYMM("event_date").as("event_yyyymm")]),
  ),
}));
