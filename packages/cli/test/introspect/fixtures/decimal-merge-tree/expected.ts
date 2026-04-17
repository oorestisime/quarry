import { Date as CHDate, Decimal, defineSchema, table, UInt64, view } from "@oorestisime/quarry";

export const schema = defineSchema({
  partner_event: table.mergeTree(
    {
      event_date: CHDate(),
      partner_id: UInt64(),
      dynamic_amount: Decimal(12, 6),
    },
    {
      orderBy: ["partner_id", "event_date"],
      partitionBy: ["event_date"],
    },
  ),
}).views((db) => ({
  partner_event_months: view.as(
    db
      .selectFrom("partner_event")
      .selectExpr((eb) => ["partner_id", eb.fn.toYYYYMM("event_date").as("event_yyyymm")]),
  ),
}));
