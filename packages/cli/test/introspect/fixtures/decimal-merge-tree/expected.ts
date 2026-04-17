import {
  Date as CHDate,
  Decimal,
  defineSchema,
  table,
  type SchemaBuilder,
  UInt64,
  view,
} from "@oorestisime/quarry";

const tables = {
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
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  partner_event_months: view.as(
    db
      .selectFrom("partner_event")
      .selectExpr((eb) => ["partner_id", eb.fn.toYYYYMM("event_date").as("event_yyyymm")]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
