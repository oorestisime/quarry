import {
  DateTime,
  defineSchema,
  LowCardinality,
  String as CHString,
  table,
  type SchemaBuilder,
  UInt64,
  view,
} from "@oorestisime/quarry";

const tables = {
  user_session_events: table.sharedMergeTree(
    {
      user_id: UInt64(),
      event_timestamp: DateTime(),
      city: LowCardinality(CHString()),
      user_agent: CHString().codec(["ZSTD(1)"]),
      ip: CHString(),
    },
    {
      orderBy: ["user_id", "event_timestamp", "ip"],
      partitionBy: ["toYYYYMM(event_timestamp)"],
      ttl: ["event_timestamp + toIntervalYear(2)"],
      settings: {
        index_granularity: 8192,
      },
    },
  ),
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  user_session_event_rollup: view.as(
    db
      .selectFrom("user_session_events")
      .selectExpr((eb) => [
        "user_id",
        eb.fn.toYYYYMM("event_timestamp").as("event_yyyymm"),
        eb.fn.lower("city").as("city_lower"),
      ]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
