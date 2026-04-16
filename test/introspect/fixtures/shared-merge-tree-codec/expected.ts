import {
  DateTime,
  defineSchema,
  LowCardinality,
  String as CHString,
  table,
  UInt64,
  view,
} from "@oorestisime/quarry";

export const schema = defineSchema({
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
}).views((db) => ({
  user_session_event_rollup: view.as(
    db
      .selectFrom("user_session_events as t0")
      .selectExpr((eb) => [
        "t0.user_id",
        eb.fn.toYYYYMM("t0.event_timestamp").as("event_yyyymm"),
        eb.fn.lower("t0.city").as("city_lower"),
      ]),
  ),
}));
