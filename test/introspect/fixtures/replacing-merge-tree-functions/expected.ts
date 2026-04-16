import {
  Date as CHDate,
  DateTime64,
  defineSchema,
  String as CHString,
  table,
  UInt32,
  UInt64,
  view,
} from "@oorestisime/quarry";

export const schema = defineSchema({
  event_versions: table.replacingMergeTree(
    {
      id: UInt32(),
      event_date: CHDate(),
      created_at: DateTime64(3),
      status: CHString(),
      version: UInt64(),
    },
    {
      orderBy: ["id", "created_at"],
      partitionBy: ["event_date"],
      ttl: ["created_at + toIntervalDay(30)"],
      settings: {
        index_granularity: 8192,
      },
      versionBy: "version",
    },
  ),
}).views((db) => ({
  event_version_labels: view.as(
    db
      .selectFrom("event_versions as t0")
      .selectExpr((eb) => [
        "t0.id",
        eb.fn.toString("t0.id").as("id_text"),
        eb.fn.lower("t0.status").as("status_lower"),
        eb.fn.formatDateTime("t0.created_at", "%Y-%m-%d").as("created_day"),
      ]),
  ),
}));
