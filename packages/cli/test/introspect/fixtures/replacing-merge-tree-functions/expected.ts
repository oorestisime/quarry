import {
  Date as CHDate,
  DateTime64,
  defineSchema,
  String as CHString,
  table,
  type SchemaBuilder,
  UInt32,
  UInt64,
  view,
} from "@oorestisime/quarry";

const tables = {
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
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  event_version_labels: view.as(
    db
      .selectFrom("event_versions")
      .selectExpr((eb) => [
        "id",
        eb.fn.toString("id").as("id_text"),
        eb.fn.lower("status").as("status_lower"),
        eb.fn.formatDateTime("created_at", "%Y-%m-%d").as("created_day"),
      ]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
