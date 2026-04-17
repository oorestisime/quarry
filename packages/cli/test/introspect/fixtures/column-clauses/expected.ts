import {
  Date as CHDate,
  DateTime64,
  defineSchema,
  String as CHString,
  table,
  type SchemaBuilder,
  UInt32,
} from "@oorestisime/quarry";

const tables = {
  user_notifications: table.mergeTree(
    {
      id: UInt32(),
      created_at: DateTime64(3).defaultSql("now64(3)"),
      event_date: CHDate().materializedSql("toDate(created_at)"),
      event_label: CHString().aliasSql("toString(event_date)"),
    },
    {
      orderBy: ["id"],
    },
  ),
};

export const schema: SchemaBuilder<typeof tables> = defineSchema(tables);
