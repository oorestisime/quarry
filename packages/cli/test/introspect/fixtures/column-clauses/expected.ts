import {
  Date as CHDate,
  DateTime64,
  defineSchema,
  String as CHString,
  table,
  UInt32,
} from "@oorestisime/quarry";

export const schema = defineSchema({
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
});
