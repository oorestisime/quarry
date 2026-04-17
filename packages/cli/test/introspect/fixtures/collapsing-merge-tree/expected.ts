import {
  DateTime,
  defineSchema,
  FixedString,
  Int8,
  table,
  UInt32,
  view,
} from "@oorestisime/quarry";

export const schema = defineSchema({
  activity_log: table.collapsingMergeTree(
    {
      id: UInt32(),
      sign: Int8(),
      category: FixedString(4),
      created_at: DateTime(),
    },
    {
      orderBy: ["id", "created_at"],
      signBy: "sign",
    },
  ),
}).views((db) => ({
  activity_log_labels: view.as(
    db.selectFrom("activity_log").selectExpr((eb) => ["id", eb.fn.toString("id").as("id_text")]),
  ),
}));
