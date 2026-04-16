import {
  DateTime64,
  defineSchema,
  String as CHString,
  table,
  UInt32,
  view,
} from "@oorestisime/quarry";

export const schema = defineSchema({
  intro_users: table.mergeTree(
    {
      id: UInt32(),
      email: CHString(),
      created_at: DateTime64(3),
    },
    {
      orderBy: ["id"],
    },
  ),
}).views((db) => ({
  intro_users_all: view.as(db.selectFrom("intro_users").selectAll()),
  intro_users_daily: view.as(
    db
      .selectFrom("intro_users")
      .selectExpr((eb) => ["created_at", eb.fn.count().as("total_users")])
      .groupBy("created_at"),
  ),
}));
