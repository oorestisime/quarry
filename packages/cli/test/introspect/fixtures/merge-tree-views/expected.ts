import {
  DateTime64,
  defineSchema,
  String as CHString,
  table,
  type SchemaBuilder,
  UInt32,
  view,
} from "@oorestisime/quarry";

const tables = {
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
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  intro_users_all: view.as(db.selectFrom("intro_users").selectAll()),
  intro_users_daily: view.as(
    db
      .selectFrom("intro_users")
      .selectExpr((eb) => ["created_at", eb.fn.count().as("total_users")])
      .groupBy("created_at"),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
