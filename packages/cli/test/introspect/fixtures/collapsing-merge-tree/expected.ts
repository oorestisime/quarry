import {
  DateTime,
  defineSchema,
  FixedString,
  Int8,
  table,
  type SchemaBuilder,
  UInt32,
  view,
} from "@oorestisime/quarry";

const tables = {
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
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  activity_log_labels: view.as(
    db.selectFrom("activity_log").selectExpr((eb) => ["id", eb.fn.toString("id").as("id_text")]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
