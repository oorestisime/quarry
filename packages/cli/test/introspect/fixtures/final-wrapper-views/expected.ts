import {
  defineSchema,
  Int32,
  Nullable,
  table,
  type SchemaBuilder,
  UInt8,
  view,
} from "@oorestisime/quarry";

const tables = {
  public_users: table.sharedReplacingMergeTree(
    {
      id: Int32(),
      _peerdb_is_deleted: UInt8(),
      student_id: Nullable(Int32()),
    },
    {
      orderBy: ["id"],
      versionBy: "id",
    },
  ),
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  final_users: view.as(
    db.selectFrom(db.table("public_users").final()).selectAll().where("_peerdb_is_deleted", "=", 0),
  ),
  final_user_students: view.as(
    db.selectFrom(db.table("public_users").final()).selectAll().whereNotNull("student_id"),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
