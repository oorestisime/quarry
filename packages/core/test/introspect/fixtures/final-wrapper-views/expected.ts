import { defineSchema, Int32, Nullable, table, UInt8, view } from "@oorestisime/quarry";

export const schema = defineSchema({
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
}).views((db) => ({
  final_users: view.as(
    db.selectFrom(db.table("public_users").final()).selectAll().where("_peerdb_is_deleted", "=", 0),
  ),
  final_user_students: view.as(
    db.selectFrom(db.table("public_users").final()).selectAll().whereNotNull("student_id"),
  ),
}));
