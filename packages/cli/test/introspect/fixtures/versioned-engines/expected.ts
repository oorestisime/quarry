import {
  DateTime64,
  defineSchema,
  Int8,
  String as CHString,
  table,
  UInt32,
  UInt64,
  UInt8,
  view,
} from "@oorestisime/quarry";

export const schema = defineSchema({
  leaderboard_user_dim: table.sharedReplacingMergeTree(
    {
      user_id: UInt64(),
      first_name: CHString(),
      refreshed_at: DateTime64(3),
      is_deleted: UInt8(),
    },
    {
      primaryKey: ["user_id"],
      orderBy: ["user_id"],
      settings: {
        index_granularity: 8192,
      },
      versionBy: "refreshed_at",
      isDeletedBy: "is_deleted",
    },
  ),
  activity_deltas: table.versionedCollapsingMergeTree(
    {
      id: UInt32(),
      sign: Int8(),
      version: DateTime64(3),
    },
    {
      orderBy: ["id"],
      signBy: "sign",
      versionBy: "version",
    },
  ),
}).views((db) => ({
  active_leaderboard_users: view.as(
    db
      .selectFrom("leaderboard_user_dim")
      .selectExpr((eb) => ["user_id", eb.fn.lower("first_name").as("first_name_lower")]),
  ),
}));
