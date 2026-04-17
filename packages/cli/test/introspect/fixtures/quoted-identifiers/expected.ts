import { Date as CHDate, defineSchema, table, UInt64, view } from "@oorestisime/quarry";

export const schema = defineSchema({
  "foo-bar": table.memory({
    "event-date": CHDate(),
    user_id: UInt64(),
  }),
}).views((db) => ({
  "view-with-dash": view.as(db.selectFrom("foo-bar").selectAll()),
}));
