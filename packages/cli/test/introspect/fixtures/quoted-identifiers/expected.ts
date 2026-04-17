import {
  Date as CHDate,
  defineSchema,
  table,
  type SchemaBuilder,
  UInt64,
  view,
} from "@oorestisime/quarry";

const tables = {
  "foo-bar": table.memory({
    "event-date": CHDate(),
    user_id: UInt64(),
  }),
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  "view-with-dash": view.as(db.selectFrom("foo-bar").selectAll()),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
