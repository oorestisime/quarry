import {
  Array as CHArray,
  Bool,
  Date as CHDate,
  Date32,
  defineSchema,
  FixedString,
  Float32,
  Float64,
  Int16,
  Int32,
  Int64,
  IPv4,
  IPv6,
  LowCardinality,
  Nullable,
  String as CHString,
  table,
  type SchemaBuilder,
  UInt16,
  UUID,
  view,
} from "@oorestisime/quarry";

const tables = {
  network_profiles: table.memory({
    profile_id: UUID(),
    has_opt_in: Bool(),
    signup_date: CHDate(),
    archive_date: Date32(),
    nickname: Nullable(CHString()).codec(["ZSTD(3)"]),
    ip_v4: IPv4(),
    ip_v6: IPv6(),
    small_u16: UInt16(),
    score_f32: Float32(),
    score_f64: Float64(),
    signed_i16: Int16(),
    signed_i32: Int32(),
    signed_i64: Int64(),
    postal_code: FixedString(2),
    tags: CHArray(LowCardinality(CHString())),
    retry_codes: CHArray(Nullable(UInt16())),
  }),
};

const baseSchema: SchemaBuilder<typeof tables> = defineSchema(tables);

const defineViews = (
  db: Parameters<typeof baseSchema.views>[0] extends (db: infer DB) => unknown ? DB : never,
) => ({
  network_profile_labels: view.as(
    db
      .selectFrom("network_profiles")
      .selectExpr((eb) => [
        "profile_id",
        eb.fn.toString("small_u16").as("small_u16_text"),
        eb.fn.lower("nickname").as("nickname_lower"),
      ]),
  ),
});

export const schema: SchemaBuilder<typeof tables & ReturnType<typeof defineViews>> =
  baseSchema.views(defineViews);
