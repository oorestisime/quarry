import {
  Array as CHArray,
  Date as CHDate,
  Date32,
  DateTime,
  DateTime64,
  FixedString,
  IPv4,
  IPv6,
  Nullable,
  LowCardinality,
  String as CHString,
  UInt8,
  UInt16,
  UInt32,
  UInt64,
  UUID,
  createClickHouseDB,
  defineSchema,
  param,
  table,
  type ClickHouseClient,
  type ClickHouseInsertResult,
  type InferResult,
  view,
  Int8,
  Int16,
} from "../src";

interface TypecheckDB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
    properties: string;
    event_date: string;
    version: number;
  };
  inquiry_downloads: {
    user_id: number;
    created_at: string;
    version: number;
  };
  users: {
    id: number;
    email: string;
    status: string;
  };
  typed_samples: {
    id: number;
    big_user_id: string;
    label: string;
    status: "pending" | "active" | "archived";
    nickname: string | null;
    tags: string[];
    amount: number;
    created_at: string;
    location: [number, number];
    attributes: Record<string, string>;
    "metrics.name": string[];
    "metrics.score": number[];
  };
  json_samples: {
    id: number;
    payload: {
      user: { id: string };
      traits: { plan: string; active: boolean };
      tags: string[];
      metrics: { score: number; rank: number };
    };
  };
}

const db = createClickHouseDB<TypecheckDB>();

const client: ClickHouseClient = {
  query: async () => ({
    json: async <T>() => [] as T[],
  }),
  insert: async () => ({
    executed: true,
    query_id: "insert-query-id",
  }),
  command: async () => ({
    query_id: "command-query-id",
  }),
};

const basicQuery = db.selectFrom("event_logs as e").select("e.user_id", "e.event_type");
const selectAllQuery = db.selectFrom("event_logs as e").selectAll();
const selectAllAliasQuery = db
  .selectFrom("users as u")
  .innerJoin("event_logs as e", "u.id", "e.user_id")
  .selectAll("u");
const signupsSubquery = db
  .selectFrom("event_logs as e")
  .select("e.user_id", "e.event_type")
  .where("e.event_type", "=", "signup")
  .as("signups");
const finalEventLogsSource = db.table("event_logs").as("e").final();
const withActiveUsers = db.with("active_users", (db) =>
  db
    .selectFrom("event_logs as e")
    .select("e.user_id")
    .where("e.event_type", "=", "signup")
    .groupBy("e.user_id"),
);
const withMultipleCtes = db
  .with("active_users", (db) =>
    db
      .selectFrom("event_logs as e")
      .select("e.user_id")
      .where("e.event_type", "=", "signup")
      .groupBy("e.user_id"),
  )
  .with("active_user_emails", (db) =>
    db
      .selectFrom("active_users as au")
      .innerJoin("users as u", "u.id", "au.user_id")
      .select("u.id", "u.email"),
  );
const selectFromSubqueryQuery = db.selectFrom(signupsSubquery).selectAll("signups");
const selectFromFinalTableSourceQuery = db
  .selectFrom(finalEventLogsSource)
  .select("e.user_id", "e.event_type");
const selectFromCteQuery = withActiveUsers.selectFrom("active_users as au").select("au.user_id");
const selectFromMultipleCtesQuery = withMultipleCtes
  .selectFrom("active_user_emails as aue")
  .select("aue.id", "aue.email");
const groupedQuery = db
  .selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
  .groupBy("e.user_id")
  .orderBy("event_count", "desc");
const downloadsSubquery = db
  .selectFrom(db.table("inquiry_downloads").as("d").final())
  .selectExpr((eb) => ["d.user_id", eb.fn.count().as("inquiries_count")])
  .prewhere("d.created_at", ">=", param("2025-01-01 00:00:00", "DateTime"))
  .groupBy("d.user_id")
  .as("downloads");
const activeUsersSubquery = db
  .selectFrom("event_logs as e")
  .select("e.user_id")
  .where("e.event_type", "=", "signup");
const selectFromJoinSubquerySettingsQuery = db
  .selectFrom("users as u")
  .leftJoin(downloadsSubquery, "downloads.user_id", "u.id")
  .select("u.id", "u.email", "downloads.inquiries_count")
  .where("u.status", "=", "active")
  .orderBy("downloads.inquiries_count", "desc")
  .orderBy("u.id", "asc")
  .limit(20)
  .offset(40)
  .settings({ join_algorithm: "grace_hash" });
const typeCastQuery = db
  .selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    "t.id",
    eb.fn.toInt32("t.id").as("id_i32"),
    eb.fn.toInt64("t.id").as("id_i64"),
    eb.fn.toUInt32("t.id").as("id_u32"),
    eb.fn.toUInt64("t.big_user_id").as("big_user_id_u64"),
    eb.fn.toFloat32("t.id").as("id_f32"),
    eb.fn.toFloat64("t.amount").as("amount_f64"),
    eb.fn.toDate("t.created_at").as("created_date"),
    eb.fn.toDateTime("t.created_at").as("created_at_dt"),
    eb.fn.toDateTime64("t.created_at", 3).as("created_at_dt64"),
    eb.fn.toString("t.id").as("id_text"),
    eb.fn.toDecimal64("t.amount", 2).as("amount_d64"),
    eb.fn.toDecimal128("t.amount", 2).as("amount_d128"),
  ])
  .where((eb) => eb.fn.toUInt32("t.id"), ">", 0)
  .orderBy("t.id", "asc");
const arrayFunctionQuery = db
  .selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    "t.id",
    eb.fn.has("t.tags", "trial").as("has_trial"),
    eb.fn.hasAny("t.tags", ["vip", "trial"]).as("has_overlap"),
    eb.fn.hasAll("t.tags", ["new", "trial"]).as("has_required"),
    eb.fn.length("t.tags").as("tag_count"),
    eb.fn.empty("t.tags").as("is_empty"),
    eb.fn.notEmpty("t.tags").as("is_not_empty"),
  ])
  .where((eb) => eb.fn.notEmpty("t.tags"))
  .orderBy("t.id", "asc");
const stringFunctionQuery = db
  .selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    "t.id",
    eb.fn.like("t.label", "%ph%").as("has_ph"),
    eb.fn.ilike("t.label", "%AL%").as("has_al_insensitive"),
    eb.fn.empty("t.label").as("label_is_empty"),
    eb.fn.notEmpty("t.label").as("label_is_not_empty"),
    eb.fn.concat(eb.ref("t.label"), "-", eb.fn.toString("t.id")).as("label_key"),
    eb.fn.lower("t.label").as("label_lower"),
    eb.fn.upper("t.label").as("label_upper"),
    eb.fn.substring("t.label", 2, 3).as("label_slice"),
    eb.fn.trimBoth(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_trimmed"),
    eb.fn.trimLeft(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_left_trimmed"),
    eb.fn.trimRight(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_right_trimmed"),
  ])
  .where((eb) => eb.fn.notEmpty("t.label"))
  .orderBy("t.id", "asc");
const nullableStringFunctionQuery = db
  .selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    "t.id",
    eb.fn.like("t.nickname", "%e%").as("nickname_has_e"),
    eb.fn
      .ilike("t.label", param<string | null>(null, "Nullable(String)"))
      .as("label_matches_maybe"),
    eb.fn.empty("t.nickname").as("nickname_is_empty"),
    eb.fn.notEmpty("t.nickname").as("nickname_is_not_empty"),
    eb.fn.concat(eb.ref("t.nickname"), "-", eb.ref("t.label")).as("nickname_key"),
    eb.fn.lower("t.nickname").as("nickname_lower"),
    eb.fn.upper("t.nickname").as("nickname_upper"),
    eb.fn.substring("t.nickname", 1, 1).as("nickname_slice"),
    eb.fn.trimBoth(eb.fn.concat("  ", eb.ref("t.nickname"), "  ")).as("nickname_trimmed"),
  ])
  .orderBy("t.id", "asc");
const nullFunctionQuery = db
  .selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    "t.id",
    eb.fn.isNull("t.nickname").as("nickname_is_null"),
    eb.fn.isNotNull("t.nickname").as("nickname_is_not_null"),
    eb.fn.nullIf("t.label", "beta").as("maybe_label"),
    eb.fn.coalesce("t.nickname", eb.ref("t.label")).as("display_name"),
    eb.fn.coalesce("t.nickname", eb.val("Unknown")).as("display_name_with_literal"),
    eb.fn.ifNull("t.nickname", param("Unknown", "String")).as("nickname_or_default"),
  ])
  .orderBy("t.id", "asc");
const aggregateFunctionQuery = db.selectFrom("typed_samples as t").selectExpr((eb) => {
  const isActive = eb.cmp("t.status", "=", "active");

  return [
    eb.fn.count().as("sample_count"),
    eb.fn.countIf(isActive).as("active_samples"),
    eb.fn.sum("t.amount").as("amount_sum"),
    eb.fn.sum("t.big_user_id").as("big_user_id_sum"),
    eb.fn.sumIf("t.amount", isActive).as("active_amount_sum"),
    eb.fn.sumIf("t.big_user_id", isActive).as("active_big_user_id_sum"),
    eb.fn.avg("t.amount").as("amount_avg"),
    eb.fn.avg("t.big_user_id").as("big_user_id_avg"),
    eb.fn.avgIf("t.amount", isActive).as("active_amount_avg"),
    eb.fn.min("t.label").as("min_label"),
    eb.fn.max("t.label").as("max_label"),
    eb.fn.uniq("t.status").as("uniq_statuses"),
    eb.fn.uniqExact("t.status").as("uniq_statuses_exact"),
    eb.fn.uniqIf("t.status", isActive).as("uniq_active_statuses"),
    eb.fn.groupArray("t.label").as("labels"),
    eb.fn.groupArray("t.nickname").as("nicknames"),
    eb.fn.any("t.label").as("any_label"),
    eb.fn.anyLast("t.label").as("any_last_label"),
  ];
});
const nullableGroupArrayQuery = db
  .selectFrom("typed_samples as t")
  .selectExpr((eb) => [eb.fn.groupArray("t.nickname").as("nicknames")]);

type BasicRow = InferResult<typeof basicQuery>;
type SelectAllRow = InferResult<typeof selectAllQuery>;
type SelectAllAliasRow = InferResult<typeof selectAllAliasQuery>;
type SelectFromSubqueryRow = InferResult<typeof selectFromSubqueryQuery>;
type SelectFromFinalTableSourceRow = InferResult<typeof selectFromFinalTableSourceQuery>;
type SelectFromCteRow = InferResult<typeof selectFromCteQuery>;
type SelectFromMultipleCtesRow = InferResult<typeof selectFromMultipleCtesQuery>;
type GroupedRow = InferResult<typeof groupedQuery>;
type SelectFromJoinSubquerySettingsRow = InferResult<typeof selectFromJoinSubquerySettingsQuery>;
type TypeCastRow = InferResult<typeof typeCastQuery>;
type ArrayFunctionRow = InferResult<typeof arrayFunctionQuery>;
type StringFunctionRow = InferResult<typeof stringFunctionQuery>;
type NullableStringFunctionRow = InferResult<typeof nullableStringFunctionQuery>;
type NullFunctionRow = InferResult<typeof nullFunctionQuery>;
type AggregateFunctionRow = InferResult<typeof aggregateFunctionQuery>;
type NullableGroupArrayRow = InferResult<typeof nullableGroupArrayQuery>;

const validRow: BasicRow = {
  user_id: 1,
  event_type: "signup",
};

const validSelectAllRow: SelectAllRow = {
  user_id: 1,
  event_type: "signup",
  created_at: "2025-01-01 10:00:00",
  properties: '{"source":"organic"}',
  event_date: "2025-01-01",
  version: 1,
};

const validSelectAllAliasRow: SelectAllAliasRow = {
  id: 1,
  email: "alice@example.com",
  status: "active",
};

const validSelectFromSubqueryRow: SelectFromSubqueryRow = {
  user_id: 1,
  event_type: "signup",
};

const validSelectFromFinalTableSourceRow: SelectFromFinalTableSourceRow = {
  user_id: 1,
  event_type: "signup",
};

const validSelectFromCteRow: SelectFromCteRow = {
  user_id: 1,
};

const validSelectFromMultipleCtesRow: SelectFromMultipleCtesRow = {
  id: 1,
  email: "alice@example.com",
};

const validGroupedRow: GroupedRow = {
  user_id: 1,
  event_count: "2",
};

const validSelectFromJoinSubquerySettingsRow: SelectFromJoinSubquerySettingsRow = {
  id: 42,
  email: "user42@example.com",
  inquiries_count: "0",
};

const validTypeCastRow: TypeCastRow = {
  id: 1,
  id_i32: 1,
  id_i64: "1",
  id_u32: 1,
  big_user_id_u64: "9007199254740993",
  id_f32: 1,
  amount_f64: 123.45,
  created_date: "2025-01-01",
  created_at_dt: "2025-01-01 10:11:12",
  created_at_dt64: "2025-01-01 10:11:12.123",
  id_text: "1",
  amount_d64: 123.45,
  amount_d128: 123.45,
};

const validArrayFunctionRow: ArrayFunctionRow = {
  id: 1,
  has_trial: 1,
  has_overlap: 1,
  has_required: 1,
  tag_count: "2",
  is_empty: 0,
  is_not_empty: 1,
};

const validStringFunctionRow: StringFunctionRow = {
  id: 1,
  has_ph: 1,
  has_al_insensitive: 1,
  label_is_empty: 0,
  label_is_not_empty: 1,
  label_key: "alpha-1",
  label_lower: "alpha",
  label_upper: "ALPHA",
  label_slice: "lph",
  label_trimmed: "alpha",
  label_left_trimmed: "alpha  ",
  label_right_trimmed: "  alpha",
};

const validNullableStringFunctionRow: NullableStringFunctionRow = {
  id: 1,
  nickname_has_e: null,
  label_matches_maybe: null,
  nickname_is_empty: null,
  nickname_is_not_empty: null,
  nickname_key: null,
  nickname_lower: null,
  nickname_upper: null,
  nickname_slice: null,
  nickname_trimmed: null,
};

const validNullFunctionRow: NullFunctionRow = {
  id: 2,
  nickname_is_null: 0,
  nickname_is_not_null: 1,
  maybe_label: null,
  display_name: "bee",
  display_name_with_literal: "bee",
  nickname_or_default: "bee",
};

const validAggregateFunctionRow: AggregateFunctionRow = {
  sample_count: "2",
  active_samples: "1",
  amount_sum: 123.55,
  big_user_id_sum: "9007199254741035",
  active_amount_sum: 123.45,
  active_big_user_id_sum: "9007199254740993",
  amount_avg: 61.775,
  big_user_id_avg: 4503599627370518,
  active_amount_avg: 123.45,
  min_label: "alpha",
  max_label: "beta",
  uniq_statuses: "2",
  uniq_statuses_exact: "2",
  uniq_active_statuses: "1",
  labels: ["alpha", "beta"],
  nicknames: ["bee"],
  any_label: "alpha",
  any_last_label: "beta",
};

const validNullableGroupArrayRow: NullableGroupArrayRow = {
  nicknames: ["bee"],
};

const validRowsPromise: Promise<BasicRow[]> = basicQuery.execute(client);
const validFirstRowPromise: Promise<BasicRow | undefined> = basicQuery.executeTakeFirst(client);
const validFirstOrThrowRowPromise: Promise<BasicRow> = basicQuery.executeTakeFirstOrThrow(client);

const dbWithClient = createClickHouseDB<TypecheckDB>({ client });
const validRowsWithoutPassingClient: Promise<BasicRow[]> = dbWithClient
  .selectFrom("event_logs as e")
  .select("e.user_id", "e.event_type")
  .execute();
const validInsertResultPromise: Promise<ClickHouseInsertResult> = dbWithClient
  .insertInto("users")
  .values([
    {
      id: 1,
      email: "alice@example.com",
      status: "active",
    },
  ])
  .execute();
const validTypedSamplesInsertPromise: Promise<ClickHouseInsertResult> = dbWithClient
  .insertInto("typed_samples")
  .values([
    {
      id: 3,
      big_user_id: "9007199254740994",
      label: "gamma",
      status: "archived",
      nickname: null,
      tags: ["vip"],
      amount: 98.76,
      created_at: "2025-01-03 00:00:00.001",
      location: [3.14, 2.72],
      attributes: { source: "partner" },
      "metrics.name": ["purchases"],
      "metrics.score": [7],
    },
  ])
  .execute();
const validJsonInsertPromise: Promise<ClickHouseInsertResult> = dbWithClient
  .insertInto("json_samples")
  .values([
    {
      id: 3,
      payload: {
        user: { id: "44" },
        traits: { plan: "pro", active: true },
        tags: ["engaged"],
        metrics: { score: 4.5, rank: 1 },
      },
    },
  ])
  .execute();
const validInsertFromSelectPromise: Promise<ClickHouseInsertResult> = dbWithClient
  .insertInto("users")
  .columns("id", "email", "status")
  .fromSelect(
    dbWithClient
      .selectFrom("users as u")
      .selectExpr((eb) => ["u.id", "u.email", eb.val("active").as("status")]),
  )
  .execute();

void validRow;
void validSelectAllRow;
void validSelectAllAliasRow;
void validSelectFromSubqueryRow;
void validSelectFromFinalTableSourceRow;
void validSelectFromCteRow;
void validSelectFromMultipleCtesRow;
void validGroupedRow;
void validSelectFromJoinSubquerySettingsRow;
void validTypeCastRow;
void validArrayFunctionRow;
void validStringFunctionRow;
void validNullableStringFunctionRow;
void validNullFunctionRow;
void validAggregateFunctionRow;
void validNullableGroupArrayRow;
void validRowsPromise;
void validFirstRowPromise;
void validFirstOrThrowRowPromise;
void validRowsWithoutPassingClient;
void validInsertResultPromise;
void validTypedSamplesInsertPromise;
void validJsonInsertPromise;
void validInsertFromSelectPromise;

db.selectFrom("users as u")
  .innerJoin("event_logs as e", "u.id", "e.user_id")
  .select("u.email", "e.event_type");

db.selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.jsonExtractString("e.properties", "source").as("source")])
  .where((eb) => eb.fn.jsonExtractString("e.properties", "source"), "=", "paid-search");

db.selectFrom("event_logs as e").selectAll();

db.selectFrom("users as u").innerJoin("event_logs as e", "u.id", "e.user_id").selectAll("u");

db.selectFrom(signupsSubquery).selectAll("signups");

db.selectFrom(finalEventLogsSource).select("e.user_id", "e.event_type");

db.selectFrom("users as u")
  .leftJoin(downloadsSubquery, "downloads.user_id", "u.id")
  .select("u.id", "u.email", "downloads.inquiries_count")
  .where("u.status", "=", "active")
  .orderBy("downloads.inquiries_count", "desc")
  .settings({ join_algorithm: "grace_hash" });

db.selectFrom("users as u").select("u.id", "u.email").where("u.id", "in", activeUsersSubquery);

db.selectFrom("users as u").select("u.id").where("u.id", "=", activeUsersSubquery);

withActiveUsers.selectFrom("active_users as au").select("au.user_id");

withMultipleCtes.selectFrom("active_user_emails as aue").select("aue.id", "aue.email");

db.with("active_users", (db) =>
  db
    .selectFrom("event_logs as e")
    .select("e.user_id")
    .where("e.event_type", "=", "signup")
    .groupBy("e.user_id"),
)
  .selectFrom("active_users as au")
  .innerJoin("users as u", "u.id", "au.user_id")
  .select("u.email", "au.user_id");

db.selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
  .groupBy("e.user_id")
  .orderBy("event_count", "desc");

db.selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
  .groupBy("e.user_id")
  .having((eb) => eb.fn.count(), ">", param(1, "Int64"))
  .orderBy("event_count", "desc");

db.selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
  .groupBy("e.user_id")
  .having("event_count", ">", param(1, "Int64"))
  .orderBy("event_count", "desc");

db.selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
  .groupBy("e.user_id")
  .having(
    (eb) => eb.fn.count(),
    ">",
    db
      .selectFrom("users as u")
      .selectExpr((eb) => [eb.fn.count().as("threshold_count")])
      .where("u.id", "=", 1),
  )
  .orderBy("event_count", "desc");

db.selectFrom("users as u")
  .innerJoin(signupsSubquery, "u.id", "signups.user_id")
  .select("u.email", "signups.event_type");

db.selectFrom("users as a")
  .innerJoin("users as b", (eb) =>
    eb.and([eb.cmpRef("a.id", "=", "b.id"), eb.cmpRef("a.email", "=", "b.email")]),
  )
  .select("a.email");

db.selectFrom("users as u")
  .innerJoin(db.table("event_logs").as("e").final(), "u.id", "e.user_id")
  .select("u.email", "e.event_type");

db.selectFrom("users as u")
  .innerJoin("event_logs as e", "u.id", "e.user_id")
  .whereRef("u.id", "=", "e.user_id")
  .select("u.email");

db.selectFrom("event_logs as e").prewhereRef("e.user_id", "=", "e.user_id").select("e.user_id");

db.selectFrom("typed_samples").whereNull("nickname");

db.selectFrom("typed_samples").whereNotNull("nickname");

db.insertInto("users").values([
  {
    id: 1,
    email: "alice@example.com",
    status: "active",
  },
]);

db.insertInto("users")
  .columns("id", "email")
  .values([
    {
      id: 1,
      email: "alice@example.com",
    },
  ]);

db.insertInto("users")
  .columns("id", "email", "status")
  .fromSelect(db.selectFrom("users as u").select("u.id", "u.email", "u.status"));

db.selectFrom("typed_samples").where("status", "=", "active");

db.selectFrom("typed_samples as t")
  .selectExpr((eb) => [eb.fn.toString("t.id").as("id_text")])
  .where((eb) => eb.fn.toUInt32("t.id"), ">", 0);

db.selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    "t.id",
    eb.fn.has("t.tags", "trial").as("has_trial"),
    eb.fn.length("t.tags").as("tag_count"),
  ])
  .prewhere((eb) => eb.fn.notEmpty("t.tags"))
  .where((eb) => eb.fn.hasAny("t.tags", ["vip", "trial"]))
  .where((eb) => eb.fn.length("t.tags"), ">", param(0, "UInt64"))
  .orderBy("t.id", "asc");

db.selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    "t.id",
    eb.fn.like("t.label", "%ph%").as("has_ph"),
    eb.fn.ilike("t.label", "%AL%").as("has_al_insensitive"),
    eb.fn.concat(eb.ref("t.label"), "-", eb.fn.toString("t.id")).as("label_key"),
    eb.fn.lower("t.label").as("label_lower"),
    eb.fn.upper("t.label").as("label_upper"),
    eb.fn.substring("t.label", 2, 3).as("label_slice"),
    eb.fn.trimBoth(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_trimmed"),
  ])
  .prewhere((eb) => eb.fn.notEmpty("t.label"))
  .where((eb) => eb.fn.empty(eb.val("")))
  .orderBy("t.id", "asc");

db.selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    eb.fn.like("t.nickname", "%e%").as("nickname_has_e"),
    eb.fn
      .ilike("t.label", param<string | null>(null, "Nullable(String)"))
      .as("label_matches_maybe"),
    eb.fn.empty("t.nickname").as("nickname_is_empty"),
    eb.fn.notEmpty("t.nickname").as("nickname_is_not_empty"),
    eb.fn.concat(eb.ref("t.nickname"), "-", eb.ref("t.label")).as("nickname_key"),
    eb.fn.lower("t.nickname").as("nickname_lower"),
    eb.fn.substring("t.nickname", 1, 1).as("nickname_slice"),
  ])
  .orderBy("t.id", "asc");

db.selectFrom("typed_samples as t")
  .selectExpr((eb) => [
    eb.fn.now().as("current_time"),
    eb.fn.today().as("current_date"),
    eb.fn.toStartOfMonth("t.created_at").as("month_start"),
    eb.fn.toStartOfWeek("t.created_at").as("week_start"),
    eb.fn.toStartOfDay("t.created_at").as("day_start"),
    eb.fn.toStartOfYear("t.created_at").as("year_start"),
    eb.fn.formatDateTime("t.created_at", "%Y-%m-%d").as("created_date_text"),
    eb.fn
      .dateDiff("day", eb.fn.toDate("t.created_at"), eb.val(param("2025-01-03", "Date")))
      .as("days_until_cutoff"),
    eb.fn.dateAdd("day", 5, "t.created_at").as("plus_five_days"),
    eb.fn.dateSub("hour", 2, "t.created_at").as("minus_two_hours"),
    eb.fn.toYYYYMM("t.created_at").as("created_yyyymm"),
    eb.fn.toYYYYMMDD("t.created_at").as("created_yyyymmdd"),
  ])
  .where((eb) => eb.fn.toYYYYMM("t.created_at"), "=", 202501)
  .orderBy("t.id", "asc");

db.selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.toDate("e.created_at").as("event_date")])
  .groupBy("e.user_id", (eb) => eb.fn.toDate("e.created_at"));

db.selectFrom("typed_samples as a")
  .innerJoin("typed_samples as b", (eb) =>
    eb.cmp(
      eb.fn.coalesce("a.nickname", eb.ref("a.label")),
      "=",
      eb.fn.coalesce("b.nickname", eb.ref("b.label")),
    ),
  )
  .select("a.label");

db.selectFrom("typed_samples as t")
  .selectExpr((eb) => ["t.label", eb.fn.count().as("match_count")])
  .groupBy("t.label", "t.nickname")
  .having((eb) => eb.fn.isNull(eb.fn.nullIf("t.nickname", "bee")));

db.selectFrom("typed_samples as t").selectExpr((eb) => {
  const isActive = eb.cmp("t.status", "=", "active");

  return [
    eb.fn.countIf(isActive).as("active_samples"),
    eb.fn.sum("t.amount").as("amount_sum"),
    eb.fn.sumIf("t.big_user_id", isActive).as("active_big_user_id_sum"),
    eb.fn.avg("t.big_user_id").as("big_user_id_avg"),
    eb.fn.avgIf("t.amount", isActive).as("active_amount_avg"),
    eb.fn.min("t.label").as("min_label"),
    eb.fn.max("t.label").as("max_label"),
    eb.fn.uniq("t.status").as("uniq_statuses"),
    eb.fn.uniqExact("t.status").as("uniq_statuses_exact"),
    eb.fn.uniqIf("t.status", isActive).as("uniq_active_statuses"),
    eb.fn.groupArray("t.label").as("labels"),
    eb.fn.groupArray("t.nickname").as("nicknames"),
    eb.fn.any("t.label").as("any_label"),
    eb.fn.anyLast("t.label").as("any_last_label"),
  ];
});

// @ts-expect-error invalid selection column
db.selectFrom("event_logs as e").select("e.missing_column");

// @ts-expect-error invalid selectAll alias
db.selectFrom("event_logs as e").selectAll("u");

// @ts-expect-error invalid subquery alias column
db.selectFrom(signupsSubquery).select("signups.missing_column");

// @ts-expect-error invalid CTE column
withActiveUsers.selectFrom("active_users as au").select("au.missing_column");

// @ts-expect-error invalid chained CTE column
withMultipleCtes.selectFrom("active_user_emails as aue").select("aue.missing_column");

// @ts-expect-error invalid whereRef column
db.selectFrom("users as u").whereRef("u.missing", "=", "u.id");

db.selectFrom("users as a")
  .innerJoin("users as b", (eb) =>
    eb.and([
      eb.cmpRef("a.id", "=", "b.id"),
      // @ts-expect-error invalid multi-condition join ref
      eb.cmpRef("a.missing", "=", "b.email"),
    ]),
  )
  .select("a.email");

// @ts-expect-error invalid prewhereRef column
db.selectFrom("event_logs as e").prewhereRef("e.user_id", "=", "e.missing");

// @ts-expect-error invalid whereNull column
db.selectFrom("typed_samples").whereNull("missing");

// @ts-expect-error invalid whereNotNull column
db.selectFrom("typed_samples").whereNotNull("missing");

// @ts-expect-error bare null predicates should use whereNull/whereNotNull or param()
db.selectFrom("typed_samples").where("nickname", "=", null);

// @ts-expect-error bare null predicates should use whereNull/whereNotNull or param()
db.selectFrom("typed_samples").prewhere("nickname", "=", null);

// @ts-expect-error invalid groupBy column
db.selectFrom("event_logs as e").groupBy("e.missing");

// @ts-expect-error invalid aggregate alias in orderBy
db.selectFrom("event_logs as e").select("e.user_id").orderBy("event_count", "desc");

// @ts-expect-error invalid having alias
db.selectFrom("event_logs as e").select("e.user_id").having("event_count", ">", param(1, "Int64"));

// @ts-expect-error invalid having value type
db.selectFrom("event_logs as e").select("e.user_id").having("e.user_id", ">", { nope: true });

// @ts-expect-error wrong where value type
db.selectFrom("event_logs as e").where("e.user_id", "=", "signup");

// @ts-expect-error invalid enum literal
db.selectFrom("typed_samples").where("status", "=", "paused");

// @ts-expect-error array helpers reject non-array columns
db.selectFrom("typed_samples").selectExpr((eb) => [eb.fn.length("nickname").as("bad_length")]);

// @ts-expect-error array helpers reject tuple columns
db.selectFrom("typed_samples").selectExpr((eb) => [eb.fn.empty("location").as("bad_empty")]);

// @ts-expect-error wrong has() element type
db.selectFrom("typed_samples").where((eb) => eb.fn.has("tags", 1));

// @ts-expect-error wrong hasAny() element type
db.selectFrom("typed_samples").where((eb) => eb.fn.hasAny("tags", [1]));

// @ts-expect-error string helpers reject numeric columns
db.selectFrom("typed_samples").selectExpr((eb) => [eb.fn.lower("id").as("bad_lower")]);

// @ts-expect-error string helpers reject array columns
db.selectFrom("typed_samples").selectExpr((eb) => [eb.fn.like("tags", "%vip%").as("bad_like")]);

db.selectFrom("typed_samples as t").selectExpr((eb) => [
  // @ts-expect-error formatDateTime() requires a string format literal
  eb.fn.formatDateTime("t.created_at", param("%Y-%m-%d", "String")).as("bad_format"),
]);

db.selectFrom("typed_samples as t").selectExpr((eb) => [
  // @ts-expect-error dateAdd() rejects unsupported date/time units
  eb.fn.dateAdd("decade", 1, "t.created_at").as("bad_add"),
]);

// @ts-expect-error nullIf() compare values must match the input type
db.selectFrom("typed_samples").selectExpr((eb) => [eb.fn.nullIf("label", 1).as("bad_null_if")]);

// @ts-expect-error ifNull() fallback values must match the input type
db.selectFrom("typed_samples").selectExpr((eb) => [eb.fn.ifNull("nickname", 1).as("bad_if_null")]);

db.selectFrom("typed_samples").selectExpr((eb) => {
  // @ts-expect-error coalesce() string literals should use eb.val() to avoid ref ambiguity
  const badCoalesce = eb.fn.coalesce("nickname", "Unknown").as("bad_coalesce");

  return [badCoalesce];
});

// @ts-expect-error groupArray() drops null elements from nullable inputs
({ nicknames: [null] }) satisfies NullableGroupArrayRow;

// @ts-expect-error wrong cmp() value type
db.selectFrom("typed_samples").where((eb) => eb.cmp("id", "=", "oops"));

db.selectFrom("typed_samples").selectExpr((eb) => [
  // @ts-expect-error substring offset must be numeric
  eb.fn.substring("label", "2", 3).as("bad_slice"),
]);

db.selectFrom("typed_samples as t").selectExpr((eb) => [
  // @ts-expect-error scale must be numeric
  eb.fn.toDecimal64("t.amount", "2").as("amount_d64"),
]);

db.insertInto("typed_samples").values([
  {
    id: 3,
    // @ts-expect-error wrong insert value type
    big_user_id: 99,
    label: "gamma",
    status: "archived",
    nickname: null,
    tags: ["vip"],
    amount: 98.76,
    created_at: "2025-01-03 00:00:00.001",
    location: [3.14, 2.72],
    attributes: { source: "partner" },
    "metrics.name": ["purchases"],
    "metrics.score": [7],
  },
]);

db.insertInto("typed_samples").values([
  {
    id: 4,
    big_user_id: "100",
    label: "delta",
    status: "pending",
    nickname: null,
    tags: [],
    amount: 1,
    created_at: "2025-01-04 00:00:00.000",
    location: [0, 0],
    attributes: {},
    "metrics.name": ["opens"],
    "metrics.score": [1],
  },
]);

db.insertInto("typed_samples").values([
  {
    id: 5,
    big_user_id: "101",
    label: "epsilon",
    // @ts-expect-error invalid enum insert literal
    status: "paused",
    nickname: null,
    tags: [],
    amount: 1,
    created_at: "2025-01-05 00:00:00.000",
    location: [0, 0],
    attributes: {},
    "metrics.name": ["opens"],
    "metrics.score": [1],
  },
]);

// @ts-expect-error missing insert field
db.insertInto("users").values([{ id: 2, email: "missing@example.com" }]);

// @ts-expect-error invalid insert column
db.insertInto("users").columns("missing");

db.insertInto("users")
  .columns("id", "email")
  .values([
    {
      id: 1,
      // @ts-expect-error columns() narrows accepted insert values
      status: "active",
      email: "alice@example.com",
    },
  ]);

db.selectFrom("users as u").innerJoin(
  "event_logs as e",
  "u.id",
  // @ts-expect-error invalid joined column reference
  "e.missing_column",
);

// @ts-expect-error invalid result type
const invalidRow: BasicRow = { user_id: "1", event_type: "signup" };

void invalidRow;

const rows = await db.selectFrom("users").selectAll().where("status", "=", "active").execute();

rows.forEach((row) => {
  void row.id;
  void row.email;
  void row.status;
});

const results = await db.selectFrom("event_logs as e").select("e.event_type").execute();
for (const result of results) {
  console.log(result.event_type);
}

db.insertInto("typed_samples").values([
  {
    id: 5,
    big_user_id: "101",
    label: "epsilon",
    status: "active",
    nickname: null,
    tags: [],
    amount: 1,
    created_at: "2025-01-05 00:00:00.000",
    location: [0, 0],
    attributes: {},
    "metrics.name": ["opens"],
    "metrics.score": [1],
  },
]);

db.insertInto("json_samples").values([
  {
    id: 4,
    payload: {
      user: { id: "45" },
      traits: { plan: "a plan", active: true },
      tags: ["tag1", "tag2"],
      metrics: { score: 100.5, rank: 1 },
    },
  },
]);

const schema = defineSchema({
  users: table.replacingMergeTree({
    id: UInt32(),
    email: CHString(),
    created_at: DateTime64(3),
    signup_date: CHDate(),
  }),
}).views((db) => ({
  final_users: view.as(db.selectFrom(db.table("users").final().as("u")).selectAll("u")),
  daily_users: view.as(
    db
      .selectFrom("users as u")
      .selectExpr((eb) => ["u.signup_date", eb.fn.count().as("total_users")])
      .groupBy("u.signup_date"),
  ),
  formatted_users: view.as(
    db
      .selectFrom("users as u")
      .selectExpr((eb) => [
        "u.id",
        eb.fn.toString("u.id").as("id_text"),
        eb.fn.lower("u.email").as("email_lower"),
        eb.fn.formatDateTime("u.created_at", "%Y-%m-%d").as("created_date_text"),
        eb.fn.toYYYYMM("u.created_at").as("created_yyyymm"),
      ]),
  ),
}));

const richerSchema = defineSchema({
  schema_runtime_samples: table({
    id: UInt32(),
    event_date: CHDate(),
    event_time: DateTime(),
    created_at: DateTime64(3),
    total_count: UInt64(),
    note: Nullable(CHString()),
    event_dates: CHArray(CHDate()),
    created_history: CHArray(DateTime64(3)),
  }),
});

const scalarSchema = defineSchema({
  schema_scalar_samples: table({
    tiny_u8: UInt8(),
    small_u16: UInt16(),
    tiny_i8: Int8(),
    small_i16: Int16(),
    event_date32: Date32(),
    code: FixedString(8),
    account_uuid: UUID(),
    client_ipv4: IPv4(),
    client_ipv6: IPv6(),
    category: LowCardinality(CHString()),
  }),
});

const schemaDb = createClickHouseDB({ schema, client });
const richerSchemaDb = createClickHouseDB({ schema: richerSchema, client });
const scalarSchemaDb = createClickHouseDB({ schema: scalarSchema, client });
const schemaQuery = schemaDb
  .selectFrom("users as u")
  .select("u.id", "u.email", "u.created_at")
  .where("u.created_at", ">=", new Date("2025-01-01T00:00:00.000Z"));
const inheritedViewQuery = schemaDb
  .selectFrom("final_users as f")
  .select("f.id", "f.email", "f.created_at")
  .where("f.signup_date", ">=", new Date("2025-01-01T00:00:00.000Z"));
const aggregateViewQuery = schemaDb
  .selectFrom("daily_users as d")
  .select("d.signup_date", "d.total_users")
  .orderBy("d.signup_date", "asc");
const formattedViewQuery = schemaDb
  .selectFrom("formatted_users as f")
  .select("f.id", "f.id_text", "f.email_lower", "f.created_date_text", "f.created_yyyymm")
  .orderBy("f.id", "asc");
const richerSchemaQuery = richerSchemaDb
  .selectFrom("schema_runtime_samples as s")
  .select(
    "s.id",
    "s.event_date",
    "s.event_time",
    "s.created_at",
    "s.total_count",
    "s.note",
    "s.event_dates",
    "s.created_history",
  )
  .where("s.event_date", ">=", new Date("2025-01-01T00:00:00.000Z"))
  .where("s.event_time", ">=", new Date("2025-01-01T12:00:00.000Z"))
  .where("s.created_at", ">=", new Date("2025-01-01T12:00:00.123Z"))
  .where("s.event_date", "in", [new Date("2025-01-01T00:00:00.000Z")])
  .where("s.total_count", "in", ["1", "2"])
  .where("s.event_time", ">=", param(new Date("2025-01-01T12:00:00.000Z"), "DateTime"))
  .orderBy("s.id", "asc");
const scalarSchemaQuery = scalarSchemaDb
  .selectFrom("schema_scalar_samples as s")
  .select(
    "s.tiny_u8",
    "s.small_u16",
    "s.tiny_i8",
    "s.small_i16",
    "s.event_date32",
    "s.code",
    "s.account_uuid",
    "s.client_ipv4",
    "s.client_ipv6",
    "s.category",
  )
  .where("s.tiny_u8", "=", 7)
  .where("s.event_date32", ">=", new Date("2025-01-01T00:00:00.000Z"))
  .where("s.category", "=", "premium")
  .orderBy("s.tiny_u8", "asc");
type SchemaRow = InferResult<typeof schemaQuery>;
type InheritedViewRow = InferResult<typeof inheritedViewQuery>;
type AggregateViewRow = InferResult<typeof aggregateViewQuery>;
type FormattedViewRow = InferResult<typeof formattedViewQuery>;
type RicherSchemaRow = InferResult<typeof richerSchemaQuery>;
type ScalarSchemaRow = InferResult<typeof scalarSchemaQuery>;
const validSchemaRow: SchemaRow = {
  id: 1,
  email: "alice@example.com",
  created_at: "2025-01-01 00:00:00.000",
};
const validInheritedViewRow: InheritedViewRow = {
  id: 1,
  email: "alice@example.com",
  created_at: "2025-01-01 00:00:00.000",
};
const validAggregateViewRow: AggregateViewRow = {
  signup_date: "2025-01-01",
  total_users: "42",
};
const validFormattedViewRow: FormattedViewRow = {
  id: 1,
  id_text: "1",
  email_lower: "alice@example.com",
  created_date_text: "2025-01-01",
  created_yyyymm: 202501,
};

// @ts-expect-error projected views should not expose source columns that are not selected
schemaDb.selectFrom("daily_users as d").select("d.email");

// @ts-expect-error projected views should not allow predicates on non-projected source columns
schemaDb.selectFrom("daily_users as d").where("d.email", "=", "alice@example.com");

const invalidAggregateViewRow: AggregateViewRow = {
  signup_date: "2025-01-01",
  total_users: "42",
  // @ts-expect-error projected view result types should not contain hidden source columns
  email: "alice@example.com",
};
const validRicherSchemaRow: RicherSchemaRow = {
  id: 1,
  event_date: "2025-01-01",
  event_time: "2025-01-01 12:00:00",
  created_at: "2025-01-01 12:00:00.123",
  total_count: "42",
  note: null,
  event_dates: ["2025-01-01", "2025-01-02"],
  created_history: ["2025-01-01 12:00:00.123"],
};
const validScalarSchemaRow: ScalarSchemaRow = {
  tiny_u8: 7,
  small_u16: 512,
  tiny_i8: -3,
  small_i16: -1024,
  event_date32: "2025-01-01",
  code: "ABCDEFGH",
  account_uuid: "550e8400-e29b-41d4-a716-446655440000",
  client_ipv4: "127.0.0.1",
  client_ipv6: "::1",
  category: "premium",
};
const schemaInsertResultPromise: Promise<ClickHouseInsertResult> = schemaDb
  .insertInto("users")
  .values([
    {
      id: 1,
      email: "alice@example.com",
      created_at: new Date("2025-01-01T00:00:00.000Z"),
      signup_date: "2025-01-01",
    },
  ])
  .execute();
const richerSchemaInsertResultPromise: Promise<ClickHouseInsertResult> = richerSchemaDb
  .insertInto("schema_runtime_samples")
  .values([
    {
      id: 1,
      event_date: new Date("2025-01-01T00:00:00.000Z"),
      event_time: "2025-01-01 12:00:00",
      created_at: new Date("2025-01-01T12:00:00.123Z"),
      total_count: 42n,
      note: null,
      event_dates: [new Date("2025-01-01T00:00:00.000Z"), "2025-01-02"],
      created_history: [new Date("2025-01-01T12:00:00.123Z"), "2025-01-02 12:00:00.456"],
    },
  ])
  .execute();
const scalarSchemaInsertResultPromise: Promise<ClickHouseInsertResult> = scalarSchemaDb
  .insertInto("schema_scalar_samples")
  .values([
    {
      tiny_u8: 7,
      small_u16: 512,
      tiny_i8: -3,
      small_i16: -1024,
      event_date32: new Date("2025-01-01T00:00:00.000Z"),
      code: "ABCDEFGH",
      account_uuid: "550e8400-e29b-41d4-a716-446655440000",
      client_ipv4: "127.0.0.1",
      client_ipv6: "::1",
      category: "premium",
    },
  ])
  .execute();

void validSchemaRow;
void validInheritedViewRow;
void validAggregateViewRow;
void validFormattedViewRow;
void validRicherSchemaRow;
void validScalarSchemaRow;
void invalidAggregateViewRow;
void schemaInsertResultPromise;
void richerSchemaInsertResultPromise;
void scalarSchemaInsertResultPromise;

// @ts-expect-error views are not insertable
schemaDb.insertInto("final_users");

// @ts-expect-error views are not table sources
schemaDb.table("final_users");

// @ts-expect-error UInt32 predicates should stay numeric
schemaDb.selectFrom("users as u").where("u.id", "=", "1");

defineSchema({
  users: table({
    id: UInt32(),
  }),
}).views((db) => ({
  // @ts-expect-error missing sources should fail in the staged view builder
  bad_view: view.as(db.selectFrom("missing_source as m").selectAll("m")),
}));

richerSchemaDb.insertInto("schema_runtime_samples").values([
  {
    id: 1,
    event_date: "2025-01-01",
    event_time: "2025-01-01 12:00:00",
    created_at: "2025-01-01 12:00:00.123",
    total_count: "1",
    note: null,
    // @ts-expect-error Date array columns should not accept scalar Date values on insert
    event_dates: new Date("2025-01-01T00:00:00.000Z"),
    created_history: [],
  },
]);

// @ts-expect-error nullable string columns should not accept numeric predicate values
richerSchemaDb.selectFrom("schema_runtime_samples as s").where("s.note", "=", 1);

// @ts-expect-error DateTime columns should not accept numeric predicate values
richerSchemaDb.selectFrom("schema_runtime_samples as s").where("s.event_time", "=", 1);

// @ts-expect-error UInt8 columns should stay numeric
scalarSchemaDb.selectFrom("schema_scalar_samples as s").where("s.tiny_u8", "=", "7");

// @ts-expect-error Date32 columns should not accept numeric predicate values
scalarSchemaDb.selectFrom("schema_scalar_samples as s").where("s.event_date32", "=", 1);

// @ts-expect-error low cardinality string columns should not accept numeric predicate values
scalarSchemaDb.selectFrom("schema_scalar_samples as s").where("s.category", "=", 1);

richerSchemaDb.insertInto("schema_runtime_samples").values([
  {
    id: 1,
    event_date: "2025-01-01",
    event_time: "2025-01-01 12:00:00",
    created_at: "2025-01-01 12:00:00.123",
    total_count: "1",
    // @ts-expect-error nullable string columns should not accept numeric insert values
    note: 1,
    event_dates: [],
    created_history: [],
  },
]);

richerSchemaDb.insertInto("schema_runtime_samples").values([
  {
    id: 1,
    event_date: "2025-01-01",
    event_time: "2025-01-01 12:00:00",
    created_at: "2025-01-01 12:00:00.123",
    total_count: "1",
    note: null,
    event_dates: ["2025-01-01"],
    // @ts-expect-error DateTime64 arrays should not accept numeric members
    created_history: [1],
  },
]);
