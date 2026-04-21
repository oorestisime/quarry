import { createClickHouseDB, param, ExpressionBuilder, type ExecutableQuery } from "../src";

export interface SpikeDB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
    event_date: string;
    properties: string;
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
  };
}

let db = createClickHouseDB<SpikeDB>();

export function setQueryCaseDb(nextDb: typeof db): void {
  db = nextDb;
}

export interface QueryCase {
  name: string;
  file: string;
  expectedParams: Record<string, unknown>;
}

export interface TypedQueryCase<TQuery extends ExecutableQuery<unknown>> extends QueryCase {
  expectedRows: Awaited<ReturnType<TQuery["execute"]>>;
  build: () => TQuery;
}

function defineQueryCase<TQuery extends ExecutableQuery<unknown>>(
  queryCase: TypedQueryCase<TQuery>,
): TypedQueryCase<TQuery> {
  return queryCase;
}

export const simpleSelectCase = defineQueryCase({
  name: "01 simple select",
  file: "01_simple_select.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { user_id: 3, event_type: "signup" },
    { user_id: 1, event_type: "signup" },
  ],
  build: () =>
    db
      .selectFrom("event_logs as e")
      .select("e.user_id", "e.event_type")
      .where("e.event_type", "=", "signup")
      .orderBy("e.created_at", "desc")
      .limit(50),
});

export const selectAllCase = defineQueryCase({
  name: "11 select all",
  file: "11_select_all.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    {
      user_id: 3,
      event_type: "signup",
      created_at: "2025-01-03 08:00:00",
      event_date: "2025-01-03",
      properties: '{"source":"paid-search"}',
      version: 1,
    },
    {
      user_id: 1,
      event_type: "signup",
      created_at: "2025-01-01 10:00:00",
      event_date: "2025-01-01",
      properties: '{"source":"organic"}',
      version: 1,
    },
  ],
  build: () =>
    db
      .selectFrom("event_logs as e")
      .selectAll()
      .where("e.event_type", "=", "signup")
      .orderBy("e.created_at", "desc")
      .limit(2),
});

export const finalPrewhereSettingsCase = defineQueryCase({
  name: "02 final prewhere settings",
  file: "02_final_prewhere_settings.sql",
  expectedParams: { p0: "2025-01-01", p1: "signup" },
  expectedRows: [
    { user_id: 3, created_at: "2025-01-03 08:00:00" },
    { user_id: 1, created_at: "2025-01-01 10:00:00" },
  ],
  build: () =>
    db
      .selectFrom("event_logs as e")
      .final()
      .select("e.user_id", "e.created_at")
      .prewhere("e.event_date", ">=", param("2025-01-01", "Date"))
      .where("e.event_type", "=", "signup")
      .orderBy("e.created_at", "desc")
      .limit(100)
      .settings({ max_threads: 8 }),
});

export const innerJoinCase = defineQueryCase({
  name: "04 inner join",
  file: "04_inner_join.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { id: 3, email: "cory@example.com", event_type: "signup" },
    { id: 1, email: "alice@example.com", event_type: "signup" },
  ],
  build: () =>
    db
      .selectFrom("users as u")
      .innerJoin("event_logs as e", "u.id", "e.user_id")
      .select("u.id", "u.email", "e.event_type")
      .where("e.event_type", "=", "signup")
      .orderBy("e.created_at", "desc")
      .limit(20),
});

export const joinSubqueryAliasCase = defineQueryCase({
  name: "13 join subquery alias",
  file: "13_join_subquery_alias.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { email: "alice@example.com", event_type: "signup" },
    { email: "cory@example.com", event_type: "signup" },
  ],
  build: () => {
    const signups = db
      .selectFrom("event_logs as e")
      .select("e.user_id", "e.event_type")
      .where("e.event_type", "=", "signup")
      .as("signups");

    return db
      .selectFrom("users as u")
      .innerJoin(signups, "u.id", "signups.user_id")
      .select("u.email", "signups.event_type")
      .orderBy("u.id", "asc")
      .limit(10);
  },
});

export const joinFinalTableSourceCase = defineQueryCase({
  name: "14 join final table source",
  file: "14_join_final_table_source.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { id: 3, email: "cory@example.com", event_type: "signup" },
    { id: 1, email: "alice@example.com", event_type: "signup" },
  ],
  build: () =>
    db
      .selectFrom("users as u")
      .innerJoin(db.table("event_logs").as("e").final(), "u.id", "e.user_id")
      .select("u.id", "u.email", "e.event_type")
      .where("e.event_type", "=", "signup")
      .orderBy("e.created_at", "desc")
      .limit(20),
});

export const whereRefCase = defineQueryCase({
  name: "15 where ref",
  file: "15_where_ref.sql",
  expectedParams: {},
  expectedRows: [
    { id: 1, email: "alice@example.com", event_type: "browse" },
    { id: 1, email: "alice@example.com", event_type: "signup" },
    { id: 2, email: "bruno@example.com", event_type: "purchase" },
    { id: 3, email: "cory@example.com", event_type: "signup" },
  ],
  build: () =>
    db
      .selectFrom("users as u")
      .innerJoin("event_logs as e", "u.id", "e.user_id")
      .select("u.id", "u.email", "e.event_type")
      .whereRef("u.id", "=", "e.user_id")
      .orderBy("u.id", "asc")
      .limit(20),
});

export const groupByAggregateCase = defineQueryCase({
  name: "16 group by aggregate",
  file: "16_group_by_aggregate.sql",
  expectedParams: { p0: ["signup", "purchase", "browse"] },
  expectedRows: [
    { user_id: 1, event_count: "2" },
    { user_id: 2, event_count: "1" },
    { user_id: 3, event_count: "1" },
  ],
  build: () =>
    db
      .selectFrom("event_logs as e")
      .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
      .where("e.event_type", "in", ["signup", "purchase", "browse"])
      .groupBy("e.user_id")
      .orderBy("event_count", "desc")
      .orderBy("e.user_id", "asc")
      .limit(25),
});

export const groupByHavingCase = defineQueryCase({
  name: "03 group by having",
  file: "03_group_by_having.sql",
  expectedParams: { p0: ["signup", "purchase", "browse"], p1: 1 },
  expectedRows: [{ user_id: 1, event_count: "2" }],
  build: () =>
    db
      .selectFrom("event_logs as e")
      .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
      .where("e.event_type", "in", ["signup", "purchase", "browse"])
      .groupBy("e.user_id")
      .having((eb) => eb.fn.count(), ">", param(1, "Int64"))
      .orderBy("event_count", "desc")
      .limit(25),
});

export const cteJoinCase = defineQueryCase({
  name: "05 cte join",
  file: "05_cte_join.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { id: 1, email: "alice@example.com" },
    { id: 3, email: "cory@example.com" },
  ],
  build: () =>
    db
      .with("active_users", (db) =>
        db
          .selectFrom("event_logs as e")
          .select("e.user_id")
          .where("e.event_type", "=", "signup")
          .groupBy("e.user_id"),
      )
      .selectFrom("active_users as au")
      .innerJoin("users as u", "u.id", "au.user_id")
      .select("u.id", "u.email")
      .orderBy("u.id", "asc")
      .limit(100),
});

export const multipleCtesCase = defineQueryCase({
  name: "17 multiple ctes",
  file: "17_multiple_ctes.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { id: 1, email: "alice@example.com" },
    { id: 3, email: "cory@example.com" },
  ],
  build: () =>
    db
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
      )
      .selectFrom("active_user_emails as aue")
      .select("aue.id", "aue.email")
      .orderBy("aue.id", "asc")
      .limit(100),
});

export const cteLeftJoinBaseTableCase = defineQueryCase({
  name: "18 cte left join base table",
  file: "18_cte_left_join_base_table.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { id: 1, email: "alice@example.com", event_count: "2" },
    { id: 3, email: "cory@example.com", event_count: "1" },
  ],
  build: () =>
    db
      .with("active_users", (db) =>
        db
          .selectFrom("event_logs as e")
          .select("e.user_id")
          .where("e.event_type", "=", "signup")
          .groupBy("e.user_id"),
      )
      .with("user_counts", (db) =>
        db
          .selectFrom("event_logs as e")
          .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
          .groupBy("e.user_id"),
      )
      .selectFrom("users as u")
      .leftJoin("active_users as au", "au.user_id", "u.id")
      .leftJoin("user_counts as uc", "uc.user_id", "u.id")
      .select("u.id", "u.email", "uc.event_count")
      .whereRef("au.user_id", "=", "u.id")
      .orderBy("u.id", "asc")
      .limit(100),
});

export const joinSubquerySettingsCase = defineQueryCase({
  name: "10 join subquery settings",
  file: "10_join_subquery_settings.sql",
  expectedParams: { p0: "2025-01-01 00:00:00", p1: "active" },
  expectedRows: [
    { id: 42, email: "user42@example.com", inquiries_count: "0" },
    { id: 43, email: "user43@example.com", inquiries_count: "0" },
  ],
  build: () => {
    const downloads = db
      .selectFrom(db.table("inquiry_downloads").as("d").final())
      .selectExpr((eb) => ["d.user_id", eb.fn.count().as("inquiries_count")])
      .prewhere("d.created_at", ">=", param("2025-01-01 00:00:00", "DateTime"))
      .groupBy("d.user_id")
      .as("downloads");

    return db
      .selectFrom("users as u")
      .leftJoin(downloads, "downloads.user_id", "u.id")
      .select("u.id", "u.email", "downloads.inquiries_count")
      .where("u.status", "=", "active")
      .orderBy("downloads.inquiries_count", "desc")
      .orderBy("u.id", "asc")
      .limit(20)
      .offset(40)
      .settings({ join_algorithm: "grace_hash" });
  },
});

export const inSubqueryCase = defineQueryCase({
  name: "06 in subquery",
  file: "06_in_subquery.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { id: 1, email: "alice@example.com" },
    { id: 3, email: "cory@example.com" },
  ],
  build: () => {
    const activeUsers = db
      .selectFrom("event_logs as e")
      .select("e.user_id")
      .where("e.event_type", "=", "signup");

    return db
      .selectFrom("users as u")
      .select("u.id", "u.email")
      .where("u.id", "in", activeUsers)
      .orderBy("u.id", "asc");
  },
});

export const multiConditionJoinCase = defineQueryCase({
  name: "19 multi condition join",
  file: "19_multi_condition_join.sql",
  expectedParams: { p0: "active" },
  expectedRows: [
    { id: 1, email: "alice@example.com" },
    { id: 2, email: "bruno@example.com" },
  ],
  build: () =>
    db
      .selectFrom("users as a")
      .innerJoin("users as b", (eb) =>
        eb.and([eb.cmpRef("a.id", "=", "b.id"), eb.cmpRef("a.email", "=", "b.email")]),
      )
      .select("a.id", "a.email")
      .where("a.status", "=", "active")
      .orderBy("a.id", "asc")
      .limit(2),
});

export const havingSubqueryCase = defineQueryCase({
  name: "20 having subquery",
  file: "20_having_subquery.sql",
  expectedParams: { p0: 1 },
  expectedRows: [{ user_id: 1, event_count: "2" }],
  build: () => {
    const threshold = db
      .selectFrom("users as u")
      .selectExpr((eb) => [eb.fn.count().as("threshold_count")])
      .where("u.id", "=", 1);

    return db
      .selectFrom("event_logs as e")
      .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
      .groupBy("e.user_id")
      .having((eb) => eb.fn.count(), ">", threshold)
      .orderBy("e.user_id", "asc");
  },
});

export const selectAllForAliasCase = defineQueryCase({
  name: "12 select all alias",
  file: "12_select_all_alias.sql",
  expectedParams: { p0: "signup" },
  expectedRows: [
    { id: 3, email: "cory@example.com", status: "inactive" },
    { id: 1, email: "alice@example.com", status: "active" },
  ],
  build: () =>
    db
      .selectFrom("users as u")
      .innerJoin("event_logs as e", "u.id", "e.user_id")
      .selectAll("u")
      .where("e.event_type", "=", "signup")
      .orderBy("e.created_at", "desc")
      .limit(20),
});

export const jsonExtractCase = defineQueryCase({
  name: "09 json extract",
  file: "09_json_extract.sql",
  expectedParams: { p0: "paid-search" },
  expectedRows: [
    { user_id: 3, source: "paid-search" },
    { user_id: 2, source: "paid-search" },
  ],
  build: () =>
    db
      .selectFrom("event_logs as e")
      .selectExpr((eb) => [
        "e.user_id",
        eb.fn.jsonExtractString("e.properties", "source").as("source"),
      ])
      .where((eb) => eb.fn.jsonExtractString("e.properties", "source"), "=", "paid-search")
      .orderBy("e.created_at", "desc")
      .limit(50),
});

export const typeCastFunctionsCase = defineQueryCase({
  name: "21 type cast functions",
  file: "21_type_cast_functions.sql",
  expectedParams: { p0: 0 },
  expectedRows: [
    {
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
    },
    {
      id: 2,
      id_i32: 2,
      id_i64: "2",
      id_u32: 2,
      big_user_id_u64: "42",
      id_f32: 2,
      amount_f64: 0.1,
      created_date: "2025-01-02",
      created_at_dt: "2025-01-02 03:04:05",
      created_at_dt64: "2025-01-02 03:04:05.678",
      id_text: "2",
      amount_d64: 0.1,
      amount_d128: 0.1,
    },
  ],
  build: () =>
    db
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
      .orderBy("t.id", "asc"),
});

export const arrayFunctionsCase = defineQueryCase({
  name: "22 array functions",
  file: "22_array_functions.sql",
  expectedParams: {
    p0: "trial",
    p1: ["vip", "trial"],
    p2: ["new", "trial"],
  },
  expectedRows: [
    {
      id: 1,
      has_trial: 1,
      has_overlap: 1,
      has_required: 1,
      tag_count: "2",
      is_empty: 0,
      is_not_empty: 1,
    },
    {
      id: 2,
      has_trial: 0,
      has_overlap: 0,
      has_required: 0,
      tag_count: "0",
      is_empty: 1,
      is_not_empty: 0,
    },
  ],
  build: () =>
    db
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
      .orderBy("t.id", "asc"),
});

export const stringFunctionsCase = defineQueryCase({
  name: "23 string functions",
  file: "23_string_functions.sql",
  expectedParams: {
    p0: "%ph%",
    p1: "%AL%",
    p2: "-",
    p3: 2,
    p4: 3,
    p5: "  ",
    p6: "  ",
    p7: "  ",
    p8: "  ",
    p9: "  ",
    p10: "  ",
  },
  expectedRows: [
    {
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
    },
    {
      id: 2,
      has_ph: 0,
      has_al_insensitive: 0,
      label_is_empty: 0,
      label_is_not_empty: 1,
      label_key: "beta-2",
      label_lower: "beta",
      label_upper: "BETA",
      label_slice: "eta",
      label_trimmed: "beta",
      label_left_trimmed: "beta  ",
      label_right_trimmed: "  beta",
    },
  ],
  build: () =>
    db
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
      .orderBy("t.id", "asc"),
});

export const aggregateFunctionsCase = defineQueryCase({
  name: "24 aggregate functions",
  file: "24_aggregate_functions.sql",
  expectedParams: {
    p0: "active",
    p1: "active",
    p2: "active",
    p3: "active",
    p4: "active",
  },
  expectedRows: [
    {
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
    },
  ],
  build: () =>
    db.selectFrom("typed_samples as t").selectExpr((eb) => {
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
    }),
});

export const nullFunctionsCase = defineQueryCase({
  name: "25 null functions",
  file: "25_null_functions.sql",
  expectedParams: {
    p0: "beta",
    p1: "Unknown",
    p2: "Unknown",
  },
  expectedRows: [
    {
      id: 1,
      nickname_is_null: 1,
      nickname_is_not_null: 0,
      maybe_label: "alpha",
      display_name: "alpha",
      display_name_with_literal: "Unknown",
      nickname_or_default: "Unknown",
    },
    {
      id: 2,
      nickname_is_null: 0,
      nickname_is_not_null: 1,
      maybe_label: null,
      display_name: "bee",
      display_name_with_literal: "bee",
      nickname_or_default: "bee",
    },
  ],
  build: () =>
    db
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
      .orderBy("t.id", "asc"),
});

export const heavyHitterFunctionsCase = defineQueryCase({
  name: "28 heavy hitter functions",
  file: "28_heavy_hitter_functions.sql",
  expectedParams: {
    p0: "active",
    p1: "inactive",
    p2: 10,
    p3: 10,
    p4: 0,
  },
  expectedRows: [
    {
      id: 1,
      status_label: "alpha",
      least_val: 1,
      greatest_val: 10,
      ceil_amount: 124,
      floor_amount: 123,
      id_u8: 1,
      created_year: 2025,
      created_month: 1,
    },
    {
      id: 2,
      status_label: "inactive",
      least_val: 2,
      greatest_val: 10,
      ceil_amount: 1,
      floor_amount: 0,
      id_u8: 2,
      created_year: 2025,
      created_month: 1,
    },
  ],
  build: () =>
    db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [
        "t.id",
        eb.fn
          .if(eb.cmp("t.status", "=", "active"), eb.ref("t.label"), eb.val("inactive"))
          .as("status_label"),
        eb.fn.least(eb.ref("t.id"), eb.val(param(10, "UInt32"))).as("least_val"),
        eb.fn.greatest(eb.ref("t.id"), eb.val(param(10, "UInt32"))).as("greatest_val"),
        eb.fn.ceil("t.amount").as("ceil_amount"),
        eb.fn.floor("t.amount").as("floor_amount"),
        eb.fn.toUInt8("t.id").as("id_u8"),
        eb.fn.toYear("t.created_at").as("created_year"),
        eb.fn.toMonth("t.created_at").as("created_month"),
      ])
      .where((eb) => eb.fn.toUInt8("t.id"), ">", 0)
      .orderBy("t.id", "asc"),
});

export const dateTimeFunctionsCase = defineQueryCase({
  name: "26 date/time functions",
  file: "26_date_time_functions.sql",
  expectedParams: {
    p0: "2025-01-03",
    p1: 5,
    p2: 2,
  },
  expectedRows: [
    {
      id: 1,
      month_start: "2025-01-01",
      year_start: "2025-01-01",
      created_date_text: "2025-01-01",
      days_until_cutoff: "2",
      plus_five_days: "2025-01-06 10:11:12.123",
      minus_two_hours: "2025-01-01 08:11:12.123",
      created_yyyymm: 202501,
      created_yyyymmdd: 20250101,
    },
    {
      id: 2,
      month_start: "2025-01-01",
      year_start: "2025-01-01",
      created_date_text: "2025-01-02",
      days_until_cutoff: "1",
      plus_five_days: "2025-01-07 03:04:05.678",
      minus_two_hours: "2025-01-02 01:04:05.678",
      created_yyyymm: 202501,
      created_yyyymmdd: 20250102,
    },
  ],
  build: () =>
    db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [
        "t.id",
        eb.fn.toStartOfMonth("t.created_at").as("month_start"),
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
      .orderBy("t.id", "asc"),
});

export const chainedExpressionWhereCase = defineQueryCase({
  name: "27 chained expression where",
  file: "27_chained_expression_where.sql",
  expectedParams: { p0: "active", p1: "trial" },
  expectedRows: [{ id: 1, label: "alpha" }],
  build: () => {
    const eb = new ExpressionBuilder<any>();
    const statusExpr = eb.cmp("t.status", "=", "active");
    const tagsExpr = eb.fn.has("t.tags", "trial");

    return db
      .selectFrom("typed_samples as t")
      .select("t.id", "t.label")
      .where(statusExpr)
      .where(tagsExpr)
      .orderBy("t.id", "asc");
  },
});
