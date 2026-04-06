import { createClickHouseDB, param, type ExecutableQuery } from "../src";

interface SpikeDB {
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
}

const db = createClickHouseDB<SpikeDB>();

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
