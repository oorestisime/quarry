import {
  createClickHouseDB,
  param,
  type ClickHouseClient,
  type ClickHouseInsertResult,
  type InferResult,
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

type BasicRow = InferResult<typeof basicQuery>;
type SelectAllRow = InferResult<typeof selectAllQuery>;
type SelectAllAliasRow = InferResult<typeof selectAllAliasQuery>;
type SelectFromSubqueryRow = InferResult<typeof selectFromSubqueryQuery>;
type SelectFromFinalTableSourceRow = InferResult<typeof selectFromFinalTableSourceQuery>;
type SelectFromCteRow = InferResult<typeof selectFromCteQuery>;
type SelectFromMultipleCtesRow = InferResult<typeof selectFromMultipleCtesQuery>;
type GroupedRow = InferResult<typeof groupedQuery>;
type SelectFromJoinSubquerySettingsRow = InferResult<typeof selectFromJoinSubquerySettingsQuery>;

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

void validRow;
void validSelectAllRow;
void validSelectAllAliasRow;
void validSelectFromSubqueryRow;
void validSelectFromFinalTableSourceRow;
void validSelectFromCteRow;
void validSelectFromMultipleCtesRow;
void validGroupedRow;
void validSelectFromJoinSubquerySettingsRow;
void validRowsPromise;
void validFirstRowPromise;
void validFirstOrThrowRowPromise;
void validRowsWithoutPassingClient;
void validInsertResultPromise;
void validTypedSamplesInsertPromise;
void validJsonInsertPromise;

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

db.selectFrom("typed_samples").where("status", "=", "active");

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
