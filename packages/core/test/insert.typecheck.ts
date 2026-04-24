import { type ClickHouseClient, type ClickHouseInsertResult, createClickHouseDB } from "../src";

interface InsertTypecheckDB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
    properties: string;
    event_date: string;
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

const db = createClickHouseDB<InsertTypecheckDB>();
const dbWithClient = createClickHouseDB<InsertTypecheckDB>({ client });

const executionOptions = {
  client,
  queryId: "typecheck-query-id",
  clickhouse_settings: {
    max_threads: 1,
    wait_end_of_query: true,
  },
};

const validInsertResultPromise: Promise<ClickHouseInsertResult> = dbWithClient
  .insertInto("users")
  .values([
    {
      id: 1,
      email: "alice@example.com",
      status: "active",
    },
  ])
  .execute(executionOptions);

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
  .execute(executionOptions);

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
  .execute(executionOptions);

const validInsertFromSelectPromise: Promise<ClickHouseInsertResult> = dbWithClient
  .insertInto("users")
  .columns("id", "email", "status")
  .fromSelect(
    dbWithClient
      .selectFrom("users as u")
      .selectExpr((eb) => ["u.id", "u.email", eb.val("active").as("status")]),
  )
  .execute(executionOptions);

void validInsertResultPromise;
void validTypedSamplesInsertPromise;
void validJsonInsertPromise;
void validInsertFromSelectPromise;

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
