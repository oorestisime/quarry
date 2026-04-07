import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClickHouseDB, type InferResult } from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface TypedSamplesDB {
  json_samples: {
    id: number;
    payload: {
      user: {
        id: string;
      };
      traits: {
        plan: string;
        active: boolean;
      };
      tags: string[];
      metrics: {
        score: number;
        rank: number;
      };
    };
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
}

const db = createClickHouseDB<TypedSamplesDB>();

function buildTypedSamplesQuery() {
  return db.selectFrom("typed_samples").selectAll().orderBy("id", "asc");
}

function buildTypeCastQuery() {
  return db
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
    .orderBy("t.id", "asc");
}

function buildArrayFunctionQuery() {
  return db
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
    .orderBy("t.id", "asc");
}

function buildJsonSamplesQuery() {
  return db.selectFrom("json_samples").selectAll().orderBy("id", "asc");
}

type TypedSampleRow = InferResult<ReturnType<typeof buildTypedSamplesQuery>>;
type TypeCastRow = InferResult<ReturnType<typeof buildTypeCastQuery>>;
type ArrayFunctionRow = InferResult<ReturnType<typeof buildArrayFunctionQuery>>;
type JsonSampleRow = InferResult<ReturnType<typeof buildJsonSamplesQuery>>;

const expectedRows: TypedSampleRow[] = [
  {
    id: 1,
    big_user_id: "9007199254740993",
    label: "alpha",
    status: "active",
    nickname: null,
    tags: ["new", "trial"],
    amount: 123.45,
    created_at: "2025-01-01 10:11:12.123",
    location: [1.25, 9.5],
    attributes: { source: "ads", campaign: "winter" },
    "metrics.name": ["clicks", "opens"],
    "metrics.score": [10, 5],
  },
  {
    id: 2,
    big_user_id: "42",
    label: "beta",
    status: "pending",
    nickname: "bee",
    tags: [],
    amount: 0.1,
    created_at: "2025-01-02 03:04:05.678",
    location: [0, 1.5],
    attributes: { source: "email" },
    "metrics.name": ["views"],
    "metrics.score": [99],
  },
];

const expectedJsonRows: JsonSampleRow[] = [
  {
    id: 1,
    payload: {
      user: { id: "9007199254740993" },
      traits: { plan: "pro", active: true },
      tags: ["new", "trial"],
      metrics: { score: 12.5, rank: 3 },
    },
  },
  {
    id: 2,
    payload: {
      user: { id: "42" },
      traits: { plan: "free", active: false },
      tags: [],
      metrics: { score: 0.25, rank: 8 },
    },
  },
];

const expectedTypeCastRows: TypeCastRow[] = [
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
];

const expectedArrayFunctionRows: ArrayFunctionRow[] = [
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
];

let context: ClickHouseTestContext | undefined;

describe("clickhouse runtime types", () => {
  function getContext(): ClickHouseTestContext {
    if (!context) {
      throw new Error("ClickHouse test context was not initialized");
    }

    return context;
  }

  beforeAll(async () => {
    context = await startClickHouse();
  });

  afterAll(async () => {
    await stopClickHouse(context);
  });

  it("returns raw JSONEachRow-compatible values for awkward ClickHouse types", async () => {
    const rows = await buildTypedSamplesQuery().execute(getContext().client);

    expect(rows).toEqual(expectedRows);
    expect(typeof rows[0].id).toBe("number");
    expect(typeof rows[0].big_user_id).toBe("string");
    expect(typeof rows[0].label).toBe("string");
    expect(typeof rows[0].status).toBe("string");
    expect(rows[0].nickname).toBeNull();
    expect(Array.isArray(rows[0].tags)).toBe(true);
    expect(typeof rows[0].amount).toBe("number");
    expect(typeof rows[0].created_at).toBe("string");
    expect(Array.isArray(rows[0].location)).toBe(true);
    expect(rows[0].attributes).toEqual({ source: "ads", campaign: "winter" });
    expect(rows[0]["metrics.name"]).toEqual(["clicks", "opens"]);
    expect(rows[0]["metrics.score"]).toEqual([10, 5]);
  });

  it("returns structured objects for JSON columns with typed paths", async () => {
    const rows = await buildJsonSamplesQuery().execute(getContext().client);

    expect(rows).toEqual(expectedJsonRows);
    expect(typeof rows[0].payload.user.id).toBe("string");
    expect(typeof rows[0].payload.traits.plan).toBe("string");
    expect(typeof rows[0].payload.traits.active).toBe("boolean");
    expect(Array.isArray(rows[0].payload.tags)).toBe(true);
    expect(typeof rows[0].payload.metrics.score).toBe("number");
    expect(typeof rows[0].payload.metrics.rank).toBe("number");
  });

  it("returns runtime-honest values for type conversion helpers", async () => {
    const rows = await buildTypeCastQuery().execute(getContext().client);

    expect(rows).toEqual(expectedTypeCastRows);
    expect(typeof rows[0].id_i32).toBe("number");
    expect(typeof rows[0].id_i64).toBe("string");
    expect(typeof rows[0].id_u32).toBe("number");
    expect(typeof rows[0].big_user_id_u64).toBe("string");
    expect(typeof rows[0].id_f32).toBe("number");
    expect(typeof rows[0].amount_f64).toBe("number");
    expect(typeof rows[0].created_date).toBe("string");
    expect(typeof rows[0].created_at_dt).toBe("string");
    expect(typeof rows[0].created_at_dt64).toBe("string");
    expect(typeof rows[0].id_text).toBe("string");
    expect(typeof rows[0].amount_d64).toBe("number");
    expect(typeof rows[0].amount_d128).toBe("number");
  });

  it("returns runtime-honest values for array function helpers", async () => {
    const rows = await buildArrayFunctionQuery().execute(getContext().client);

    expect(rows).toEqual(expectedArrayFunctionRows);
    expect(typeof rows[0].has_trial).toBe("number");
    expect(typeof rows[0].has_overlap).toBe("number");
    expect(typeof rows[0].has_required).toBe("number");
    expect(typeof rows[0].tag_count).toBe("string");
    expect(typeof rows[0].is_empty).toBe("number");
    expect(typeof rows[0].is_not_empty).toBe("number");
  });
});
