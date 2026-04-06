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

function buildJsonSamplesQuery() {
  return db.selectFrom("json_samples").selectAll().orderBy("id", "asc");
}

type TypedSampleRow = InferResult<ReturnType<typeof buildTypedSamplesQuery>>;
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
});
