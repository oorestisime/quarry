import { describe, expect, it } from "vitest";
import { createClickHouseDB } from "../src";

interface InsertTestDB {
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

const db = createClickHouseDB<InsertTestDB>();

describe("insert builder", () => {
  it("compiles single-row inserts to JSONEachRow", () => {
    const compiled = db
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
      .toSQL();

    expect(compiled.query).toBe("INSERT INTO typed_samples FORMAT JSONEachRow");
    expect(compiled.values).toEqual([
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
    ]);
  });

  it("compiles multi-row inserts to JSONEachRow", () => {
    const compiled = db
      .insertInto("typed_samples")
      .values([
        {
          id: 4,
          big_user_id: "100",
          label: "delta",
          status: "active",
          nickname: "dee",
          tags: [],
          amount: 10,
          created_at: "2025-01-04 00:00:00.000",
          location: [0, 0],
          attributes: { source: "email" },
          "metrics.name": ["opens"],
          "metrics.score": [11],
        },
        {
          id: 5,
          big_user_id: "101",
          label: "epsilon",
          status: "pending",
          nickname: null,
          tags: ["new"],
          amount: 11.5,
          created_at: "2025-01-05 00:00:00.000",
          location: [1, 1],
          attributes: { source: "ads" },
          "metrics.name": ["views"],
          "metrics.score": [12],
        },
      ])
      .toSQL();

    expect(compiled.query).toBe("INSERT INTO typed_samples FORMAT JSONEachRow");
    expect(compiled.values).toHaveLength(2);
    expect(compiled.values[0]?.id).toBe(4);
    expect(compiled.values[1]?.id).toBe(5);
  });

  it("rejects duplicate values calls", () => {
    const query = db.insertInto("typed_samples").values([
      {
        id: 6,
        big_user_id: "102",
        label: "zeta",
        status: "active",
        nickname: null,
        tags: [],
        amount: 1,
        created_at: "2025-01-06 00:00:00.000",
        location: [0, 0],
        attributes: {},
        "metrics.name": ["opens"],
        "metrics.score": [1],
      },
    ]);

    expect(() =>
      query.values([
        {
          id: 7,
          big_user_id: "103",
          label: "eta",
          status: "pending",
          nickname: null,
          tags: [],
          amount: 2,
          created_at: "2025-01-07 00:00:00.000",
          location: [1, 1],
          attributes: {},
          "metrics.name": ["views"],
          "metrics.score": [2],
        },
      ]),
    ).toThrow("values() can only be called once per insert query.");
  });
});
