import { describe, expect, it } from "vitest";
import { createClickHouseDB, param } from "../src";

interface InsertTestDB {
  event_logs: {
    user_id: number;
    created_at: string;
    amount: number;
  };
  daily_aggregates: {
    user_id: number;
    event_date: string;
    total_amount: number;
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
    expect(compiled.params).toEqual({});
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
    expect(compiled.params).toEqual({});
    expect(compiled.values).toHaveLength(2);
    expect(compiled.values?.[0]?.id).toBe(4);
    expect(compiled.values?.[1]?.id).toBe(5);
  });

  it("compiles insert into select queries with target columns and params", () => {
    const compiled = db
      .insertInto("daily_aggregates")
      .columns("user_id", "event_date", "total_amount")
      .fromSelect(
        db
          .selectFrom("event_logs as e")
          .selectExpr((eb) => [
            "e.user_id",
            eb.fn.toDate("e.created_at").as("event_date"),
            eb.fn.sum("e.amount").as("total_amount"),
          ])
          .where("e.created_at", ">=", param("2025-01-01", "Date"))
          .groupBy("e.user_id", (eb) => eb.fn.toDate("e.created_at")),
      )
      .toSQL();

    expect(compiled.query).toBe(
      "INSERT INTO daily_aggregates (user_id, event_date, total_amount) SELECT e.user_id, toDate(e.created_at) AS event_date, sum(e.amount) AS total_amount FROM event_logs AS e WHERE e.created_at >= {p0:Date} GROUP BY e.user_id, toDate(e.created_at)",
    );
    expect(compiled.params).toEqual({ p0: "2025-01-01" });
    expect(compiled.values).toBeUndefined();
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
    ).toThrow("Insert source has already been set for this query.");
  });

  it("rejects switching from values to fromSelect", () => {
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
      query.fromSelect(
        db.selectFrom("event_logs").selectExpr((eb) => [eb.fn.sum("amount").as("amount")]),
      ),
    ).toThrow("Insert source has already been set for this query.");
  });

  it("rejects compiling inserts without a source", () => {
    expect(() => db.insertInto("typed_samples").toSQL()).toThrow(
      "Cannot compile an insert without a source",
    );
  });
});
