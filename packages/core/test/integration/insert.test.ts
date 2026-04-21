import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClickHouseDB, param } from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface InsertDB {
  daily_aggregates: {
    user_id: number;
    event_date: string;
    event_count: string;
    total_amount: string;
  };
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
    event_date: string;
    properties: string;
    version: number;
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

const db = createClickHouseDB<InsertDB>();

let context: ClickHouseTestContext | undefined;

describe("clickhouse insert integration", () => {
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

  it("inserts runtime-shaped values for awkward ClickHouse types", async () => {
    const insertResult = await db
      .insertInto("typed_samples")
      .values([
        {
          id: 3,
          big_user_id: "1844674407370955161",
          label: "gamma",
          status: "archived",
          nickname: null,
          tags: ["vip"],
          amount: 98.76,
          created_at: "2025-01-03 00:00:00.001",
          location: [3.14, 2.72],
          attributes: { source: "partner", medium: "referral" },
          "metrics.name": ["purchases"],
          "metrics.score": [7],
        },
      ])
      .execute({ client: getContext().client });

    expect(insertResult.executed).toBe(true);

    const rows = await db
      .selectFrom("typed_samples")
      .selectAll()
      .where("id", "=", 3)
      .execute({ client: getContext().client });

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: 3,
      big_user_id: "1844674407370955161",
      label: "gamma",
      status: "archived",
      nickname: null,
      tags: ["vip"],
      amount: 98.76,
      created_at: "2025-01-03 00:00:00.001",
      attributes: { source: "partner", medium: "referral" },
      "metrics.name": ["purchases"],
      "metrics.score": [7],
    });
    expect(rows[0]?.location[0]).toBeCloseTo(3.14);
    expect(rows[0]?.location[1]).toBeCloseTo(2.72);
  });

  it("inserts nested objects into JSON columns", async () => {
    const insertResult = await db
      .insertInto("json_samples")
      .values([
        {
          id: 3,
          payload: {
            user: { id: "44" },
            traits: { plan: "enterprise", active: true },
            tags: ["engaged", "priority"],
            metrics: { score: 100.5, rank: 1 },
          },
        },
      ])
      .execute({ client: getContext().client });

    expect(insertResult.executed).toBe(true);

    const rows = await db
      .selectFrom("json_samples")
      .selectAll()
      .where("id", "=", 3)
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      {
        id: 3,
        payload: {
          user: { id: "44" },
          traits: { plan: "enterprise", active: true },
          tags: ["engaged", "priority"],
          metrics: { score: 100.5, rank: 1 },
        },
      },
    ]);
  });

  it("inserts transformed rows from a select query", async () => {
    await getContext().client.command({
      query: `
        CREATE TABLE daily_aggregates (
          user_id UInt32,
          event_date Date,
          event_count UInt64,
          total_amount UInt64
        )
        ENGINE = Memory
      `,
    });

    const insertResult = await db
      .insertInto("daily_aggregates")
      .columns("user_id", "event_date", "event_count", "total_amount")
      .fromSelect(
        db
          .selectFrom("event_logs as e")
          .selectExpr((eb) => [
            "e.user_id",
            eb.fn.toDate("e.created_at").as("event_date"),
            eb.fn.count().as("event_count"),
            eb.fn.sum(eb.val(1)).as("total_amount"),
          ])
          .where("e.created_at", ">=", param("2025-01-01", "Date"))
          .groupBy("e.user_id", (eb) => eb.fn.toDate("e.created_at")),
      )
      .execute({ client: getContext().client });

    expect(insertResult.executed).toBe(true);

    const rows = await db
      .selectFrom("daily_aggregates")
      .selectAll()
      .orderBy("user_id", "asc")
      .orderBy("event_date", "asc")
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      {
        user_id: 1,
        event_date: "2025-01-01",
        event_count: "1",
        total_amount: "1",
      },
      {
        user_id: 2,
        event_date: "2025-01-02",
        event_count: "1",
        total_amount: "1",
      },
      {
        user_id: 3,
        event_date: "2025-01-03",
        event_count: "1",
        total_amount: "1",
      },
    ]);
  });
});
