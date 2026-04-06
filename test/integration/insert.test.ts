import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClickHouseDB } from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface InsertDB {
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
      .execute(getContext().client);

    expect(insertResult.executed).toBe(true);

    const rows = await db
      .selectFrom("typed_samples")
      .selectAll()
      .where("id", "=", 3)
      .execute(getContext().client);

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
      .execute(getContext().client);

    expect(insertResult.executed).toBe(true);

    const rows = await db
      .selectFrom("json_samples")
      .selectAll()
      .where("id", "=", 3)
      .execute(getContext().client);

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
});
