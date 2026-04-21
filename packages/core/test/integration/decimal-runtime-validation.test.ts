import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClickHouseDB } from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface DecimalRuntimeDB {
  decimal_runtime_samples: {
    id: number;
    amount_d18: number;
    amount_d38: number;
  };
}

const db = createClickHouseDB<DecimalRuntimeDB>();

let context: ClickHouseTestContext | undefined;

describe("decimal runtime validation", () => {
  function getContext(): ClickHouseTestContext {
    if (!context) {
      throw new Error("ClickHouse test context was not initialized");
    }

    return context;
  }

  beforeAll(async () => {
    context = await startClickHouse();

    await getContext().client.command({ query: "DROP TABLE IF EXISTS decimal_runtime_samples" });
    await getContext().client.command({
      query: `
        CREATE TABLE decimal_runtime_samples (
          id UInt32,
          amount_d18 Decimal(18, 2),
          amount_d38 Decimal(38, 4)
        )
        ENGINE = Memory
      `,
    });
  });

  beforeEach(async () => {
    await getContext().client.command({
      query: "TRUNCATE TABLE IF EXISTS decimal_runtime_samples",
    });
  });

  afterAll(async () => {
    await stopClickHouse(context);
  });

  it("returns moderate Decimal columns as numbers through JSONEachRow", async () => {
    await getContext().client.insert({
      table: "decimal_runtime_samples",
      format: "JSONEachRow",
      values: [
        {
          id: 1,
          amount_d18: 123.45,
          amount_d38: 9876.5432,
        },
      ],
    });

    const rows = await db
      .selectFrom("decimal_runtime_samples as d")
      .selectAll()
      .orderBy("d.id", "asc")
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      {
        id: 1,
        amount_d18: 123.45,
        amount_d38: 9876.5432,
      },
    ]);

    expect(typeof rows[0].amount_d18).toBe("number");
    expect(typeof rows[0].amount_d38).toBe("number");
  });
});
