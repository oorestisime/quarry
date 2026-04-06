import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

let context: ClickHouseTestContext | undefined;

describe("clickhouse client query param semantics", () => {
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

  it("accepts raw Date values in query params with an explicit DateTime placeholder", async () => {
    const result = await getContext().client.query({
      query:
        "SELECT user_id, created_at FROM inquiry_downloads WHERE created_at >= {from:DateTime} ORDER BY created_at ASC",
      query_params: {
        from: new Date("2025-01-02T00:00:00.000Z"),
      },
      format: "JSONEachRow",
    });

    const rows = await result.json<Array<{ user_id: number; created_at: string }>>();

    expect(rows).toEqual([
      { user_id: 1, created_at: "2025-01-02 08:00:00" },
      { user_id: 2, created_at: "2025-01-02 12:00:00" },
    ]);
  });
});
