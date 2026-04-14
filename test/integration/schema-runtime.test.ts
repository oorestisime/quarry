import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  Array,
  Date as CHDate,
  DateTime,
  DateTime64,
  Nullable,
  String,
  UInt32,
  UInt64,
  createClickHouseDB,
  defineSchema,
  param,
  table,
} from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

const schema = defineSchema({
  schema_runtime_samples: table({
    id: UInt32(),
    event_date: CHDate(),
    event_time: DateTime(),
    created_at: DateTime64(3),
    total_count: UInt64(),
    note: Nullable(String()),
    event_dates: Array(CHDate()),
    created_history: Array(DateTime64(3)),
  }),
});

const db = createClickHouseDB({ schema });

let context: ClickHouseTestContext | undefined;

describe("schema runtime integration", () => {
  function getContext(): ClickHouseTestContext {
    if (!context) {
      throw new Error("ClickHouse test context was not initialized");
    }

    return context;
  }

  beforeAll(async () => {
    context = await startClickHouse();

    await getContext().client.command({
      query: `
        CREATE TABLE schema_runtime_samples (
          id UInt32,
          event_date Date,
          event_time DateTime,
          created_at DateTime64(3),
          total_count UInt64,
          note Nullable(String),
          event_dates Array(Date),
          created_history Array(DateTime64(3))
        )
        ENGINE = Memory
      `,
    });
  });

  beforeEach(async () => {
    await getContext().client.command({ query: "TRUNCATE TABLE IF EXISTS schema_runtime_samples" });
  });

  afterAll(async () => {
    await stopClickHouse(context);
  });

  it("inserts schema-mode Date values and reads runtime-shaped rows back", async () => {
    const insertResult = await db
      .insertInto("schema_runtime_samples")
      .values([
        {
          id: 1,
          event_date: new Date("2025-01-04T00:00:00.000Z"),
          event_time: new Date("2025-01-04T08:09:10.000Z"),
          created_at: new Date("2025-01-04T12:34:56.789Z"),
          total_count: 9007199254740993n,
          note: null,
          event_dates: [new Date("2025-01-03T00:00:00.000Z"), new Date("2025-01-04T00:00:00.000Z")],
          created_history: [
            new Date("2025-01-04T12:34:56.789Z"),
            new Date("2025-01-04T12:35:56.789Z"),
          ],
        },
      ])
      .execute(getContext().client);

    expect(insertResult.executed).toBe(true);

    const rows = await db
      .selectFrom("schema_runtime_samples")
      .selectAll()
      .where("id", "=", 1)
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        id: 1,
        event_date: "2025-01-04",
        event_time: "2025-01-04 08:09:10",
        created_at: "2025-01-04 12:34:56.789",
        total_count: "9007199254740993",
        note: null,
        event_dates: ["2025-01-03", "2025-01-04"],
        created_history: ["2025-01-04 12:34:56.789", "2025-01-04 12:35:56.789"],
      },
    ]);
  });

  it("filters schema-mode date columns with bare Date values inferred from schema", async () => {
    await db
      .insertInto("schema_runtime_samples")
      .values([
        {
          id: 1,
          event_date: new Date("2025-01-03T00:00:00.000Z"),
          event_time: new Date("2025-01-03T09:00:00.000Z"),
          created_at: new Date("2025-01-03T09:00:00.000Z"),
          total_count: 1n,
          note: "older",
          event_dates: [new Date("2025-01-03T00:00:00.000Z")],
          created_history: [new Date("2025-01-03T09:00:00.000Z")],
        },
        {
          id: 2,
          event_date: new Date("2025-01-04T00:00:00.000Z"),
          event_time: new Date("2025-01-04T12:34:56.000Z"),
          created_at: new Date("2025-01-04T12:34:56.789Z"),
          total_count: 9007199254740993n,
          note: "matching",
          event_dates: [new Date("2025-01-04T00:00:00.000Z")],
          created_history: [new Date("2025-01-04T12:34:56.789Z")],
        },
      ])
      .execute(getContext().client);

    const rows = await db
      .selectFrom("schema_runtime_samples")
      .selectAll()
      .where("event_date", ">=", new Date("2025-01-04T00:00:00.000Z"))
      .where("event_time", ">=", new Date("2025-01-04T12:34:56.000Z"))
      .where("created_at", ">=", new Date("2025-01-04T12:34:56.789Z"))
      .where("total_count", "=", "9007199254740993")
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        id: 2,
        event_date: "2025-01-04",
        event_time: "2025-01-04 12:34:56",
        created_at: "2025-01-04 12:34:56.789",
        total_count: "9007199254740993",
        note: "matching",
        event_dates: ["2025-01-04"],
        created_history: ["2025-01-04 12:34:56.789"],
      },
    ]);
  });

  it("supports explicit typed params and already-safe string inputs in schema mode", async () => {
    await db
      .insertInto("schema_runtime_samples")
      .values([
        {
          id: 3,
          event_date: "2025-01-05",
          event_time: "2025-01-05 10:11:12",
          created_at: "2025-01-05 10:11:12.345",
          total_count: "7",
          note: "string-input",
          event_dates: ["2025-01-05", "2025-01-06"],
          created_history: ["2025-01-05 10:11:12.345"],
        },
      ])
      .execute(getContext().client);

    const rows = await db
      .selectFrom("schema_runtime_samples")
      .selectAll()
      .where("event_date", "=", param(new Date("2025-01-05T00:00:00.000Z"), "Date"))
      .where("created_at", "=", param(new Date("2025-01-05T10:11:12.345Z"), "DateTime64(3)"))
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        id: 3,
        event_date: "2025-01-05",
        event_time: "2025-01-05 10:11:12",
        created_at: "2025-01-05 10:11:12.345",
        total_count: "7",
        note: "string-input",
        event_dates: ["2025-01-05", "2025-01-06"],
        created_history: ["2025-01-05 10:11:12.345"],
      },
    ]);
  });
});
