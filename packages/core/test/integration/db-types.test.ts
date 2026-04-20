import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  createClickHouseDB,
  type ClickHouseDate,
  type ClickHouseDate32,
  type ClickHouseDateTime,
  type ClickHouseDateTime64,
  type ClickHouseDecimal,
  type ClickHouseInt64,
  type ClickHouseUInt64,
  type InferResult,
  type TypedTable,
  type TypedView,
} from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface DbTypesIntegrationDB {
  typed_alias_users: TypedTable<{
    id: number;
    event_date: ClickHouseDate;
    event_date32: ClickHouseDate32;
    event_time: ClickHouseDateTime;
    created_at: ClickHouseDateTime64;
    amount: ClickHouseDecimal;
    signed_total: ClickHouseInt64;
    unsigned_total: ClickHouseUInt64;
  }>;
  typed_alias_daily_users: TypedView<{
    signup_date: ClickHouseDate;
    total_users: ClickHouseUInt64;
  }>;
}

const db = createClickHouseDB<DbTypesIntegrationDB>();

let context: ClickHouseTestContext | undefined;

describe("db types integration", () => {
  function getContext(): ClickHouseTestContext {
    if (!context) {
      throw new Error("ClickHouse test context was not initialized");
    }

    return context;
  }

  beforeAll(async () => {
    context = await startClickHouse();

    await getContext().client.command({ query: "DROP VIEW IF EXISTS typed_alias_daily_users" });
    await getContext().client.command({ query: "DROP TABLE IF EXISTS typed_alias_users" });

    await getContext().client.command({
      query: `
        CREATE TABLE typed_alias_users (
          id UInt32,
          event_date Date,
          event_date32 Date32,
          event_time DateTime,
          created_at DateTime64(3),
          amount Decimal(18, 2),
          signed_total Int64,
          unsigned_total UInt64
        )
        ENGINE = Memory
      `,
    });

    await getContext().client.command({
      query: `
        CREATE VIEW typed_alias_daily_users AS
        SELECT
          event_date AS signup_date,
          unsigned_total AS total_users
        FROM typed_alias_users
      `,
    });
  });

  beforeEach(async () => {
    await getContext().client.command({
      query: "TRUNCATE TABLE IF EXISTS typed_alias_users",
    });
  });

  afterAll(async () => {
    if (context) {
      await context.client.command({ query: "DROP VIEW IF EXISTS typed_alias_daily_users" });
      await context.client.command({ query: "DROP TABLE IF EXISTS typed_alias_users" });
    }

    await stopClickHouse(context);
  });

  it("round-trips typed table inserts through runtime-backed alias values", async () => {
    await db
      .insertInto("typed_alias_users")
      .values([
        {
          id: 1,
          event_date: "2025-01-01",
          event_date32: "2025-01-02",
          event_time: new Date("2025-01-03T12:34:56.000Z"),
          created_at: new Date("2025-01-04T12:34:56.789Z"),
          amount: "12.34",
          signed_total: -42n,
          unsigned_total: 42n,
        },
      ])
      .execute(getContext().client);

    const rows = await db
      .selectFrom("typed_alias_users as t")
      .selectAll()
      .orderBy("t.id", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        id: 1,
        event_date: "2025-01-01",
        event_date32: "2025-01-02",
        event_time: "2025-01-03 12:34:56",
        created_at: "2025-01-04 12:34:56.789",
        amount: 12.34,
        signed_total: "-42",
        unsigned_total: "42",
      },
    ]);
  });

  it("selects from typed views with the wrapped row shape", async () => {
    await getContext().client.insert({
      table: "typed_alias_users",
      format: "JSONEachRow",
      values: [
        {
          id: 1,
          event_date: "2025-01-01",
          event_date32: "2025-01-01",
          event_time: "2025-01-01 10:00:00",
          created_at: "2025-01-01 10:00:00.123",
          amount: "12.34",
          signed_total: "-42",
          unsigned_total: "42",
        },
      ],
    });

    const query = db
      .selectFrom("typed_alias_daily_users as d")
      .select("d.signup_date", "d.total_users")
      .orderBy("d.signup_date", "asc");

    type Row = InferResult<typeof query>;

    const rows = await query.execute(getContext().client);
    const expected: Row[] = [{ signup_date: "2025-01-01", total_users: "42" }];

    expect(rows).toEqual(expected);
  });

  it("normalizes Date arrays for typed DateTime and DateTime64 predicates", async () => {
    await getContext().client.insert({
      table: "typed_alias_users",
      format: "JSONEachRow",
      values: [
        {
          id: 1,
          event_date: "2025-01-01",
          event_date32: "2025-01-01",
          event_time: "2025-01-03 12:34:56",
          created_at: "2025-01-04 12:34:56.789",
          amount: "12.34",
          signed_total: "-42",
          unsigned_total: "42",
        },
        {
          id: 2,
          event_date: "2025-01-02",
          event_date32: "2025-01-02",
          event_time: "2025-01-06 00:00:00",
          created_at: "2025-01-05 12:34:56.123",
          amount: "56.78",
          signed_total: "-7",
          unsigned_total: "7",
        },
      ],
    });

    const rows = await db
      .selectFrom("typed_alias_users as t")
      .select("t.id", "t.event_time", "t.created_at")
      .where("t.event_time", "in", [new Date("2025-01-03T12:34:56.000Z")])
      .where("t.created_at", "not in", [new Date("2025-01-05T12:34:56.123Z")])
      .orderBy("t.id", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        id: 1,
        event_time: "2025-01-03 12:34:56",
        created_at: "2025-01-04 12:34:56.789",
      },
    ]);
  });
});
