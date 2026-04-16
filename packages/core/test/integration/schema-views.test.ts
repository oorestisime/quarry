import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  Date as CHDate,
  DateTime64,
  String as CHString,
  UInt32,
  createClickHouseDB,
  defineSchema,
  table,
  view,
} from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

const schema = defineSchema({
  schema_view_users: table({
    id: UInt32(),
    email: CHString(),
    signup_date: CHDate(),
    created_at: DateTime64(3),
  }),
}).views((db) => ({
  schema_view_all_users: view.as(db.selectFrom("schema_view_users as u").selectAll("u")),
  schema_view_daily_users: view.as(
    db
      .selectFrom("schema_view_users as u")
      .selectExpr((eb) => ["u.signup_date", eb.fn.count().as("total_users")])
      .groupBy("u.signup_date"),
  ),
  schema_view_formatted_users: view.as(
    db
      .selectFrom("schema_view_users as u")
      .selectExpr((eb) => [
        "u.id",
        eb.fn.toString("u.id").as("id_text"),
        eb.fn.lower("u.email").as("email_lower"),
        eb.fn.formatDateTime("u.created_at", "%Y-%m-%d").as("created_date_text"),
        eb.fn.toYYYYMM("u.created_at").as("created_yyyymm"),
      ]),
  ),
}));

const db = createClickHouseDB({ schema });

let context: ClickHouseTestContext | undefined;

describe("schema query-backed views integration", () => {
  function getContext(): ClickHouseTestContext {
    if (!context) {
      throw new Error("ClickHouse test context was not initialized");
    }

    return context;
  }

  beforeAll(async () => {
    context = await startClickHouse();

    await getContext().client.command({ query: "DROP VIEW IF EXISTS schema_view_all_users" });
    await getContext().client.command({ query: "DROP VIEW IF EXISTS schema_view_daily_users" });
    await getContext().client.command({ query: "DROP VIEW IF EXISTS schema_view_formatted_users" });
    await getContext().client.command({ query: "DROP TABLE IF EXISTS schema_view_users" });

    await getContext().client.command({
      query: `
        CREATE TABLE schema_view_users (
          id UInt32,
          email String,
          signup_date Date,
          created_at DateTime64(3)
        )
        ENGINE = Memory
      `,
    });

    await getContext().client.command({
      query: `
        CREATE VIEW schema_view_all_users AS
        SELECT *
        FROM schema_view_users
      `,
    });

    await getContext().client.command({
      query: `
        CREATE VIEW schema_view_daily_users AS
        SELECT
          signup_date,
          count() AS total_users
        FROM schema_view_users
        GROUP BY signup_date
      `,
    });

    await getContext().client.command({
      query: `
        CREATE VIEW schema_view_formatted_users AS
        SELECT
          id,
          toString(id) AS id_text,
          lower(email) AS email_lower,
          formatDateTime(created_at, '%Y-%m-%d') AS created_date_text,
          toYYYYMM(created_at) AS created_yyyymm
        FROM schema_view_users
      `,
    });
  });

  beforeEach(async () => {
    await getContext().client.command({ query: "TRUNCATE TABLE IF EXISTS schema_view_users" });

    await getContext().client.insert({
      table: "schema_view_users",
      format: "JSONEachRow",
      values: [
        {
          id: 1,
          email: "alice@example.com",
          signup_date: "2025-01-01",
          created_at: "2025-01-01 10:11:12.123",
        },
        {
          id: 2,
          email: "Bruno@Example.com",
          signup_date: "2025-01-01",
          created_at: "2025-01-02 10:11:12.123",
        },
        {
          id: 3,
          email: "cory@example.com",
          signup_date: "2025-01-02",
          created_at: "2025-01-03 10:11:12.123",
        },
      ],
    });
  });

  afterAll(async () => {
    await stopClickHouse(context);
  });

  it("queries select-all views with inherited output columns", async () => {
    const rows = await db
      .selectFrom("schema_view_all_users as v")
      .selectAll()
      .orderBy("v.id", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        id: 1,
        email: "alice@example.com",
        signup_date: "2025-01-01",
        created_at: "2025-01-01 10:11:12.123",
      },
      {
        id: 2,
        email: "Bruno@Example.com",
        signup_date: "2025-01-01",
        created_at: "2025-01-02 10:11:12.123",
      },
      {
        id: 3,
        email: "cory@example.com",
        signup_date: "2025-01-02",
        created_at: "2025-01-03 10:11:12.123",
      },
    ]);
  });

  it("queries projected views with only the selected output columns", async () => {
    const rows = await db
      .selectFrom("schema_view_daily_users as d")
      .selectAll()
      .orderBy("d.signup_date", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        signup_date: "2025-01-01",
        total_users: "2",
      },
      {
        signup_date: "2025-01-02",
        total_users: "1",
      },
    ]);
  });

  it("queries formatted views backed by helper expressions", async () => {
    const rows = await db
      .selectFrom("schema_view_formatted_users as f")
      .selectAll()
      .orderBy("f.id", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      {
        id: 1,
        id_text: "1",
        email_lower: "alice@example.com",
        created_date_text: "2025-01-01",
        created_yyyymm: 202501,
      },
      {
        id: 2,
        id_text: "2",
        email_lower: "bruno@example.com",
        created_date_text: "2025-01-02",
        created_yyyymm: 202501,
      },
      {
        id: 3,
        id_text: "3",
        email_lower: "cory@example.com",
        created_date_text: "2025-01-03",
        created_yyyymm: 202501,
      },
    ]);
  });
});
