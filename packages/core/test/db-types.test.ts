import { describe, expect, it } from "vitest";
import {
  createClickHouseDB,
  param,
  type ClickHouseDate,
  type ClickHouseDateTime64,
  type ClickHouseUInt64,
  type ColumnType,
  type TypedTable,
  type TypedView,
} from "../src";

interface DbTypesTestDB {
  users: TypedTable<{
    id: number;
    created_at: ClickHouseDateTime64;
    big_user_id: ClickHouseUInt64;
    custom_metric: ColumnType<string, number, number>;
  }>;
  daily_users: TypedView<{
    signup_date: ClickHouseDate;
    total_users: ClickHouseUInt64;
  }>;
}

const db = createClickHouseDB<DbTypesTestDB>();

describe("db types helpers", () => {
  it("compiles queries against typed tables and typed views", () => {
    const query = db
      .selectFrom("users as u")
      .leftJoin("daily_users as d", "u.created_at", "d.signup_date")
      .select("u.id", "u.created_at", "u.big_user_id", "d.total_users")
      .where("u.custom_metric", "=", 7)
      .where("u.created_at", ">=", param(new Date("2025-01-01T00:00:00.000Z"), "DateTime64(3)"))
      .orderBy("u.id", "asc")
      .toSQL();

    expect(query.query).toBe(
      "SELECT u.id, u.created_at, u.big_user_id, d.total_users FROM users AS u LEFT JOIN daily_users AS d ON u.created_at = d.signup_date WHERE u.custom_metric = {p0:Int64} AND u.created_at >= {p1:DateTime64(3)} ORDER BY u.id ASC",
    );
    expect(query.params).toEqual({
      p0: 7,
      p1: "2025-01-01 00:00:00.000",
    });
  });

  it("still builds explicit table sources for typed tables", () => {
    const query = db
      .selectFrom(db.table("users").as("u").final())
      .select("u.id", "u.big_user_id")
      .toSQL();

    expect(query.query).toBe("SELECT u.id, u.big_user_id FROM users AS u FINAL");
    expect(query.params).toEqual({});
  });

  it("compiles inserts for typed tables without changing runtime payloads", () => {
    const compiled = db
      .insertInto("users")
      .values([
        {
          id: 1,
          created_at: "2025-01-01 00:00:00.000",
          big_user_id: "42",
          custom_metric: 9,
        },
      ])
      .toSQL();

    expect(compiled.query).toBe("INSERT INTO users FORMAT JSONEachRow");
    expect(compiled.params).toEqual({});
    expect(compiled.values).toEqual([
      {
        id: 1,
        created_at: "2025-01-01 00:00:00.000",
        big_user_id: "42",
        custom_metric: 9,
      },
    ]);
  });
});
