import { describe, expect, it } from "vitest";
import {
  Date as CHDate,
  DateTime64,
  String as CHString,
  UInt32,
  UInt64,
  createClickHouseDB,
  defineSchema,
  param,
  table,
  view,
} from "../src";

const schema = defineSchema({
  users: table.replacingMergeTree({
    id: UInt32(),
    email: CHString(),
    created_at: DateTime64(3),
    signup_date: CHDate(),
  }),
  final_users: view.from("users"),
  daily_users: view({
    signup_date: CHDate(),
    total_users: UInt64(),
  }),
});

const db = createClickHouseDB({ schema });

describe("schema-first mode", () => {
  it("compiles queries against schema-defined tables and inherited views", () => {
    const tableQuery = db
      .selectFrom("users as u")
      .select("u.id", "u.created_at")
      .where("u.created_at", ">=", param(new Date("2025-01-01T00:00:00.000Z"), "DateTime64(3)"))
      .orderBy("u.id", "asc")
      .toSQL();

    const inheritedViewQuery = db
      .selectFrom("final_users as f")
      .select("f.id", "f.email", "f.created_at")
      .where("f.signup_date", ">=", param(new Date("2025-01-01T00:00:00.000Z"), "Date"))
      .orderBy("f.id", "asc")
      .toSQL();

    expect(tableQuery.query).toBe(
      "SELECT u.id, u.created_at FROM users AS u WHERE u.created_at >= {p0:DateTime64(3)} ORDER BY u.id ASC",
    );
    expect(tableQuery.params).toEqual({
      p0: "2025-01-01 00:00:00.000",
    });

    expect(inheritedViewQuery.query).toBe(
      "SELECT f.id, f.email, f.created_at FROM final_users AS f WHERE f.signup_date >= {p0:Date} ORDER BY f.id ASC",
    );
    expect(inheritedViewQuery.params).toEqual({
      p0: "2025-01-01",
    });
  });

  it("allows FINAL for final-capable tables and rejects it for views", () => {
    const finalTableQuery = db.selectFrom(db.table("users").final().as("u")).select("u.id").toSQL();

    expect(finalTableQuery.query).toBe("SELECT u.id FROM users AS u FINAL");
    expect(() => db.selectFrom("final_users as f").select("f.id").final()).toThrow(
      "FINAL is not supported for source 'final_users'.",
    );
  });
});
