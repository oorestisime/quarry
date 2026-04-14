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
} from "../src";

interface PlainParityDB {
  users: {
    id: number;
    email: string;
    created_at: string;
    signup_date: string;
    total_count: string;
  };
}

const plainDb = createClickHouseDB<PlainParityDB>();

const schema = defineSchema({
  users: table.replacingMergeTree({
    id: UInt32(),
    email: CHString(),
    created_at: DateTime64(3),
    signup_date: CHDate(),
    total_count: UInt64(),
  }),
});

const schemaDb = createClickHouseDB({ schema });

describe("plain/schema parity", () => {
  it("compiles equivalent select queries identically", () => {
    const plainQuery = plainDb
      .selectFrom(plainDb.table("users").final().as("u"))
      .select("u.id", "u.email", "u.created_at")
      .where("u.created_at", ">=", param("2025-01-01 00:00:00.000", "DateTime64(3)"))
      .where("u.signup_date", ">=", param("2025-01-01", "Date"))
      .orderBy("u.id", "asc")
      .toSQL();

    const schemaQuery = schemaDb
      .selectFrom(schemaDb.table("users").final().as("u"))
      .select("u.id", "u.email", "u.created_at")
      .where("u.created_at", ">=", param("2025-01-01 00:00:00.000", "DateTime64(3)"))
      .where("u.signup_date", ">=", param("2025-01-01", "Date"))
      .orderBy("u.id", "asc")
      .toSQL();

    expect(schemaQuery).toEqual(plainQuery);
  });

  it("compiles equivalent insert queries identically", () => {
    const rows = [
      {
        id: 1,
        email: "alice@example.com",
        created_at: "2025-01-01 00:00:00.000",
        signup_date: "2025-01-01",
        total_count: "42",
      },
    ] as const;

    const plainInsert = plainDb.insertInto("users").values(rows).toSQL();
    const schemaInsert = schemaDb.insertInto("users").values(rows).toSQL();

    expect(schemaInsert).toEqual(plainInsert);
  });
});
