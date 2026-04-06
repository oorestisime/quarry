import { describe, expect, it } from "vitest";
import { createClickHouseDB } from "../src";

interface QueryBuilderTestDB {
  event_logs: {
    user_id: number;
    event_type: string;
  };
  typed_samples: {
    nickname: string | null;
  };
}

const db = createClickHouseDB<QueryBuilderTestDB>();

describe("query builder validation", () => {
  it("rejects invalid limit values", () => {
    expect(() => db.selectFrom("event_logs").selectAll().limit(-1)).toThrow(
      "LIMIT must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().limit(Number.NaN)).toThrow(
      "LIMIT must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().limit(3.7)).toThrow(
      "LIMIT must be a non-negative integer.",
    );
  });

  it("rejects invalid offset values", () => {
    expect(() => db.selectFrom("event_logs").selectAll().offset(-1)).toThrow(
      "OFFSET must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().offset(Number.NaN)).toThrow(
      "OFFSET must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().offset(3.7)).toThrow(
      "OFFSET must be a non-negative integer.",
    );
  });

  it("gives a clearer final error for non-table sources", () => {
    const subquery = db.selectFrom("event_logs").selectAll().as("logs");

    expect(() => db.selectFrom(subquery).final()).toThrow(
      "FINAL can only be applied to table sources.",
    );
  });

  it("compiles whereNull and whereNotNull predicates", () => {
    const whereNullQuery = db.selectFrom("typed_samples").selectAll().whereNull("nickname").toSQL();
    const whereNotNullQuery = db
      .selectFrom("typed_samples")
      .selectAll()
      .whereNotNull("nickname")
      .toSQL();

    expect(whereNullQuery.query).toBe("SELECT * FROM typed_samples WHERE nickname IS NULL");
    expect(whereNullQuery.params).toEqual({});
    expect(whereNotNullQuery.query).toBe("SELECT * FROM typed_samples WHERE nickname IS NOT NULL");
    expect(whereNotNullQuery.params).toEqual({});
  });

  it("rejects bare null predicate values at runtime", () => {
    expect(() =>
      db
        .selectFrom("typed_samples")
        .selectAll()
        .where("nickname" as never, "=", null as never),
    ).toThrow(
      'Bare null predicate values are not supported. Use whereNull()/whereNotNull() or param(null, "Nullable(...)").',
    );
  });
});
