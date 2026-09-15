import { describe, expect, it, vi } from "vitest";
import { createClickHouseDB, ExpressionBuilder, param, type ClickHouseDateTime64 } from "../src";

describe("empty predicate group regressions", () => {
  // Finding 8: JavaScript callers need an immediate error as well as the static checks.
  it.each(["and", "or"] as const)("rejects an empty %s group when it is built", (operator) => {
    const eb = new ExpressionBuilder<{}>();

    // Reflect.apply bypasses the TypeScript contract to exercise the JavaScript boundary.
    expect(() => Reflect.apply(eb[operator], eb, [[]])).toThrow(/at least one condition/i);
  });
});

describe("Date parameter regressions", () => {
  const db = createClickHouseDB<{ events: { at: ClickHouseDateTime64 } }>();
  const date = new Date("2025-01-01T10:11:12.123Z");

  it("retains milliseconds and UTC while honoring an explicit seconds-only type", () => {
    const query = db.selectFrom("events").selectAll();
    expect(query.where("at", "=", date).toSQL()).toEqual({
      query: "SELECT * FROM events WHERE at = {p0:DateTime64(3, 'UTC')}",
      params: { p0: "2025-01-01 10:11:12.123" },
    });
    expect(query.where("at", "=", param(date, "DateTime")).toSQL()).toEqual({
      query: "SELECT * FROM events WHERE at = {p0:DateTime}",
      params: { p0: "2025-01-01 10:11:12" },
    });
  });

  it("preserves an explicit nullable cast type through a subquery predicate", () => {
    const cast = db
      .selectFrom("events")
      .selectExpr((eb) => [
        eb.fn.toDateTime64(eb.val(param<string | null>(null, "Nullable(String)")), 3).as("cast_at"),
      ]);

    expect(db.selectFrom(cast.as("c")).selectAll().where("cast_at", "=", date).toSQL()).toEqual({
      query:
        "SELECT * FROM (SELECT toDateTime64({p0:Nullable(String)}, 3) AS cast_at FROM events) AS c WHERE cast_at = {p1:Nullable(DateTime64(3))}",
      params: { p0: null, p1: "2025-01-01 10:11:12.123" },
    });
  });
});

describe("execution setting regressions", () => {
  const db = createClickHouseDB<{ source: { id: number }; target: { id: number } }>();

  it("rejects a conflicting INSERT SELECT join policy before calling the client", async () => {
    const client = { query: vi.fn(), command: vi.fn() };

    const query = db
      .insertInto("target")
      .columns("id")
      .fromSelect(db.selectFrom("source").select("id"));

    await expect(
      query.execute({ client, clickhouse_settings: { join_use_nulls: 1 } }),
    ).rejects.toThrow("join_use_nulls");
    expect(client.command).not.toHaveBeenCalled();
  });

  it("rejects settings that would serialize an empty average as a string", async () => {
    const client = { query: vi.fn() };
    const query = db.selectFrom("source").selectExpr((eb) => [eb.fn.avg("id").as("average")]);
    expect(() => query.settings({ output_format_json_quote_denormals: 1 }).toSQL()).toThrow(
      "output_format_json_quote_denormals",
    );
    await expect(
      query.execute({ client, clickhouse_settings: { output_format_json_quote_denormals: 1 } }),
    ).rejects.toThrow("output_format_json_quote_denormals");
    expect(client.query).not.toHaveBeenCalled();
  });
});
