import { describe, expect, it, vi } from "vitest";
import { createClickHouseDB, identifier, param, sql } from "../src";

interface DB {
  users: { id: number; email: string };
  events: { user_id: number; event_type: string };
}
const db = createClickHouseDB<DB>();

describe("composable SQL", () => {
  it("binds hostile values and merges nested fragment parameters with builder parameters", () => {
    const value = "'); DROP TABLE users; --";
    const predicate = sql`${identifier("u", "email")} = ${value}`;
    const compiled = db
      .selectFrom("users as u")
      .select(sql<string>`concat(${identifier("u", "email")}, ${"!"})`.as("label"))
      .where(predicate)
      .where("u.id", "=", param(42, "UInt32"))
      .toSQL();
    expect(compiled).toEqual({
      query:
        "SELECT concat(u.email, {p0:String}) AS label FROM users AS u WHERE (u.email = {p1:String}) AND u.id = {p2:UInt32}",
      params: { p0: "!", p1: value, p2: 42 },
    });
  });
  it("quotes literal identifier segments, including dots and backticks", () => {
    const compiled = sql`${identifier("a", "a.b")} = ${identifier("a", "back`tick")}`;
    expect(db.selectFrom("users").select(compiled.as("select")).toSQL().query).toBe(
      "SELECT a.`a.b` = a.`back\\`tick` AS `select` FROM users",
    );
    const odd = createClickHouseDB<{ "order details": { select: string } }>();
    expect(odd.selectFrom("order details").select("select").toSQL().query).toBe(
      "SELECT `select` FROM `order details`",
    );
  });
  it("requires explicit nullable parameter types", () => {
    expect(() => sql`${null}`).toThrow("Nullable");
    expect(() => sql`${undefined}`).toThrow("undefined");
    expect(
      db
        .selectFrom("users")
        .select(sql<string | null>`${param(null, "Nullable(String)")}`.as("value"))
        .toSQL().params,
    ).toEqual({ p0: null });
  });
});

describe("query contracts", () => {
  it("rejects incompatible settings on queries and execution options before calling the client", async () => {
    const client = { query: vi.fn() };
    expect(() =>
      db.selectFrom("users").select("id").settings({ join_use_nulls: 1 }).toSQL(),
    ).toThrow("leftJoinNullable");
    const query = db.selectFrom("users").selectExpr((eb) => [eb.fn.count().as("n")]);
    expect(() => query.settings({ output_format_json_quote_64bit_integers: 0 }).toSQL()).toThrow(
      "result types",
    );
    await expect(
      query.execute({
        client,
        clickhouse_settings: { output_format_json_quote_64bit_integers: false },
      }),
    ).rejects.toThrow("result types");
    expect(client.query).not.toHaveBeenCalled();
  });
  it("rejects mixed outer-join policies across subqueries", () => {
    const nullable = db
      .selectFrom("users as u")
      .leftJoinNullable("events as e", "u.id", "e.user_id")
      .select("u.id");
    const mixed = db
      .selectFrom(nullable.as("n"))
      .leftJoin("events as e", "n.id", "e.user_id")
      .select("n.id");
    expect(() => mixed.toSQL()).toThrow("one outer-join null policy");
  });
  it("checks INSERT SELECT arity for untyped callers", () => {
    const insert = db.insertInto("events").columns("user_id", "event_type");
    // @ts-expect-error runtime boundary also rejects invalid JavaScript callers
    expect(() => insert.fromSelect(db.selectFrom("users").select("id"))).toThrow("column count");
  });
  it("merges UNION ALL parameters and applies subsequent ordering to the combined result", () => {
    const first = db.selectFrom("users").select("id").where("id", "=", 1);
    const second = db.selectFrom("events").select("user_id").where("user_id", "=", 2);
    const compiled = first.unionAll(second).orderBy("id", "desc").limit(1).toSQL();
    expect(compiled.query).toBe(
      "SELECT _quarry_union.* FROM ((SELECT id FROM users WHERE id = {p0:Int64}) UNION ALL (SELECT user_id FROM events WHERE user_id = {p1:Int64})) AS _quarry_union ORDER BY id DESC LIMIT 1",
    );
    expect(compiled.params).toEqual({ p0: 1, p1: 2 });
    expect(first.toSQL().params).toEqual({ p0: 1 });
  });
  it("cancels retry backoff promptly and avoids another request", async () => {
    const controller = new AbortController();
    const error = Object.assign(new Error("reset"), { code: "ECONNRESET" });
    const client = { query: vi.fn().mockRejectedValue(error) };
    const promise = createClickHouseDB<DB>({ client, retries: { attempts: 3, delayMs: 60_000 } })
      .selectFrom("users")
      .select("id")
      .execute({ abortSignal: controller.signal });
    await vi.waitFor(() => expect(client.query).toHaveBeenCalledTimes(1));
    controller.abort(new Error("cancelled"));
    await expect(promise).rejects.toThrow("cancelled");
    expect(client.query).toHaveBeenCalledTimes(1);
    expect(client.query.mock.calls[0][0].abort_signal).toBe(controller.signal);
  });
  it("does not issue a request for an already aborted operation", async () => {
    const client = { query: vi.fn() };
    await expect(
      db
        .selectFrom("users")
        .select("id")
        .execute({ client, abortSignal: AbortSignal.abort(new Error("cancelled")) }),
    ).rejects.toThrow("cancelled");
    expect(client.query).not.toHaveBeenCalled();
  });
  it("validates window frames", () => {
    expect(() => sql<number>`sum(1)`.over({ rows: { start: 1, end: -1 } })).toThrow("start");
    expect(() => sql<number>`sum(1)`.over({ rows: { start: 0.5, end: 0 } })).toThrow(
      "safe integers",
    );
    expect(() =>
      // @ts-expect-error JavaScript callers must not inject a frame clause
      sql<number>`sum(1)`.over({ rows: { start: "current row); DROP TABLE users", end: 0 } }),
    ).toThrow("Invalid window frame start");
  });
});
