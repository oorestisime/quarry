import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  createClickHouseDB,
  sql,
  type ClickHouseClient,
  type Generated,
  type GeneratedAlways,
} from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface DB {
  users: { id: number; email: string };
  event_logs: { user_id: number; event_type: string };
  "order details": { id: number; select: string; "a.b": string; "back`tick": string };
  generated_values: {
    id: number;
    label: Generated<string>;
    doubled: GeneratedAlways<number>;
    alias_id: GeneratedAlways<number>;
  };
}
let context: ClickHouseTestContext;
const db = createClickHouseDB<DB>();
describe("native composition and runtime contracts", () => {
  beforeAll(async () => {
    context = await startClickHouse();
  }, 120_000);
  afterAll(async () => {
    await stopClickHouse(context);
  });
  it("returns nullable right-side values and preserves defaults for ordinary left joins", async () => {
    const ordinary = db
      .selectFrom("users as u")
      .leftJoin("event_logs as e", "u.id", "e.user_id")
      .select("e.event_type")
      .where("u.id", "=", 43);
    const nullable = db
      .selectFrom("users as u")
      .leftJoinNullable("event_logs as e", "u.id", "e.user_id")
      .select("e.event_type")
      .where("u.id", "=", 43);
    expect(await ordinary.execute({ client: context.client })).toEqual([{ event_type: "" }]);
    expect(await nullable.execute({ client: context.client })).toEqual([{ event_type: null }]);
  });
  it("overrides client defaults that would otherwise change result types", async () => {
    const client = {
      query: (params: Parameters<ClickHouseClient["query"]>[0]) =>
        context.client.query({
          ...params,
          clickhouse_settings: {
            join_use_nulls: 1,
            output_format_json_quote_64bit_integers: 0,
            ...params.clickhouse_settings,
          },
        }),
    };
    const rows = await db
      .selectFrom("users")
      .selectExpr((eb) => [eb.fn.count().as("n")])
      .execute({ client });
    expect(rows).toEqual([{ n: "43" }]);
  });
  it("combines branch aliases and parameters with global ordering and supports CTEs", async () => {
    const first = db.selectFrom("users").select("id").where("id", "=", 1);
    const second = db.selectFrom("event_logs").select("user_id").where("user_id", "=", 2);
    const union = first.unionAll(second).orderBy("id", "desc").limit(2);
    expect(await union.execute({ client: context.client })).toEqual([{ id: 2 }, { id: 1 }]);
    expect(
      await db
        .with("combined", union)
        .selectFrom("combined")
        .select("id")
        .orderBy("id")
        .execute({ client: context.client }),
    ).toEqual([{ id: 1 }, { id: 2 }]);
    expect(
      await first
        .unionAll(first)
        .unionAll(second)
        .orderBy("id")
        .execute({ client: context.client }),
    ).toEqual([{ id: 1 }, { id: 1 }, { id: 2 }]);
  });
  it("executes nested SQL fragments with bound values", async () => {
    const suffix = "'; DROP TABLE users; --";
    const rows = await db
      .selectFrom("users")
      .selectExpr((eb) => [sql<string>`concat(${eb.ref("email")}, ${suffix})`.as("label")])
      .where("id", "=", 1)
      .execute({ client: context.client });
    expect(rows).toEqual([{ label: `alice@example.com${suffix}` }]);
  });
  it("preserves fragment predicate precedence when composing additional filters", async () => {
    const rows = await db
      .selectFrom("users")
      .select("id")
      .where(sql`id = ${1} OR id = ${2}`)
      .where("id", "=", 2)
      .execute({ client: context.client });
    expect(rows).toEqual([{ id: 2 }]);
  });
  it("executes ranking and running aggregation windows", async () => {
    const rows = await db
      .selectFrom("users")
      .selectExpr((eb) => [
        "id",
        eb.fn
          .rowNumber()
          .over({ orderBy: [{ by: eb.ref("id") }] })
          .as("position"),
        eb.fn
          .sum("id")
          .over({
            orderBy: [{ by: eb.ref("id") }],
            rows: { start: "unbounded preceding", end: "current row" },
          })
          .as("running"),
      ])
      .where("id", "<=", 3)
      .orderBy("id")
      .execute({ client: context.client });
    expect(rows).toEqual([
      { id: 1, position: "1", running: "1" },
      { id: 2, position: "2", running: "3" },
      { id: 3, position: "3", running: "6" },
    ]);
  });
  it("reads generated-schema identifiers containing SQL syntax and literal dots", async () => {
    await context.client.command({
      query:
        "CREATE TABLE `order details` (id UInt32, `select` String, `a.b` String, `back\\`tick` String) ENGINE = Memory",
    });
    await db
      .insertInto("order details")
      .columns("id", "select", "a.b", "back`tick")
      .values([{ id: 1, select: "ok", "a.b": "dot", "back`tick": "tick" }])
      .execute({ client: context.client });
    expect(
      await db
        .selectFrom("order details as odd alias")
        .select("odd alias.select", "odd alias.a.b as dotted", "odd alias.back`tick as tick")
        .execute({ client: context.client }),
    ).toEqual([{ select: "ok", dotted: "dot", tick: "tick" }]);
  });
  it("preserves unqualified dotted column names in result keys and predicates", async () => {
    const rows = await db
      .selectFrom("order details")
      .select("a.b")
      .where("a.b", "=", "dot")
      .execute({ client: context.client });
    expect(rows).toEqual([{ "a.b": "dot" }]);
  });
  it("omits DEFAULT and server-generated values on insert", async () => {
    await context.client.command({
      query:
        "CREATE TABLE generated_values (id UInt32, label String DEFAULT 'default', doubled UInt32 MATERIALIZED id * 2, alias_id UInt32 ALIAS id) ENGINE = Memory",
    });
    await db
      .insertInto("generated_values")
      .values([{ id: 2 }])
      .execute({ client: context.client });
    expect(
      await db
        .selectFrom("generated_values")
        .select("id", "label", "doubled", "alias_id")
        .execute({ client: context.client }),
    ).toEqual([{ id: 2, label: "default", doubled: 4, alias_id: 2 }]);
  });
});
