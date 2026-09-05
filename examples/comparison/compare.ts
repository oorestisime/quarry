import assert from "node:assert/strict";
import { createClient } from "@clickhouse/client";
import { ClickhouseDialect } from "@founderpath/kysely-clickhouse";
import { createQueryBuilder } from "@hypequery/clickhouse";
import { Kysely, sql } from "kysely";
import { analyticsQuery } from "../analytics-api/src/analytics";
import { rawAnalytics } from "../analytics-api/src/raw-analytics";

const connection = {
  url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
  username: "quarry",
  password: "quarry",
  database: "analytics",
  clickhouse_settings: { output_format_json_quote_64bit_integers: 1 as const },
};
interface KyselyDB {
  events: { tenant_id: number; user_id: string; event_type: string; created_at: string };
}
interface HypeSchema {
  events: { tenant_id: "UInt32"; user_id: "UInt64"; event_type: "String"; created_at: "DateTime" };
}
const client = createClient(connection);
const kysely = new Kysely<KyselyDB>({ dialect: new ClickhouseDialect({ options: connection }) });
const hype = createQueryBuilder<HypeSchema>(connection);
try {
  for (const eventType of [undefined, "purchase"] as const) {
    const filters = { from: "2026-08-01 00:00:00", to: "2026-09-01 00:00:00", eventType };
    const quarry = analyticsQuery(client, 1, filters);
    let k = kysely
      .selectFrom("events")
      .where("tenant_id", "=", 1)
      .where("created_at", ">=", filters.from)
      .where("created_at", "<", filters.to);
    if (eventType) k = k.where("event_type", "=", eventType);
    const kQuery = k
      .select((eb) => [
        "event_type",
        eb.fn.countAll<string>().as("events"),
        sql<string>`uniqExact(${eb.ref("user_id")})`.as("users"),
      ])
      .groupBy("event_type")
      .orderBy("event_type");
    let h = hype
      .table("events")
      .prewhere("tenant_id", "eq", 1)
      .where("created_at", "gte", filters.from)
      .where("created_at", "lt", filters.to);
    if (eventType) h = h.where("event_type", "eq", eventType);
    const hQuery = h
      .select(["event_type"])
      .count("user_id", "events")
      .countDistinct("user_id", "users")
      .groupBy("event_type")
      .orderBy("event_type", "ASC");
    const expected = await rawAnalytics(client, 1, filters);
    const quarryRows = await quarry.execute();
    const kyselyRows = await kQuery.execute();
    const hypeRows = await hQuery.execute();
    // These assignments also check the inferred result shape. Kysely's two
    // aggregate types are explicit above; Quarry/hypequery infer them here.
    const typedRows: Array<{ event_type: string; events: string; users: string }>[] = [
      quarryRows,
      kyselyRows,
      hypeRows,
    ];
    for (const rows of typedRows) assert.deepEqual(rows, expected);
    assert.deepEqual(
      expected,
      eventType
        ? [{ event_type: "purchase", events: "2", users: "1" }]
        : [
            { event_type: "purchase", events: "2", users: "1" },
            { event_type: "signup", events: "2", users: "2" },
          ],
    );
    console.log(
      JSON.stringify(
        {
          filter: eventType ?? "all",
          quarry: quarry.toSQL(),
          kysely: { sql: kQuery.compile().sql, parameters: kQuery.compile().parameters },
          hypequery: hQuery.toSQLWithParams(),
          rows: expected,
        },
        null,
        2,
      ),
    );
  }
} finally {
  await Promise.all([client.close(), kysely.destroy(), hype.close()]);
}
