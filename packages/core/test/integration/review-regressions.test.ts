import { createClient } from "@clickhouse/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  createClickHouseDB,
  param,
  type ClickHouseDateTime64,
  type TypedDictionary,
} from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface DB {
  users: { id: number };
  event_logs: { user_id: number; event_type: string };
  typed_samples: { id: number; created_at: ClickHouseDateTime64; nickname: string | null };
  partner_rate_ranges: TypedDictionary<{ rate_cents: number }>;
  review_insert_results: { id: number; event_type: string | null };
  review_instants: { instant: ClickHouseDateTime64 };
}

const db = createClickHouseDB<DB>();

let context: ClickHouseTestContext;

describe("review regressions", () => {
  beforeAll(async () => {
    context = await startClickHouse();
  }, 120_000);

  afterAll(async () => {
    await stopClickHouse(context);
  });

  // Finding 1: RANGE_HASHED expects the range value before the fallback.
  it.each([
    { date: "2025-01-15", expected: 100 },
    { date: "2024-12-31", expected: 999 },
  ])("returns $expected for a range dictionary lookup on $date", async ({ date, expected }) => {
    const rows = await db
      .selectFrom("users")
      .where("id", "=", 1)
      .selectExpr((eb) => [
        eb.fn
          .dictGetOrDefault(
            "partner_rate_ranges",
            "rate_cents",
            "id",
            999,
            eb.val(param(date, "Date")),
          )
          .as("rate"),
      ])
      .execute({ client: context.client });

    expect(rows).toEqual([{ rate: expected }]);
  });

  // Finding 2: an accepted Date input must retain its fractional seconds.
  it("matches a DateTime64 value using a JavaScript Date with milliseconds", async () => {
    const query = db.selectFrom("typed_samples").select("id");
    const expected = [{ id: 1 }];

    expect(
      await query
        .where("created_at", "=", param("2025-01-01 10:11:12.123", "DateTime64(3)"))
        .execute({ client: context.client }),
    ).toEqual(expected);

    const rows = await query
      .where("created_at", "=", new Date("2025-01-01T10:11:12.123Z"))
      .execute({
        client: context.client,
        clickhouse_settings: { session_timezone: "America/New_York" },
      });

    expect(rows).toEqual(expected);

    expect(
      await query
        .where("created_at", "in", [
          new Date("2025-01-01T10:11:12.000Z"),
          new Date("2025-01-01T10:11:12.123Z"),
        ])
        .execute({ client: context.client }),
    ).toEqual(expected);
  });

  it("does not round microsecond timestamps when comparing them with Date arrays", async () => {
    await context.client.command({
      query: "CREATE TABLE review_instants (instant DateTime64(6, 'UTC')) ENGINE = Memory",
    });
    await db
      .insertInto("review_instants")
      .values([{ instant: "2025-01-01 00:00:00.123456" }])
      .execute({ client: context.client });
    const query = db.selectFrom("review_instants").selectAll();
    const dates = [new Date("2025-01-01T00:00:00.123Z")];
    expect(await query.where("instant", "in", dates).execute({ client: context.client })).toEqual(
      [],
    );
    expect(
      await query.where("instant", "not in", dates).execute({ client: context.client }),
    ).toEqual([{ instant: "2025-01-01 00:00:00.123456" }]);
  });

  it("returns null from casts of nullable values through a subquery", async () => {
    const cast = db
      .selectFrom("typed_samples")
      .where("id", "=", 1)
      .selectExpr((eb) => [
        eb.fn.toUInt32("nickname").as("number"),
        eb.fn.toString("nickname").as("text"),
        eb.fn.toDateTime64("nickname", 3).as("date"),
      ]);

    const rows = await db
      .selectFrom(cast.as("cast"))
      .selectAll()
      .execute({ client: context.client });

    expect(rows).toEqual([{ number: null, text: null, date: null }]);
  });

  it("serializes empty aggregates as null even with inherited denormal quoting", async () => {
    const client = createClient({
      url: `http://${context.container.getHost()}:${context.container.getMappedPort(8123)}`,
      username: "test",
      password: "test",
      clickhouse_settings: { output_format_json_quote_denormals: 1 },
    });

    try {
      const aggregate = db
        .selectFrom("users")
        .selectExpr((eb) => [
          eb.fn.avg("id").as("average"),
          eb.fn.avgIf("id", eb.cmp("id", "<", 0)).as("conditional_average"),
          eb.fn.quantile(0.5, "id").as("median"),
        ]);

      expect(await aggregate.where("id", "<", 0).execute({ client })).toEqual([
        { average: null, conditional_average: null, median: null },
      ]);
      const populated = await aggregate.execute({ client });
      expect(populated[0]).toEqual({
        average: expect.any(Number),
        conditional_average: null,
        median: expect.any(Number),
      });
    } finally {
      await client.close();
    }
  });

  // Finding 3: client defaults must not change a select when used as an insert source.
  it("preserves ordinary left-join defaults through INSERT SELECT", async () => {
    await context.client.command({
      query:
        "CREATE TABLE review_insert_results (id UInt32, event_type Nullable(String)) ENGINE = Memory",
    });

    const client = createClient({
      url: `http://${context.container.getHost()}:${context.container.getMappedPort(8123)}`,
      username: "test",
      password: "test",
      clickhouse_settings: { join_use_nulls: 1 },
    });

    try {
      const select = db
        .selectFrom("users as u")
        .leftJoin("event_logs as e", "u.id", "e.user_id")
        .where("u.id", "=", 4)
        .select("u.id", "e.event_type");

      const selected = await select.execute({ client });
      expect(selected).toEqual([{ id: 4, event_type: "" }]);

      await db
        .insertInto("review_insert_results")
        .columns("id", "event_type")
        .fromSelect(select)
        .execute({ client });

      const inserted = await db.selectFrom("review_insert_results").selectAll().execute({ client });

      expect(inserted).toEqual(selected);
    } finally {
      await client.close();
    }
  });
});
