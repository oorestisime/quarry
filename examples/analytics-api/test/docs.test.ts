import { strict as assert } from "node:assert";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { createClient } from "@clickhouse/client";
import {
  createAnalyticsDB,
  dailyActivity,
  latestEvents,
  filteredCounts,
  activityPage,
  exportEvents,
} from "../src/recipes";

function codeBlock(source: string, language: string): string {
  const block = source.match(new RegExp("```" + language + "\\n([\\s\\S]*?)```"));
  assert.ok(block, `Missing ${language} example`);
  return block[1].trim();
}

function normalizeSQL(sql: string): string {
  return sql.replace(/;$/, "").replace(/\s+/g, " ").trim();
}

test("docs recipes compile and return their displayed ClickHouse results", async () => {
  const client = createClient({
    url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
    username: "quarry",
    password: "quarry",
    database: "analytics",
  });
  const db = createAnalyticsDB(client);
  const recipes = [
    ["daily-activity", dailyActivity(db, 1)],
    ["latest-event", latestEvents(db, 1)],
    [
      "optional-filters",
      filteredCounts(db, 1, { from: "2026-08-04 00:00:00", eventType: "purchase" }),
    ],
    ["pagination", activityPage(db, 1, 2)],
    ["streaming-export", exportEvents(db, 1)],
  ] as const;
  try {
    for (const [name, query] of recipes) {
      const doc = await readFile(
        new URL(`../../../docs/content/docs/recipes/${name}.mdx`, import.meta.url),
        "utf8",
      );
      assert.equal(normalizeSQL(query.toSQL().query), normalizeSQL(codeBlock(doc, "sql")), name);
      const expected: unknown = JSON.parse(codeBlock(doc, "json"));
      if (name === "streaming-export") {
        const rows = [];
        for await (const row of query.stream()) rows.push(row);
        assert.deepEqual(rows, expected, name);
      } else {
        assert.deepEqual(await query.execute(), expected, name);
      }
    }
    assert.deepEqual(await dailyActivity(db, 2).execute(), [{ day: "2026-08-01", events: "1" }]);
    assert.deepEqual(await filteredCounts(db, 1, { from: "2026-08-01 00:00:00" }).execute(), [
      { event_type: "purchase", events: "2" },
      { event_type: "signup", events: "2" },
    ]);
    assert.deepEqual(await activityPage(db, 1, 4).execute(), []);
  } finally {
    await client.close();
  }
});
