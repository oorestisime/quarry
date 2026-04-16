import { describe, expect, it } from "vitest";
import {
  buildSchemaModule,
  buildSchemaModuleResult,
  formatIntrospectionFailureReport,
  resolveConnectionOptions,
} from "../src/introspect";

describe("CLI introspection helpers", () => {
  it("prefers command args and falls back to env vars", () => {
    expect(
      resolveConnectionOptions(
        {
          url: "http://localhost:8123",
          database: "analytics",
        },
        {
          CLICKHOUSE_URL: "http://env-host:8123",
          CLICKHOUSE_DATABASE: "default",
          CLICKHOUSE_USER: "env-user",
        },
      ),
    ).toEqual({
      url: "http://localhost:8123",
      user: "env-user",
      password: undefined,
      database: "analytics",
    });
  });

  it("requires a ClickHouse URL from args or env", () => {
    expect(() => resolveConnectionOptions({}, {})).toThrow(
      "Missing ClickHouse URL. Pass --url or set CLICKHOUSE_URL in the environment.",
    );
  });

  it("can restrict introspection input to table-like objects", () => {
    expect(
      buildSchemaModule([
        {
          name: "users",
          engine: "MergeTree",
          createTableQuery: `
            CREATE TABLE default.users (
              id UInt32
            )
            ENGINE = MergeTree
            ORDER BY id
          `,
        },
      ]),
    ).toContain("table.mergeTree");
  });

  it("returns supported output and reports unsupported objects separately", () => {
    const result = buildSchemaModuleResult([
      {
        name: "users",
        engine: "MergeTree",
        createTableQuery: `
          CREATE TABLE default.users (
            id UInt32
          )
          ENGINE = MergeTree
          ORDER BY id
        `,
      },
      {
        name: "user_events",
        engine: "SharedMergeTree",
        createTableQuery: `
          CREATE TABLE default.user_events (
            id UInt32,
            event_date Date MATERIALIZED toDate(now())
          )
          ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
          ORDER BY id
        `,
      },
    ]);

    expect(result.source).toContain("users: table.mergeTree");
    expect(result.failures).toEqual([
      {
        name: "user_events",
        engine: "SharedMergeTree",
        message: "Unsupported column clause in 'event_date Date MATERIALIZED toDate(now())'.",
      },
    ]);
    expect(formatIntrospectionFailureReport(result.failures)).toContain(
      "Skipped 1 unsupported objects:",
    );
  });

  it("ignores default clauses while building the schema module", () => {
    expect(
      buildSchemaModule([
        {
          name: "users",
          engine: "MergeTree",
          createTableQuery: `
            CREATE TABLE default.users (
              id UInt32,
              created_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = MergeTree
            ORDER BY id
          `,
        },
      ]),
    ).toContain("created_at: DateTime64(3)");
  });

  it("fails fast with object-specific errors for unsupported DDL", () => {
    expect(() =>
      buildSchemaModule([
        {
          name: "users",
          engine: "MergeTree",
          createTableQuery: `
            CREATE TABLE default.users (
              id UInt32,
              event_date Date MATERIALIZED toDate(created_at)
            )
            ENGINE = MergeTree
            ORDER BY id
          `,
        },
      ]),
    ).toThrow("- users (MergeTree): Unsupported column clause");
  });
});
