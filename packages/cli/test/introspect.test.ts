import { describe, expect, it } from "vitest";
import { buildSchemaModule, resolveConnectionOptions } from "../src/introspect";

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

  it("fails fast with object-specific errors for unsupported DDL", () => {
    expect(() =>
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
    ).toThrow("- users (MergeTree): Unsupported column clause");
  });
});
