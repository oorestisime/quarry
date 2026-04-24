import { createClient } from "@clickhouse/client";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  buildTypeScriptModuleResult,
  fetchDictionaryAttributes,
  filterExcludedObjects,
  formatIntrospectionFailureReport,
  formatIntrospectionSummaryReport,
  loadIntrospectionConfig,
  resolveIntrospectionArgs,
  resolveConnectionOptions,
} from "../src/introspect";

vi.mock("@clickhouse/client", () => ({
  createClient: vi.fn(),
}));

describe("CLI introspection helpers", () => {
  async function writeConfig(contents: unknown): Promise<string> {
    const dir = await mkdtemp(join(tmpdir(), "quarry-introspect-"));
    const path = join(dir, "quarry.introspect.json");
    await writeFile(path, JSON.stringify(contents), "utf8");
    return path;
  }

  it("loads JSON config and merges CLI args over config", async () => {
    const config = await writeConfig({
      url: "http://config-host:8123",
      database: "analytics",
      out: "src/db.ts",
      tablesOnly: true,
      includePattern: "^public_",
      imports: {
        "./db-overrides": ["UserPayload"],
      },
      typeOverrides: {
        users: {
          payload: "UserPayload",
        },
      },
    });

    const resolved = await resolveIntrospectionArgs({
      config,
      database: "analytics_staging",
      out: "src/db.staging.ts",
    });

    expect(resolveConnectionOptions(resolved, {})).toMatchObject({
      url: "http://config-host:8123",
      database: "analytics_staging",
      includePattern: /^public_/,
    });
    expect(resolved.out).toBe("src/db.staging.ts");
    expect(resolved.tablesOnly).toBe(true);
    expect(resolved.imports).toEqual([{ from: "./db-overrides", name: "UserPayload" }]);
    expect(resolved.typeOverrides?.get("users")?.get("payload")).toBe("UserPayload");
  });

  it("validates JSON config", async () => {
    const unknownKeyConfig = await writeConfig({
      url: "http://localhost:8123",
      includePatern: "^public_",
    });
    await expect(loadIntrospectionConfig(unknownKeyConfig)).rejects.toThrow(
      "unknown key 'includePatern'",
    );

    const invalidImportConfig = await writeConfig({
      imports: {
        "./types": ["123Payload"],
      },
      typeOverrides: {
        users: {
          payload: "UserPayload",
        },
      },
    });
    await expect(loadIntrospectionConfig(invalidImportConfig)).rejects.toThrow(
      "must be a TypeScript identifier",
    );
  });

  it("falls back from config to environment and defaults", async () => {
    const config = await writeConfig({
      includePattern: "^public_",
    });
    const resolved = await resolveIntrospectionArgs({ config });

    expect(
      resolveConnectionOptions(resolved, {
        CLICKHOUSE_URL: "http://env-host:8123",
      }),
    ).toMatchObject({
      url: "http://env-host:8123",
      database: "default",
      includePattern: /^public_/,
    });
  });

  it("uses include and exclude patterns from command args", () => {
    expect(
      resolveConnectionOptions(
        {
          url: "http://localhost:8123",
          database: "analytics",
          includePattern: "^public_",
          excludePattern: "^public_tmp_",
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
      includePattern: /^public_/,
      excludePattern: /^public_tmp_/,
    });
  });

  it("validates include and exclude patterns", () => {
    expect(() =>
      resolveConnectionOptions({ url: "http://localhost:8123", includePattern: "(" }, {}),
    ).toThrow("Invalid includePattern '(':");

    expect(() =>
      resolveConnectionOptions({ url: "http://localhost:8123", excludePattern: "(" }, {}),
    ).toThrow("Invalid excludePattern '(':");
  });

  it("requires a ClickHouse URL from args or env", () => {
    expect(() => resolveConnectionOptions({}, {})).toThrow(
      "Missing ClickHouse URL. Pass --url or set CLICKHOUSE_URL in the environment.",
    );
  });

  it("filters object names through include and exclude patterns", () => {
    expect(
      filterExcludedObjects(
        [
          {
            name: "_peerdb_raw_mirror_7b15851c__04a4__4279__83c7__16a491c86c75",
            engine: "SharedMergeTree",
          },
          {
            name: "public_users",
            engine: "SharedReplacingMergeTree",
          },
          {
            name: "public_tmp_users",
            engine: "SharedReplacingMergeTree",
          },
        ],
        /^public_/,
        /^public_tmp_/,
      ),
    ).toEqual([
      {
        name: "public_users",
        engine: "SharedReplacingMergeTree",
      },
    ]);
  });

  it("builds named row interfaces with typed source wrappers and ClickHouse aliases", () => {
    const result = buildTypeScriptModuleResult(
      [
        {
          name: "users",
          engine: "MergeTree",
        },
        {
          name: "daily_users",
          engine: "View",
        },
        {
          name: "ai.applicationFeature",
          engine: "View",
        },
        {
          name: "28DaysOfBlackHistoryLogs",
          engine: "MergeTree",
        },
      ],
      [
        { objectName: "users", name: "id", clickhouseType: "UInt32", position: 1 },
        { objectName: "users", name: "created_at", clickhouseType: "DateTime64(3)", position: 2 },
        { objectName: "users", name: "big_user_id", clickhouseType: "UInt64", position: 3 },
        {
          objectName: "users",
          name: "location",
          clickhouseType: "Tuple(Float64, Float64)",
          position: 4,
        },
        {
          objectName: "users",
          name: "attributes",
          clickhouseType: "Map(String, String)",
          position: 5,
        },
        {
          objectName: "users",
          name: "history",
          clickhouseType: "Array(DateTime64(3))",
          position: 6,
        },
        {
          objectName: "users",
          name: "maybe_created",
          clickhouseType: "Nullable(DateTime64(3))",
          position: 7,
        },
        { objectName: "users", name: "payload", clickhouseType: "Object('json')", position: 8 },
        { objectName: "daily_users", name: "signup_date", clickhouseType: "Date", position: 1 },
        { objectName: "daily_users", name: "total_users", clickhouseType: "UInt64", position: 2 },
        {
          objectName: "ai.applicationFeature",
          name: "feature_name",
          clickhouseType: "String",
          position: 1,
        },
        {
          objectName: "28DaysOfBlackHistoryLogs",
          name: "created_at",
          clickhouseType: "DateTime",
          position: 1,
        },
      ],
      {
        database: "analytics",
        tablesOnly: false,
        includePattern: "^public_",
      },
    );

    expect(result.source.startsWith("// This file is autogenerated by `quarry introspect`.")).toBe(
      true,
    );
    expect(result.source).not.toContain("// Generated with:");
    expect(result.source).not.toContain("// - database:");
    expect(result.source).toContain("import type {");
    expect(result.source).toContain('} from "@oorestisime/quarry";');
    expect(result.source).toContain("ClickHouseDateTime64");
    expect(result.source).toContain("ClickHouseUInt64");
    expect(result.source).toContain("TypedTable");
    expect(result.source).toContain("TypedView");
    expect(result.source).toContain("export interface Users {");
    expect(result.source).toContain("export interface DailyUsers {");
    expect(result.source).toContain("export interface AiApplicationFeature {");
    expect(result.source).toContain("export interface _28DaysOfBlackHistoryLogs {");
    expect(result.source).toContain("export interface Tables");
    expect(result.source).toContain("export interface Views");
    expect(result.source).toContain("export interface DB extends Tables, Views {}");
    expect(result.source).toContain("users: TypedTable<Users>;");
    expect(result.source).toContain(
      '"28DaysOfBlackHistoryLogs": TypedTable<_28DaysOfBlackHistoryLogs>;',
    );
    expect(result.source).toContain('"ai.applicationFeature": TypedView<AiApplicationFeature>;');
    expect(result.source).toContain("daily_users: TypedView<DailyUsers>;");
    expect(result.source).toContain("id: number;");
    expect(result.source).toContain("created_at: ClickHouseDateTime64;");
    expect(result.source).toContain("big_user_id: ClickHouseUInt64;");
    expect(result.source).toContain("location: [number, number];");
    expect(result.source).toContain("attributes: Record<string, string>;");
    expect(result.source).toContain("history: Array<string>;");
    expect(result.source).toContain("maybe_created: ClickHouseDateTime64 | null;");
    expect(result.source).toContain("payload: unknown;");
    expect(result.source).toContain("signup_date: ClickHouseDate;");
    expect(result.source).toContain("total_users: ClickHouseUInt64;");
    expect(result.summary).toEqual({
      generatedObjects: 4,
      generatedTables: 2,
      generatedViews: 2,
      generatedDictionaries: 0,
      skippedObjects: 0,
    });
  });

  it("overrides configured column types and emits configured imports", () => {
    const typeOverrides = new Map([
      ["users", new Map([["payload", "UserPayload | null"]])],
      ["daily_users", new Map([["summary", "DailyUserSummary"]])],
      ["partner_rates", new Map([["metadata", "PartnerRateMetadata"]])],
    ]);

    const result = buildTypeScriptModuleResult(
      [
        { name: "users", engine: "MergeTree" },
        { name: "daily_users", engine: "View" },
      ],
      [
        { objectName: "users", name: "id", clickhouseType: "UInt32", position: 1 },
        { objectName: "users", name: "payload", clickhouseType: "Object('json')", position: 2 },
        { objectName: "daily_users", name: "day", clickhouseType: "Date", position: 1 },
        { objectName: "daily_users", name: "summary", clickhouseType: "String", position: 2 },
      ],
      {},
      [
        {
          name: "partner_rates",
          columns: [
            {
              objectName: "partner_rates",
              name: "rate_cents",
              clickhouseType: "UInt32",
              position: 1,
            },
            {
              objectName: "partner_rates",
              name: "metadata",
              clickhouseType: "Object('json')",
              position: 2,
            },
          ],
        },
      ],
      {
        imports: [
          { from: "./db-overrides", name: "UserPayload" },
          { from: "./db-overrides", name: "DailyUserSummary" },
          { from: "./external", name: "Metadata", as: "PartnerRateMetadata" },
        ],
        typeOverrides,
      },
    );

    expect(result.source).toContain(
      'import type { DailyUserSummary, UserPayload } from "./db-overrides";',
    );
    expect(result.source).toContain(
      'import type { Metadata as PartnerRateMetadata } from "./external";',
    );
    expect(result.source).toContain("id: number;");
    expect(result.source).toContain("payload: UserPayload | null;");
    expect(result.source).toContain("day: ClickHouseDate;");
    expect(result.source).toContain("summary: DailyUserSummary;");
    expect(result.source).toContain("rate_cents: number;");
    expect(result.source).toContain("metadata: PartnerRateMetadata;");
  });

  it("rejects type overrides for missing sources and columns", () => {
    expect(() =>
      buildTypeScriptModuleResult(
        [{ name: "users", engine: "MergeTree" }],
        [{ objectName: "users", name: "id", clickhouseType: "UInt32", position: 1 }],
        {},
        undefined,
        { typeOverrides: new Map([["events", new Map([["payload", "EventPayload"]])]]) },
      ),
    ).toThrow("unknown source 'events'");

    expect(() =>
      buildTypeScriptModuleResult(
        [{ name: "users", engine: "MergeTree" }],
        [{ objectName: "users", name: "id", clickhouseType: "UInt32", position: 1 }],
        {},
        undefined,
        { typeOverrides: new Map([["users", new Map([["payload", "UserPayload"]])]]) },
      ),
    ).toThrow("unknown column 'users.payload'");
  });

  it("skips objects whose column metadata is missing", () => {
    const result = buildTypeScriptModuleResult(
      [
        {
          name: "users",
          engine: "MergeTree",
        },
        {
          name: "daily_users",
          engine: "View",
        },
      ],
      [{ objectName: "users", name: "id", clickhouseType: "UInt32", position: 1 }],
    );

    expect(result.source).toContain("export interface Tables");
    expect(result.source).not.toContain("daily_users");
    expect(result.failures).toEqual([
      {
        name: "daily_users",
        engine: "View",
        message: "Could not load column metadata for the introspected object.",
      },
    ]);
    expect(formatIntrospectionSummaryReport(result.summary)).toBe(
      "Generated 1 object: 1 table, 0 views, 0 dictionaries. Skipped 1 object.",
    );
    expect(formatIntrospectionFailureReport(result.failures)).toContain(
      "Skipped 1 unsupported objects:",
    );
  });

  it("builds dictionary interfaces with TypedDictionary wrapper", () => {
    const result = buildTypeScriptModuleResult(
      [{ name: "users", engine: "MergeTree" }],
      [
        { objectName: "users", name: "id", clickhouseType: "UInt32", position: 1 },
        { objectName: "users", name: "name", clickhouseType: "String", position: 2 },
      ],
      {},
      [
        {
          name: "partner_rates",
          columns: [
            { objectName: "partner_rates", name: "id", clickhouseType: "UInt32", position: 1 },
            {
              objectName: "partner_rates",
              name: "rate_cents",
              clickhouseType: "UInt32",
              position: 2,
            },
            {
              objectName: "partner_rates",
              name: "currency",
              clickhouseType: "String",
              position: 3,
            },
          ],
        },
      ],
    );

    expect(result.source).toContain("TypedDictionary");
    expect(result.source).toContain("export interface PartnerRates {");
    expect(result.source).toContain("rate_cents: number;");
    expect(result.source).toContain("currency: string;");
    expect(result.source).toContain("export interface Dictionaries");
    expect(result.source).toContain("partner_rates: TypedDictionary<PartnerRates>;");
    expect(result.source).toContain("export interface DB extends Tables, Views, Dictionaries {}");
    expect(result.summary).toEqual({
      generatedObjects: 2,
      generatedTables: 1,
      generatedViews: 0,
      generatedDictionaries: 1,
      skippedObjects: 0,
    });
  });

  it("fetches dictionary attributes from system.dictionary_attributes", async () => {
    const close = vi.fn();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ json: vi.fn().mockResolvedValue([{ count: "1" }]) })
      .mockResolvedValueOnce({
        json: vi.fn().mockResolvedValue([
          { objectName: "partner_rates", name: "rate_cents", clickhouseType: "UInt32" },
          { objectName: "partner_rates", name: "currency", clickhouseType: "String" },
        ]),
      });
    vi.mocked(createClient).mockReturnValueOnce({ query, close } as never);

    await expect(
      fetchDictionaryAttributes({
        url: "http://localhost:8123",
        database: "default",
      }),
    ).resolves.toEqual([
      { objectName: "partner_rates", name: "rate_cents", clickhouseType: "UInt32", position: 1 },
      { objectName: "partner_rates", name: "currency", clickhouseType: "String", position: 2 },
    ]);
    expect(query).toHaveBeenCalledTimes(2);
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("falls back to system.dictionaries attribute arrays", async () => {
    const close = vi.fn();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ json: vi.fn().mockResolvedValue([{ count: "0" }]) })
      .mockResolvedValueOnce({
        json: vi.fn().mockResolvedValue([
          {
            objectName: "partner_rates",
            name: "rate_cents",
            clickhouseType: "UInt32",
            position: 1,
          },
          { objectName: "partner_rates", name: "currency", clickhouseType: "String", position: 1 },
        ]),
      });
    vi.mocked(createClient).mockReturnValueOnce({ query, close } as never);

    await expect(
      fetchDictionaryAttributes({
        url: "http://localhost:8123",
        database: "default",
      }),
    ).resolves.toEqual([
      { objectName: "partner_rates", name: "rate_cents", clickhouseType: "UInt32", position: 1 },
      { objectName: "partner_rates", name: "currency", clickhouseType: "String", position: 1 },
    ]);
    expect(query).toHaveBeenCalledTimes(2);
    expect(close).toHaveBeenCalledTimes(1);
  });
});
