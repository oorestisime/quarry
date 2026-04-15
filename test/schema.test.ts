import { describe, expect, it } from "vitest";
import {
  Date as CHDate,
  DateTime64,
  String as CHString,
  UInt8,
  UInt32,
  createClickHouseDB,
  defineSchema,
  param,
  table,
  view,
} from "../src";
import { normalizeSchema } from "../src/schema";

const schema = defineSchema({
  users: table.replacingMergeTree({
    id: UInt32(),
    email: CHString(),
    created_at: DateTime64(3),
    signup_date: CHDate(),
  }),
}).views((db) => ({
  final_users: view.as(db.selectFrom(db.table("users").final().as("u")).selectAll("u")),
  daily_users: view.as(
    db
      .selectFrom("users as u")
      .selectExpr((eb) => ["u.signup_date", eb.fn.count().as("total_users")])
      .groupBy("u.signup_date"),
  ),
  formatted_users: view.as(
    db
      .selectFrom("users as u")
      .selectExpr((eb) => [
        "u.id",
        eb.fn.toString("u.id").as("id_text"),
        eb.fn.lower("u.email").as("email_lower"),
        eb.fn.formatDateTime("u.created_at", "%Y-%m-%d").as("created_date_text"),
        eb.fn.toYYYYMM("u.created_at").as("created_yyyymm"),
      ]),
  ),
}));

const db = createClickHouseDB({ schema });

const engineSchema = defineSchema({
  memory_events: table.memory({
    id: UInt32(),
  }),
  merge_events: table.mergeTree({
    id: UInt32(),
  }),
  shared_merge_events: table.sharedMergeTree(
    {
      id: UInt32(),
      created_at: DateTime64(3),
    },
    {
      orderBy: ["id"],
      partitionBy: ["toYYYYMM(created_at)"],
      ttl: ["created_at + toIntervalYear(1)"],
      settings: {
        index_granularity: 8192,
      },
    },
  ),
  replacing_events: table.replacingMergeTree(
    {
      id: UInt32(),
      version: DateTime64(3),
      is_deleted: UInt8(),
    },
    {
      versionBy: "version",
      isDeletedBy: "is_deleted",
      orderBy: ["id"],
    },
  ),
  shared_replacing_events: table.sharedReplacingMergeTree(
    {
      id: UInt32(),
      version: DateTime64(3),
      is_deleted: UInt8(),
    },
    {
      versionBy: "version",
      isDeletedBy: "is_deleted",
      orderBy: ["id"],
      primaryKey: ["id"],
      settings: {
        index_granularity: 8192,
      },
    },
  ),
  summing_events: table.summingMergeTree(
    {
      id: UInt32(),
      total: UInt32(),
    },
    {
      orderBy: ["id"],
      sumColumns: ["total"],
    },
  ),
  aggregating_events: table.aggregatingMergeTree(
    {
      id: UInt32(),
      created_at: DateTime64(3),
    },
    {
      orderBy: ["id"],
      partitionBy: ["toYYYYMM(created_at)"],
    },
  ),
  collapsing_events: table.collapsingMergeTree(
    {
      id: UInt32(),
      sign: UInt8(),
    },
    {
      signBy: "sign",
      orderBy: ["id"],
    },
  ),
  versioned_collapsing_events: table.versionedCollapsingMergeTree(
    {
      id: UInt32(),
      sign: UInt8(),
      version: DateTime64(3),
    },
    {
      signBy: "sign",
      versionBy: "version",
      orderBy: ["id"],
    },
  ),
});

const engineDb = createClickHouseDB({ schema: engineSchema });

describe("schema-first mode", () => {
  it("compiles queries against schema-defined tables and inherited views", () => {
    const tableQuery = db
      .selectFrom("users as u")
      .select("u.id", "u.created_at")
      .where("u.created_at", ">=", param(new Date("2025-01-01T00:00:00.000Z"), "DateTime64(3)"))
      .orderBy("u.id", "asc")
      .toSQL();

    const inheritedViewQuery = db
      .selectFrom("final_users as f")
      .select("f.id", "f.email", "f.created_at")
      .where("f.signup_date", ">=", param(new Date("2025-01-01T00:00:00.000Z"), "Date"))
      .orderBy("f.id", "asc")
      .toSQL();

    expect(tableQuery.query).toBe(
      "SELECT u.id, u.created_at FROM users AS u WHERE u.created_at >= {p0:DateTime64(3)} ORDER BY u.id ASC",
    );
    expect(tableQuery.params).toEqual({
      p0: "2025-01-01 00:00:00.000",
    });

    expect(inheritedViewQuery.query).toBe(
      "SELECT f.id, f.email, f.created_at FROM final_users AS f WHERE f.signup_date >= {p0:Date} ORDER BY f.id ASC",
    );
    expect(inheritedViewQuery.params).toEqual({
      p0: "2025-01-01",
    });

    const formattedViewQuery = db
      .selectFrom("formatted_users as f")
      .select("f.id", "f.id_text", "f.email_lower", "f.created_date_text", "f.created_yyyymm")
      .orderBy("f.id", "asc")
      .toSQL();

    expect(formattedViewQuery.query).toBe(
      "SELECT f.id, f.id_text, f.email_lower, f.created_date_text, f.created_yyyymm FROM formatted_users AS f ORDER BY f.id ASC",
    );
    expect(formattedViewQuery.params).toEqual({});
  });

  it("allows FINAL for final-capable tables and rejects it for views", () => {
    const finalTableQuery = db.selectFrom(db.table("users").final().as("u")).select("u.id").toSQL();

    expect(finalTableQuery.query).toBe("SELECT u.id FROM users AS u FINAL");
    expect(() => db.selectFrom("final_users as f").select("f.id").final()).toThrow(
      "FINAL is not supported for source 'final_users'.",
    );
  });

  it("supports FINAL only for the ClickHouse engine families that allow it", () => {
    expect(engineDb.selectFrom("summing_events as s").select("s.id").final().toSQL().query).toBe(
      "SELECT s.id FROM summing_events AS s FINAL",
    );
    expect(
      engineDb.selectFrom("aggregating_events as a").select("a.id").final().toSQL().query,
    ).toBe("SELECT a.id FROM aggregating_events AS a FINAL");
    expect(engineDb.selectFrom("collapsing_events as c").select("c.id").final().toSQL().query).toBe(
      "SELECT c.id FROM collapsing_events AS c FINAL",
    );
    expect(
      engineDb.selectFrom("versioned_collapsing_events as v").select("v.id").final().toSQL().query,
    ).toBe("SELECT v.id FROM versioned_collapsing_events AS v FINAL");
    expect(engineDb.selectFrom("replacing_events as r").select("r.id").final().toSQL().query).toBe(
      "SELECT r.id FROM replacing_events AS r FINAL",
    );
    expect(
      engineDb.selectFrom("shared_replacing_events as s").select("s.id").final().toSQL().query,
    ).toBe("SELECT s.id FROM shared_replacing_events AS s FINAL");

    expect(() => engineDb.table("memory_events").final()).toThrow(
      "FINAL is not supported for source 'memory_events'.",
    );
    expect(() => engineDb.selectFrom("merge_events as m").select("m.id").final()).toThrow(
      "FINAL is not supported for source 'merge_events'.",
    );
    expect(() => engineDb.selectFrom("shared_merge_events as s").select("s.id").final()).toThrow(
      "FINAL is not supported for source 'shared_merge_events'.",
    );
  });

  it("preserves engine options for merge-tree family tables", () => {
    const normalized = normalizeSchema(engineSchema);

    expect(normalized.shared_merge_events.engine).toEqual({
      name: "SharedMergeTree",
      finalCapable: false,
      options: {
        orderBy: ["id"],
        partitionBy: ["toYYYYMM(created_at)"],
        ttl: ["created_at + toIntervalYear(1)"],
        settings: {
          index_granularity: 8192,
        },
      },
    });

    expect(normalized.shared_replacing_events.engine).toEqual({
      name: "SharedReplacingMergeTree",
      finalCapable: true,
      options: {
        versionBy: "version",
        isDeletedBy: "is_deleted",
        orderBy: ["id"],
        primaryKey: ["id"],
        settings: {
          index_granularity: 8192,
        },
      },
    });

    expect(normalized.summing_events.engine).toEqual({
      name: "SummingMergeTree",
      finalCapable: true,
      options: {
        orderBy: ["id"],
        sumColumns: ["total"],
      },
    });

    expect(normalized.collapsing_events.engine).toEqual({
      name: "CollapsingMergeTree",
      finalCapable: true,
      options: {
        signBy: "sign",
        orderBy: ["id"],
      },
    });

    expect(normalized.versioned_collapsing_events.engine).toEqual({
      name: "VersionedCollapsingMergeTree",
      finalCapable: true,
      options: {
        signBy: "sign",
        versionBy: "version",
        orderBy: ["id"],
      },
    });
  });

  it("validates engine option column references at runtime", () => {
    expect(() =>
      table.sharedReplacingMergeTree(
        {
          id: UInt32(),
          is_deleted: UInt8(),
        },
        {
          isDeletedBy: "is_deleted" as never,
          orderBy: ["id"],
        },
      ),
    ).toThrow("sharedReplacingMergeTree.isDeletedBy requires sharedReplacingMergeTree.versionBy.");

    expect(() =>
      table.replacingMergeTree(
        {
          id: UInt32(),
          version: DateTime64(3),
        },
        {
          versionBy: "missing" as never,
          orderBy: ["id"],
        },
      ),
    ).toThrow("Unknown column 'missing' in replacingMergeTree.versionBy.");

    expect(() =>
      table.collapsingMergeTree(
        {
          id: UInt32(),
          sign: UInt8(),
        },
        {
          signBy: "missing" as never,
          orderBy: ["id"],
        },
      ),
    ).toThrow("Unknown column 'missing' in collapsingMergeTree.signBy.");
  });
});
