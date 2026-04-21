import { describe, expect, it, vi } from "vitest";
import { createClickHouseDB, param, ExpressionBuilder, Expression } from "../src";

interface QueryBuilderTestDB {
  event_logs: {
    user_id: number;
    event_type: string;
  };
  user_session_events: {
    user_id: number;
    activity_key_id: number;
    data: string | null;
  };
  typed_samples: {
    id: number;
    label: string;
    nickname: string | null;
    status: "pending" | "active" | "archived";
    tags: string[];
    amount: number;
    created_at: string;
  };
}

const db = createClickHouseDB<QueryBuilderTestDB>();

describe("query builder validation", () => {
  it("rejects invalid limit values", () => {
    expect(() => db.selectFrom("event_logs").selectAll().limit(-1)).toThrow(
      "LIMIT must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().limit(Number.NaN)).toThrow(
      "LIMIT must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().limit(3.7)).toThrow(
      "LIMIT must be a non-negative integer.",
    );
  });

  it("rejects invalid offset values", () => {
    expect(() => db.selectFrom("event_logs").selectAll().offset(-1)).toThrow(
      "OFFSET must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().offset(Number.NaN)).toThrow(
      "OFFSET must be a non-negative integer.",
    );
    expect(() => db.selectFrom("event_logs").selectAll().offset(3.7)).toThrow(
      "OFFSET must be a non-negative integer.",
    );
  });

  it("gives a clearer final error for non-table sources", () => {
    const subquery = db.selectFrom("event_logs").selectAll().as("logs");

    expect(() => db.selectFrom(subquery).final()).toThrow(
      "FINAL can only be applied to table sources.",
    );
  });

  it("compiles whereNull and whereNotNull predicates", () => {
    const whereNullQuery = db.selectFrom("typed_samples").selectAll().whereNull("nickname").toSQL();
    const whereNotNullQuery = db
      .selectFrom("typed_samples")
      .selectAll()
      .whereNotNull("nickname")
      .toSQL();

    expect(whereNullQuery.query).toBe("SELECT * FROM typed_samples WHERE nickname IS NULL");
    expect(whereNullQuery.params).toEqual({});
    expect(whereNotNullQuery.query).toBe("SELECT * FROM typed_samples WHERE nickname IS NOT NULL");
    expect(whereNotNullQuery.params).toEqual({});
  });

  it("compiles unary where and prewhere expression predicates", () => {
    const query = db
      .selectFrom("typed_samples")
      .selectExpr((eb) => ["tags", eb.fn.length("tags").as("tag_count")])
      .prewhere((eb) => eb.fn.has("tags", "vip"))
      .where((eb) => eb.fn.notEmpty("tags"))
      .toSQL();

    expect(query.query).toBe(
      "SELECT tags, length(tags) AS tag_count FROM typed_samples PREWHERE has(tags, {p0:String}) WHERE notEmpty(tags)",
    );
    expect(query.params).toEqual({ p0: "vip" });
  });

  it("compiles chained prewhere predicates with a separate where clause", () => {
    const query = db
      .selectFrom("user_session_events")
      .select("data")
      .prewhere("user_id", "=", param(42, "Int64"))
      .prewhere("activity_key_id", "=", param(7, "UInt8"))
      .whereNotNull("data")
      .where("data", "!=", "")
      .groupBy("data")
      .toSQL();

    expect(query.query).toBe(
      "SELECT data FROM user_session_events PREWHERE user_id = {p0:Int64} AND activity_key_id = {p1:UInt8} WHERE data IS NOT NULL AND data != {p2:String} GROUP BY data",
    );
    expect(query.params).toEqual({
      p0: 42,
      p1: 7,
      p2: "",
    });
  });

  it("compiles distinct selections", () => {
    const query = db
      .selectFrom("event_logs as e")
      .distinct()
      .select("e.event_type")
      .orderBy("e.event_type", "asc")
      .toSQL();

    expect(query.query).toBe(
      "SELECT DISTINCT e.event_type FROM event_logs AS e ORDER BY e.event_type ASC",
    );
    expect(query.params).toEqual({});
  });

  it("compiles distinct on selections", () => {
    const query = db
      .selectFrom("event_logs as e")
      .distinctOn("e.user_id")
      .select("e.user_id", "e.event_type")
      .orderBy("e.user_id", "asc")
      .orderBy("e.event_type", "asc")
      .toSQL();

    expect(query.query).toBe(
      "SELECT DISTINCT ON (e.user_id) e.user_id, e.event_type FROM event_logs AS e ORDER BY e.user_id ASC, e.event_type ASC",
    );
    expect(query.params).toEqual({});
  });

  it("rejects distinctOn without expressions", () => {
    expect(() => db.selectFrom("event_logs as e").distinctOn()).toThrow(
      "DISTINCT ON requires at least one expression.",
    );
  });

  it("compiles OR groups and preserves grouping across chained where calls", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .select("t.label")
      .where((eb) => eb.or([eb.fn.has("t.tags", "trial"), eb.cmp("t.nickname", "=", "bee")]))
      .where((eb) => eb.fn.notEmpty("t.label"))
      .toSQL();

    expect(query.query).toBe(
      "SELECT t.label FROM typed_samples AS t WHERE (has(t.tags, {p0:String}) OR t.nickname = {p1:String}) AND notEmpty(t.label)",
    );
    expect(query.params).toEqual({ p0: "trial", p1: "bee" });
  });

  it("compiles string function expressions", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [
        "t.label",
        eb.fn.like("t.label", "%ph%").as("has_ph"),
        eb.fn.concat(eb.ref("t.label"), "-", "suffix").as("label_key"),
        eb.fn.substring("t.label", 2, 3).as("label_slice"),
        eb.fn.trimBoth(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_trimmed"),
      ])
      .where((eb) => eb.fn.notEmpty("t.label"))
      .toSQL();

    expect(query.query).toBe(
      "SELECT t.label, like(t.label, {p0:String}) AS has_ph, concat(t.label, {p1:String}, {p2:String}) AS label_key, substring(t.label, {p3:Int64}, {p4:Int64}) AS label_slice, trimBoth(concat({p5:String}, t.label, {p6:String})) AS label_trimmed FROM typed_samples AS t WHERE notEmpty(t.label)",
    );
    expect(query.params).toEqual({
      p0: "%ph%",
      p1: "-",
      p2: "suffix",
      p3: 2,
      p4: 3,
      p5: "  ",
      p6: "  ",
    });
  });

  it("compiles heavy-hitter function expressions", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [
        "t.id",
        eb.fn
          .if(eb.cmp("t.status", "=", "active"), eb.ref("t.label"), eb.val("inactive"))
          .as("status_label"),
        eb.fn.least(eb.ref("t.id"), eb.val(param(10, "UInt32"))).as("least_val"),
        eb.fn.greatest(eb.ref("t.id"), eb.val(param(10, "UInt32"))).as("greatest_val"),
        eb.fn.ceil("t.amount").as("ceil_amount"),
        eb.fn.floor("t.amount").as("floor_amount"),
        eb.fn.toUInt8("t.id").as("id_u8"),
        eb.fn.toYear("t.created_at").as("created_year"),
        eb.fn.toMonth("t.created_at").as("created_month"),
      ])
      .where((eb) => eb.fn.toUInt8("t.id"), ">", 0)
      .toSQL();

    expect(query.query).toBe(
      "SELECT t.id, if(t.status = {p0:String}, t.label, {p1:String}) AS status_label, least(t.id, {p2:UInt32}) AS least_val, greatest(t.id, {p3:UInt32}) AS greatest_val, ceil(t.amount) AS ceil_amount, floor(t.amount) AS floor_amount, toUInt8(t.id) AS id_u8, toYear(t.created_at) AS created_year, toMonth(t.created_at) AS created_month FROM typed_samples AS t WHERE toUInt8(t.id) > {p4:Int64}",
    );
    expect(query.params).toEqual({
      p0: "active",
      p1: "inactive",
      p2: 10,
      p3: 10,
      p4: 0,
    });
  });

  it("compiles least and greatest with single argument", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [
        eb.fn.least("t.id").as("single_least"),
        eb.fn.greatest("t.id").as("single_greatest"),
      ])
      .toSQL();

    expect(query.query).toBe(
      "SELECT least(t.id) AS single_least, greatest(t.id) AS single_greatest FROM typed_samples AS t",
    );
    expect(query.params).toEqual({});
  });

  it("compiles countDistinct aggregate", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [eb.fn.countDistinct("t.label").as("distinct_labels")])
      .toSQL();

    expect(query.query).toBe(
      "SELECT count(DISTINCT t.label) AS distinct_labels FROM typed_samples AS t",
    );
    expect(query.params).toEqual({});
  });

  it("compiles now64 with precision", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [eb.fn.now64(3).as("current_time_precise")])
      .toSQL();

    expect(query.query).toBe("SELECT now64(3) AS current_time_precise FROM typed_samples AS t");
    expect(query.params).toEqual({});
  });

  it("compiles date/time function expressions", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [
        "t.label",
        eb.fn.now().as("current_time"),
        eb.fn.today().as("current_date"),
        eb.fn.toStartOfMonth("t.created_at").as("month_start"),
        eb.fn.toStartOfWeek("t.created_at").as("week_start"),
        eb.fn.toStartOfDay("t.created_at").as("day_start"),
        eb.fn.toStartOfYear("t.created_at").as("year_start"),
        eb.fn.formatDateTime("t.created_at", "%Y-%m-%d").as("created_date_text"),
        eb.fn
          .dateDiff("day", eb.fn.toDate("t.created_at"), eb.val(param("2025-01-03", "Date")))
          .as("days_until_cutoff"),
        eb.fn.dateAdd("day", 5, "t.created_at").as("plus_five_days"),
        eb.fn.dateSub("hour", 2, "t.created_at").as("minus_two_hours"),
        eb.fn.toYYYYMM("t.created_at").as("created_yyyymm"),
        eb.fn.toYYYYMMDD("t.created_at").as("created_yyyymmdd"),
      ])
      .where((eb) => eb.fn.toYYYYMM("t.created_at"), "=", 202501)
      .toSQL();

    expect(query.query).toBe(
      "SELECT t.label, now() AS current_time, today() AS current_date, toStartOfMonth(t.created_at) AS month_start, toStartOfWeek(t.created_at) AS week_start, toStartOfDay(t.created_at) AS day_start, toStartOfYear(t.created_at) AS year_start, formatDateTime(t.created_at, '%Y-%m-%d') AS created_date_text, dateDiff('day', toDate(t.created_at), {p0:Date}) AS days_until_cutoff, addDays(t.created_at, {p1:Int64}) AS plus_five_days, subtractHours(t.created_at, {p2:Int64}) AS minus_two_hours, toYYYYMM(t.created_at) AS created_yyyymm, toYYYYMMDD(t.created_at) AS created_yyyymmdd FROM typed_samples AS t WHERE toYYYYMM(t.created_at) = {p3:Int64}",
    );
    expect(query.params).toEqual({
      p0: "2025-01-03",
      p1: 5,
      p2: 2,
      p3: 202501,
    });
  });

  it("compiles null function expressions", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [
        "t.label",
        eb.fn.isNull("t.nickname").as("nickname_is_null"),
        eb.fn.isNotNull("t.nickname").as("nickname_is_not_null"),
        eb.fn.nullIf("t.label", "beta").as("maybe_label"),
        eb.fn.coalesce("t.nickname", eb.ref("t.label")).as("display_name"),
        eb.fn.coalesce("t.nickname", eb.val("Unknown")).as("display_name_with_literal"),
        eb.fn.ifNull("t.nickname", "Unknown").as("nickname_or_default"),
      ])
      .toSQL();

    expect(query.query).toBe(
      "SELECT t.label, isNull(t.nickname) AS nickname_is_null, isNotNull(t.nickname) AS nickname_is_not_null, nullIf(t.label, {p0:String}) AS maybe_label, coalesce(t.nickname, t.label) AS display_name, coalesce(t.nickname, {p1:String}) AS display_name_with_literal, ifNull(t.nickname, {p2:String}) AS nickname_or_default FROM typed_samples AS t",
    );
    expect(query.params).toEqual({
      p0: "beta",
      p1: "Unknown",
      p2: "Unknown",
    });
  });

  it("compiles null function predicates in joins and having clauses", () => {
    const query = db
      .selectFrom("typed_samples as a")
      .innerJoin("typed_samples as b", (eb) =>
        eb.cmp(
          eb.fn.coalesce("a.nickname", eb.ref("a.label")),
          "=",
          eb.fn.coalesce("b.nickname", eb.ref("b.label")),
        ),
      )
      .selectExpr((eb) => ["a.label", eb.fn.count().as("match_count")])
      .where((eb) => eb.fn.isNotNull(eb.fn.nullIf("a.label", "beta")))
      .groupBy("a.label", "a.nickname")
      .having((eb) => eb.fn.isNull(eb.fn.nullIf("a.nickname", "bee")))
      .toSQL();

    expect(query.query).toBe(
      "SELECT a.label, count() AS match_count FROM typed_samples AS a INNER JOIN typed_samples AS b ON coalesce(a.nickname, a.label) = coalesce(b.nickname, b.label) WHERE isNotNull(nullIf(a.label, {p0:String})) GROUP BY a.label, a.nickname HAVING isNull(nullIf(a.nickname, {p1:String}))",
    );
    expect(query.params).toEqual({ p0: "beta", p1: "bee" });
  });

  it("compiles left anti joins", () => {
    const query = db
      .selectFrom("event_logs as base")
      .leftAntiJoin("event_logs as denied", "base.user_id", "denied.user_id")
      .select("base.user_id", "denied.event_type")
      .toSQL();

    expect(query.query).toBe(
      "SELECT base.user_id, denied.event_type FROM event_logs AS base LEFT ANTI JOIN event_logs AS denied ON base.user_id = denied.user_id",
    );
    expect(query.params).toEqual({});
  });

  it("treats plain concat strings as literals, so column refs must use eb.ref()", () => {
    const query = db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => [eb.fn.concat("t.label", "-", "suffix").as("label_key")])
      .toSQL();

    expect(query.query).toBe(
      "SELECT concat({p0:String}, {p1:String}, {p2:String}) AS label_key FROM typed_samples AS t",
    );
    expect(query.params).toEqual({
      p0: "t.label",
      p1: "-",
      p2: "suffix",
    });
  });

  it("compiles unary having expression predicates", () => {
    const query = db
      .selectFrom("event_logs as e")
      .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
      .groupBy("e.user_id")
      .having((eb) => eb.raw<number>("count() > 0"))
      .toSQL();

    expect(query.query).toBe(
      "SELECT e.user_id, count() AS event_count FROM event_logs AS e GROUP BY e.user_id HAVING count() > 0",
    );
    expect(query.params).toEqual({});
  });

  it("accepts pre-built Expression objects in where, prewhere, and having", () => {
    const eb = new ExpressionBuilder<any>();
    const tagExpr = eb.fn.has("tags", "vip");
    const notEmptyExpr = eb.fn.notEmpty("label");

    const query = db
      .selectFrom("typed_samples")
      .selectAll()
      .prewhere(tagExpr)
      .where(notEmptyExpr)
      .toSQL();

    expect(query.query).toBe(
      "SELECT * FROM typed_samples PREWHERE has(tags, {p0:String}) WHERE notEmpty(label)",
    );
    expect(query.params).toEqual({ p0: "vip" });
  });

  it("accepts structurally-typed expression-like objects (not just instanceof Expression)", () => {
    const eb = new ExpressionBuilder<any>();
    const realExpr = eb.cmp("label", "=", "a");
    // Simulate a cross-package or wrapped value that satisfies the type
    // but is not the exact local constructor instance.
    const structuralExpr = {
      node: realExpr.node,
      clickhouseType: realExpr.clickhouseType,
    } as Expression<unknown>;

    const query = db.selectFrom("typed_samples").selectAll().where(structuralExpr).toSQL();

    expect(query.query).toBe("SELECT * FROM typed_samples WHERE label = {p0:String}");
    expect(query.params).toEqual({ p0: "a" });
  });

  it("chains pre-built Expression objects with AND via appendCondition", () => {
    const eb = new ExpressionBuilder<any>();
    const expr1 = eb.cmp("label", "=", "a");
    const expr2 = eb.cmp("nickname", "=", "b");

    const query = db.selectFrom("typed_samples").selectAll().where(expr1).where(expr2).toSQL();

    expect(query.query).toBe(
      "SELECT * FROM typed_samples WHERE label = {p0:String} AND nickname = {p1:String}",
    );
    expect(query.params).toEqual({ p0: "a", p1: "b" });
  });

  it("accepts pre-built Expression in having", () => {
    const eb = new ExpressionBuilder<any>();
    const havingExpr = eb.raw<number>("count() > 0");

    const query = db
      .selectFrom("event_logs as e")
      .selectExpr((eb2) => ["e.user_id", eb2.fn.count().as("event_count")])
      .groupBy("e.user_id")
      .having(havingExpr)
      .toSQL();

    expect(query.query).toBe(
      "SELECT e.user_id, count() AS event_count FROM event_logs AS e GROUP BY e.user_id HAVING count() > 0",
    );
    expect(query.params).toEqual({});
  });

  it("rejects bare null predicate values at runtime", () => {
    expect(() =>
      db
        .selectFrom("typed_samples")
        .selectAll()
        .where("nickname" as never, "=", null as never),
    ).toThrow(
      'Bare null predicate values are not supported. Use whereNull()/whereNotNull() or param(null, "Nullable(...)").',
    );
  });

  it("uses the configured client for executeTakeFirst helpers", async () => {
    const json = vi.fn().mockResolvedValue([
      {
        user_id: 1,
        event_type: "signup",
      },
    ]);
    const queryClient = {
      query: vi.fn().mockResolvedValue({ json }),
    };
    const dbWithClient = createClickHouseDB<QueryBuilderTestDB>({
      client: queryClient,
    });
    const query = dbWithClient
      .selectFrom("event_logs")
      .select("user_id", "event_type")
      .where("user_id", "=", 1);

    await expect(query.executeTakeFirst()).resolves.toEqual({
      user_id: 1,
      event_type: "signup",
    });
    await expect(query.executeTakeFirstOrThrow()).resolves.toEqual({
      user_id: 1,
      event_type: "signup",
    });

    expect(queryClient.query).toHaveBeenCalledTimes(2);
    expect(queryClient.query).toHaveBeenNthCalledWith(1, {
      query: "SELECT user_id, event_type FROM event_logs WHERE user_id = {p0:Int64}",
      query_params: { p0: 1 },
      format: "JSONEachRow",
    });
  });

  it("forwards query_id and clickhouse_settings through execute", async () => {
    const json = vi.fn().mockResolvedValue([]);
    const queryClient = {
      query: vi.fn().mockResolvedValue({ json }),
    };
    const dbWithClient = createClickHouseDB<QueryBuilderTestDB>({
      client: queryClient,
    });
    const query = dbWithClient.selectFrom("event_logs").selectAll();

    await query.execute({
      queryId: "select-query-id",
      clickhouse_settings: {
        max_threads: 1,
        wait_end_of_query: true,
      },
    });

    expect(queryClient.query).toHaveBeenCalledWith({
      query: "SELECT * FROM event_logs",
      query_params: {},
      format: "JSONEachRow",
      query_id: "select-query-id",
      clickhouse_settings: {
        max_threads: 1,
        wait_end_of_query: true,
      },
    });
  });

  it("supports overriding the configured client through execution options", async () => {
    const defaultClient = {
      query: vi.fn(),
    };
    const overrideJson = vi.fn().mockResolvedValue([]);
    const overrideClient = {
      query: vi.fn().mockResolvedValue({ json: overrideJson }),
    };
    const dbWithClient = createClickHouseDB<QueryBuilderTestDB>({
      client: defaultClient,
    });
    const query = dbWithClient.selectFrom("event_logs").selectAll();

    const options = {
      client: overrideClient,
      queryId: "override-query-id",
      clickhouse_settings: {
        max_threads: 2,
      },
    };

    await expect(query.executeTakeFirst(options)).resolves.toBeUndefined();
    await expect(query.executeTakeFirstOrThrow(options)).rejects.toThrow("Query returned no rows.");

    expect(defaultClient.query).not.toHaveBeenCalled();
    expect(overrideClient.query).toHaveBeenCalledTimes(2);
    expect(overrideClient.query).toHaveBeenNthCalledWith(1, {
      query: "SELECT * FROM event_logs",
      query_params: {},
      format: "JSONEachRow",
      query_id: "override-query-id",
      clickhouse_settings: {
        max_threads: 2,
      },
    });
  });

  it("requires a query-capable client for execution", async () => {
    const query = db.selectFrom("event_logs").selectAll();

    await expect(query.execute()).rejects.toThrow(
      "No ClickHouse client configured. Pass one to execute() or createClickHouseDB().",
    );
  });
});
