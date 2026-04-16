import { describe, expect, it } from "vitest";
import { createClickHouseDB, param } from "../src";

interface QueryBuilderTestDB {
  event_logs: {
    user_id: number;
    event_type: string;
  };
  typed_samples: {
    label: string;
    nickname: string | null;
    tags: string[];
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
});
