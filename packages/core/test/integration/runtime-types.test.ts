import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClickHouseDB, param, type InferResult } from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";

interface TypedSamplesDB {
  json_samples: {
    id: number;
    payload: {
      user: {
        id: string;
      };
      traits: {
        plan: string;
        active: boolean;
      };
      tags: string[];
      metrics: {
        score: number;
        rank: number;
      };
    };
  };
  typed_samples: {
    id: number;
    big_user_id: string;
    label: string;
    status: "pending" | "active" | "archived";
    nickname: string | null;
    tags: string[];
    amount: number;
    created_at: string;
    location: [number, number];
    attributes: Record<string, string>;
    "metrics.name": string[];
    "metrics.score": number[];
  };
}

const db = createClickHouseDB<TypedSamplesDB>();

function buildTypedSamplesQuery() {
  return db.selectFrom("typed_samples").selectAll().orderBy("id", "asc");
}

function buildTypeCastQuery() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [
      "t.id",
      eb.fn.toInt32("t.id").as("id_i32"),
      eb.fn.toInt64("t.id").as("id_i64"),
      eb.fn.toUInt32("t.id").as("id_u32"),
      eb.fn.toUInt64("t.big_user_id").as("big_user_id_u64"),
      eb.fn.toFloat32("t.id").as("id_f32"),
      eb.fn.toFloat64("t.amount").as("amount_f64"),
      eb.fn.toDate("t.created_at").as("created_date"),
      eb.fn.toDateTime("t.created_at").as("created_at_dt"),
      eb.fn.toDateTime64("t.created_at", 3).as("created_at_dt64"),
      eb.fn.toString("t.id").as("id_text"),
      eb.fn.toDecimal64("t.amount", 2).as("amount_d64"),
      eb.fn.toDecimal128("t.amount", 2).as("amount_d128"),
    ])
    .orderBy("t.id", "asc");
}

function buildArrayFunctionQuery() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [
      "t.id",
      eb.fn.has("t.tags", "trial").as("has_trial"),
      eb.fn.hasAny("t.tags", ["vip", "trial"]).as("has_overlap"),
      eb.fn.hasAll("t.tags", ["new", "trial"]).as("has_required"),
      eb.fn.length("t.tags").as("tag_count"),
      eb.fn.empty("t.tags").as("is_empty"),
      eb.fn.notEmpty("t.tags").as("is_not_empty"),
    ])
    .orderBy("t.id", "asc");
}

function buildStringFunctionQuery() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [
      "t.id",
      eb.fn.like("t.label", "%ph%").as("has_ph"),
      eb.fn.ilike("t.label", "%AL%").as("has_al_insensitive"),
      eb.fn.empty("t.label").as("label_is_empty"),
      eb.fn.notEmpty("t.label").as("label_is_not_empty"),
      eb.fn.concat(eb.ref("t.label"), "-", eb.fn.toString("t.id")).as("label_key"),
      eb.fn.lower("t.label").as("label_lower"),
      eb.fn.upper("t.label").as("label_upper"),
      eb.fn.substring("t.label", 2, 3).as("label_slice"),
      eb.fn.trimBoth(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_trimmed"),
      eb.fn.trimLeft(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_left_trimmed"),
      eb.fn.trimRight(eb.fn.concat("  ", eb.ref("t.label"), "  ")).as("label_right_trimmed"),
    ])
    .where((eb) => eb.fn.notEmpty("t.label"))
    .orderBy("t.id", "asc");
}

function buildNullableStringFunctionQuery() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [
      "t.id",
      eb.fn.like("t.nickname", "%e%").as("nickname_has_e"),
      eb.fn
        .ilike("t.label", param<string | null>(null, "Nullable(String)"))
        .as("label_matches_maybe"),
      eb.fn.empty("t.nickname").as("nickname_is_empty"),
      eb.fn.notEmpty("t.nickname").as("nickname_is_not_empty"),
      eb.fn.concat(eb.ref("t.nickname"), "-", eb.ref("t.label")).as("nickname_key"),
      eb.fn.lower("t.nickname").as("nickname_lower"),
      eb.fn.upper("t.nickname").as("nickname_upper"),
      eb.fn.substring("t.nickname", 1, 1).as("nickname_slice"),
      eb.fn.trimBoth(eb.fn.concat("  ", eb.ref("t.nickname"), "  ")).as("nickname_trimmed"),
      eb.fn.trimLeft(eb.fn.concat("  ", eb.ref("t.nickname"), "  ")).as("nickname_left_trimmed"),
      eb.fn.trimRight(eb.fn.concat("  ", eb.ref("t.nickname"), "  ")).as("nickname_right_trimmed"),
    ])
    .orderBy("t.id", "asc");
}

function buildDateTimeFunctionQuery() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [
      "t.id",
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
    .orderBy("t.id", "asc");
}

function buildNullFunctionQuery() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [
      "t.id",
      eb.fn.isNull("t.nickname").as("nickname_is_null"),
      eb.fn.isNotNull("t.nickname").as("nickname_is_not_null"),
      eb.fn.nullIf("t.label", "beta").as("maybe_label"),
      eb.fn.coalesce("t.nickname", eb.ref("t.label")).as("display_name"),
      eb.fn.coalesce("t.nickname", eb.val("Unknown")).as("display_name_with_literal"),
      eb.fn.ifNull("t.nickname", param("Unknown", "String")).as("nickname_or_default"),
    ])
    .orderBy("t.id", "asc");
}

function buildAggregateFunctionQuery() {
  return db.selectFrom("typed_samples as t").selectExpr((eb) => {
    const isActive = eb.cmp("t.status", "=", "active");

    return [
      eb.fn.count().as("sample_count"),
      eb.fn.countIf(isActive).as("active_samples"),
      eb.fn.sum("t.amount").as("amount_sum"),
      eb.fn.sum("t.big_user_id").as("big_user_id_sum"),
      eb.fn.sumIf("t.amount", isActive).as("active_amount_sum"),
      eb.fn.sumIf("t.big_user_id", isActive).as("active_big_user_id_sum"),
      eb.fn.avg("t.amount").as("amount_avg"),
      eb.fn.avg("t.big_user_id").as("big_user_id_avg"),
      eb.fn.avgIf("t.amount", isActive).as("active_amount_avg"),
      eb.fn.min("t.label").as("min_label"),
      eb.fn.max("t.label").as("max_label"),
      eb.fn.uniq("t.status").as("uniq_statuses"),
      eb.fn.uniqExact("t.status").as("uniq_statuses_exact"),
      eb.fn.uniqIf("t.status", isActive).as("uniq_active_statuses"),
      eb.fn.groupArray("t.label").as("labels"),
      eb.fn.groupArray("t.nickname").as("nicknames"),
      eb.fn.any("t.label").as("any_label"),
      eb.fn.anyLast("t.label").as("any_last_label"),
    ];
  });
}

function buildJsonSamplesQuery() {
  return db.selectFrom("json_samples").selectAll().orderBy("id", "asc");
}

function buildHeavyHitterFunctionQuery() {
  return db
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
    .orderBy("t.id", "asc");
}

function buildCountDistinctQuery() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [eb.fn.countDistinct("t.label").as("distinct_labels")]);
}

function buildNow64Query() {
  return db
    .selectFrom("typed_samples as t")
    .selectExpr((eb) => [eb.fn.now64(3).as("current_time_precise")])
    .limit(1);
}

type TypedSampleRow = InferResult<ReturnType<typeof buildTypedSamplesQuery>>;
type TypeCastRow = InferResult<ReturnType<typeof buildTypeCastQuery>>;
type ArrayFunctionRow = InferResult<ReturnType<typeof buildArrayFunctionQuery>>;
type StringFunctionRow = InferResult<ReturnType<typeof buildStringFunctionQuery>>;
type NullableStringFunctionRow = InferResult<ReturnType<typeof buildNullableStringFunctionQuery>>;
type DateTimeFunctionRow = InferResult<ReturnType<typeof buildDateTimeFunctionQuery>>;
type NullFunctionRow = InferResult<ReturnType<typeof buildNullFunctionQuery>>;
type AggregateFunctionRow = InferResult<ReturnType<typeof buildAggregateFunctionQuery>>;
type JsonSampleRow = InferResult<ReturnType<typeof buildJsonSamplesQuery>>;
type HeavyHitterFunctionRow = InferResult<ReturnType<typeof buildHeavyHitterFunctionQuery>>;

const expectedRows: TypedSampleRow[] = [
  {
    id: 1,
    big_user_id: "9007199254740993",
    label: "alpha",
    status: "active",
    nickname: null,
    tags: ["new", "trial"],
    amount: 123.45,
    created_at: "2025-01-01 10:11:12.123",
    location: [1.25, 9.5],
    attributes: { source: "ads", campaign: "winter" },
    "metrics.name": ["clicks", "opens"],
    "metrics.score": [10, 5],
  },
  {
    id: 2,
    big_user_id: "42",
    label: "beta",
    status: "pending",
    nickname: "bee",
    tags: [],
    amount: 0.1,
    created_at: "2025-01-02 03:04:05.678",
    location: [0, 1.5],
    attributes: { source: "email" },
    "metrics.name": ["views"],
    "metrics.score": [99],
  },
];

const expectedJsonRows: JsonSampleRow[] = [
  {
    id: 1,
    payload: {
      user: { id: "9007199254740993" },
      traits: { plan: "pro", active: true },
      tags: ["new", "trial"],
      metrics: { score: 12.5, rank: 3 },
    },
  },
  {
    id: 2,
    payload: {
      user: { id: "42" },
      traits: { plan: "free", active: false },
      tags: [],
      metrics: { score: 0.25, rank: 8 },
    },
  },
];

const expectedTypeCastRows: TypeCastRow[] = [
  {
    id: 1,
    id_i32: 1,
    id_i64: "1",
    id_u32: 1,
    big_user_id_u64: "9007199254740993",
    id_f32: 1,
    amount_f64: 123.45,
    created_date: "2025-01-01",
    created_at_dt: "2025-01-01 10:11:12",
    created_at_dt64: "2025-01-01 10:11:12.123",
    id_text: "1",
    amount_d64: 123.45,
    amount_d128: 123.45,
  },
  {
    id: 2,
    id_i32: 2,
    id_i64: "2",
    id_u32: 2,
    big_user_id_u64: "42",
    id_f32: 2,
    amount_f64: 0.1,
    created_date: "2025-01-02",
    created_at_dt: "2025-01-02 03:04:05",
    created_at_dt64: "2025-01-02 03:04:05.678",
    id_text: "2",
    amount_d64: 0.1,
    amount_d128: 0.1,
  },
];

const expectedArrayFunctionRows: ArrayFunctionRow[] = [
  {
    id: 1,
    has_trial: 1,
    has_overlap: 1,
    has_required: 1,
    tag_count: "2",
    is_empty: 0,
    is_not_empty: 1,
  },
  {
    id: 2,
    has_trial: 0,
    has_overlap: 0,
    has_required: 0,
    tag_count: "0",
    is_empty: 1,
    is_not_empty: 0,
  },
];

const expectedStringFunctionRows: StringFunctionRow[] = [
  {
    id: 1,
    has_ph: 1,
    has_al_insensitive: 1,
    label_is_empty: 0,
    label_is_not_empty: 1,
    label_key: "alpha-1",
    label_lower: "alpha",
    label_upper: "ALPHA",
    label_slice: "lph",
    label_trimmed: "alpha",
    label_left_trimmed: "alpha  ",
    label_right_trimmed: "  alpha",
  },
  {
    id: 2,
    has_ph: 0,
    has_al_insensitive: 0,
    label_is_empty: 0,
    label_is_not_empty: 1,
    label_key: "beta-2",
    label_lower: "beta",
    label_upper: "BETA",
    label_slice: "eta",
    label_trimmed: "beta",
    label_left_trimmed: "beta  ",
    label_right_trimmed: "  beta",
  },
];

const expectedNullableStringFunctionRows: NullableStringFunctionRow[] = [
  {
    id: 1,
    nickname_has_e: null,
    label_matches_maybe: null,
    nickname_is_empty: null,
    nickname_is_not_empty: null,
    nickname_key: null,
    nickname_lower: null,
    nickname_upper: null,
    nickname_slice: null,
    nickname_trimmed: null,
    nickname_left_trimmed: null,
    nickname_right_trimmed: null,
  },
  {
    id: 2,
    nickname_has_e: 1,
    label_matches_maybe: null,
    nickname_is_empty: 0,
    nickname_is_not_empty: 1,
    nickname_key: "bee-beta",
    nickname_lower: "bee",
    nickname_upper: "BEE",
    nickname_slice: "b",
    nickname_trimmed: "bee",
    nickname_left_trimmed: "bee  ",
    nickname_right_trimmed: "  bee",
  },
];

const expectedDateTimeFunctionRows: Array<
  Pick<
    DateTimeFunctionRow,
    "id" | "created_date_text" | "days_until_cutoff" | "created_yyyymm" | "created_yyyymmdd"
  >
> = [
  {
    id: 1,
    created_date_text: "2025-01-01",
    days_until_cutoff: "2",
    created_yyyymm: 202501,
    created_yyyymmdd: 20250101,
  },
  {
    id: 2,
    created_date_text: "2025-01-02",
    days_until_cutoff: "1",
    created_yyyymm: 202501,
    created_yyyymmdd: 20250102,
  },
];

const expectedNullFunctionRows: NullFunctionRow[] = [
  {
    id: 1,
    nickname_is_null: 1,
    nickname_is_not_null: 0,
    maybe_label: "alpha",
    display_name: "alpha",
    display_name_with_literal: "Unknown",
    nickname_or_default: "Unknown",
  },
  {
    id: 2,
    nickname_is_null: 0,
    nickname_is_not_null: 1,
    maybe_label: null,
    display_name: "bee",
    display_name_with_literal: "bee",
    nickname_or_default: "bee",
  },
];

const expectedAggregateFunctionRows: AggregateFunctionRow[] = [
  {
    sample_count: "2",
    active_samples: "1",
    amount_sum: 123.55,
    big_user_id_sum: "9007199254741035",
    active_amount_sum: 123.45,
    active_big_user_id_sum: "9007199254740993",
    amount_avg: 61.775,
    big_user_id_avg: 4503599627370518,
    active_amount_avg: 123.45,
    min_label: "alpha",
    max_label: "beta",
    uniq_statuses: "2",
    uniq_statuses_exact: "2",
    uniq_active_statuses: "1",
    labels: ["alpha", "beta"],
    nicknames: ["bee"],
    any_label: "alpha",
    any_last_label: "beta",
  },
];

const expectedHeavyHitterRows: HeavyHitterFunctionRow[] = [
  {
    id: 1,
    status_label: "alpha",
    least_val: 1,
    greatest_val: 10,
    ceil_amount: 124,
    floor_amount: 123,
    id_u8: 1,
    created_year: 2025,
    created_month: 1,
  },
  {
    id: 2,
    status_label: "inactive",
    least_val: 2,
    greatest_val: 10,
    ceil_amount: 1,
    floor_amount: 0,
    id_u8: 2,
    created_year: 2025,
    created_month: 1,
  },
];

let context: ClickHouseTestContext | undefined;

describe("clickhouse runtime types", () => {
  function getContext(): ClickHouseTestContext {
    if (!context) {
      throw new Error("ClickHouse test context was not initialized");
    }

    return context;
  }

  beforeAll(async () => {
    context = await startClickHouse();
  });

  afterAll(async () => {
    await stopClickHouse(context);
  });

  it("returns raw JSONEachRow-compatible values for awkward ClickHouse types", async () => {
    const rows = await buildTypedSamplesQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedRows);
    expect(typeof rows[0].id).toBe("number");
    expect(typeof rows[0].big_user_id).toBe("string");
    expect(typeof rows[0].label).toBe("string");
    expect(typeof rows[0].status).toBe("string");
    expect(rows[0].nickname).toBeNull();
    expect(Array.isArray(rows[0].tags)).toBe(true);
    expect(typeof rows[0].amount).toBe("number");
    expect(typeof rows[0].created_at).toBe("string");
    expect(Array.isArray(rows[0].location)).toBe(true);
    expect(rows[0].attributes).toEqual({ source: "ads", campaign: "winter" });
    expect(rows[0]["metrics.name"]).toEqual(["clicks", "opens"]);
    expect(rows[0]["metrics.score"]).toEqual([10, 5]);
  });

  it("returns structured objects for JSON columns with typed paths", async () => {
    const rows = await buildJsonSamplesQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedJsonRows);
    expect(typeof rows[0].payload.user.id).toBe("string");
    expect(typeof rows[0].payload.traits.plan).toBe("string");
    expect(typeof rows[0].payload.traits.active).toBe("boolean");
    expect(Array.isArray(rows[0].payload.tags)).toBe(true);
    expect(typeof rows[0].payload.metrics.score).toBe("number");
    expect(typeof rows[0].payload.metrics.rank).toBe("number");
  });

  it("returns runtime-honest values for type conversion helpers", async () => {
    const rows = await buildTypeCastQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedTypeCastRows);
    expect(typeof rows[0].id_i32).toBe("number");
    expect(typeof rows[0].id_i64).toBe("string");
    expect(typeof rows[0].id_u32).toBe("number");
    expect(typeof rows[0].big_user_id_u64).toBe("string");
    expect(typeof rows[0].id_f32).toBe("number");
    expect(typeof rows[0].amount_f64).toBe("number");
    expect(typeof rows[0].created_date).toBe("string");
    expect(typeof rows[0].created_at_dt).toBe("string");
    expect(typeof rows[0].created_at_dt64).toBe("string");
    expect(typeof rows[0].id_text).toBe("string");
    expect(typeof rows[0].amount_d64).toBe("number");
    expect(typeof rows[0].amount_d128).toBe("number");
  });

  it("returns runtime-honest values for array function helpers", async () => {
    const rows = await buildArrayFunctionQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedArrayFunctionRows);
    expect(typeof rows[0].has_trial).toBe("number");
    expect(typeof rows[0].has_overlap).toBe("number");
    expect(typeof rows[0].has_required).toBe("number");
    expect(typeof rows[0].tag_count).toBe("string");
    expect(typeof rows[0].is_empty).toBe("number");
    expect(typeof rows[0].is_not_empty).toBe("number");
  });

  it("returns runtime-honest values for string function helpers", async () => {
    const rows = await buildStringFunctionQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedStringFunctionRows);
    expect(typeof rows[0].has_ph).toBe("number");
    expect(typeof rows[0].has_al_insensitive).toBe("number");
    expect(typeof rows[0].label_is_empty).toBe("number");
    expect(typeof rows[0].label_is_not_empty).toBe("number");
    expect(typeof rows[0].label_key).toBe("string");
    expect(typeof rows[0].label_lower).toBe("string");
    expect(typeof rows[0].label_upper).toBe("string");
    expect(typeof rows[0].label_slice).toBe("string");
    expect(typeof rows[0].label_trimmed).toBe("string");
    expect(typeof rows[0].label_left_trimmed).toBe("string");
    expect(typeof rows[0].label_right_trimmed).toBe("string");
  });

  it("propagates nulls through nullable string helpers", async () => {
    const rows = await buildNullableStringFunctionQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedNullableStringFunctionRows);
    expect(rows[0].nickname_has_e).toBeNull();
    expect(rows[0].label_matches_maybe).toBeNull();
    expect(rows[0].nickname_is_empty).toBeNull();
    expect(rows[0].nickname_is_not_empty).toBeNull();
    expect(rows[0].nickname_key).toBeNull();
    expect(rows[0].nickname_lower).toBeNull();
    expect(rows[0].nickname_upper).toBeNull();
    expect(rows[0].nickname_slice).toBeNull();
    expect(rows[0].nickname_trimmed).toBeNull();
    expect(rows[0].nickname_left_trimmed).toBeNull();
    expect(rows[0].nickname_right_trimmed).toBeNull();
    expect(typeof rows[1].nickname_has_e).toBe("number");
    expect(rows[1].label_matches_maybe).toBeNull();
    expect(typeof rows[1].nickname_is_empty).toBe("number");
    expect(typeof rows[1].nickname_is_not_empty).toBe("number");
    expect(typeof rows[1].nickname_key).toBe("string");
    expect(typeof rows[1].nickname_lower).toBe("string");
    expect(typeof rows[1].nickname_upper).toBe("string");
    expect(typeof rows[1].nickname_slice).toBe("string");
    expect(typeof rows[1].nickname_trimmed).toBe("string");
    expect(typeof rows[1].nickname_left_trimmed).toBe("string");
    expect(typeof rows[1].nickname_right_trimmed).toBe("string");
  });

  it("returns runtime-honest values for date/time helpers", async () => {
    const rows = await buildDateTimeFunctionQuery().execute({ client: getContext().client });

    expect(
      rows.map(
        ({ id, created_date_text, days_until_cutoff, created_yyyymm, created_yyyymmdd }) => ({
          id,
          created_date_text,
          days_until_cutoff,
          created_yyyymm,
          created_yyyymmdd,
        }),
      ),
    ).toEqual(expectedDateTimeFunctionRows);

    expect(typeof rows[0].current_time).toBe("string");
    expect(typeof rows[0].current_date).toBe("string");
    expect(rows[0].current_time.startsWith(rows[0].current_date)).toBe(true);
    expect(rows[0].current_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(rows[0].month_start).toMatch(/^2025-01-01/);
    expect(rows[0].week_start).toMatch(/^202[45]-\d{2}-\d{2}/);
    expect(rows[0].week_start).toBe(rows[1].week_start);
    expect(rows[0].day_start).toMatch(/^2025-01-01/);
    expect(rows[1].day_start).toMatch(/^2025-01-02/);
    expect(rows[0].year_start).toMatch(/^2025-01-01/);
    expect(typeof rows[0].created_date_text).toBe("string");
    expect(typeof rows[0].days_until_cutoff).toBe("string");
    expect(rows[0].plus_five_days).toMatch(/^2025-01-06 10:11:12/);
    expect(rows[1].plus_five_days).toMatch(/^2025-01-07 03:04:05/);
    expect(rows[0].minus_two_hours).toMatch(/^2025-01-01 08:11:12/);
    expect(rows[1].minus_two_hours).toMatch(/^2025-01-02 01:04:05/);
    expect(typeof rows[0].created_yyyymm).toBe("number");
    expect(typeof rows[0].created_yyyymmdd).toBe("number");
  });

  it("returns runtime-honest values for null helpers", async () => {
    const rows = await buildNullFunctionQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedNullFunctionRows);
    expect(typeof rows[0].nickname_is_null).toBe("number");
    expect(typeof rows[0].nickname_is_not_null).toBe("number");
    expect(typeof rows[0].maybe_label).toBe("string");
    expect(typeof rows[0].display_name).toBe("string");
    expect(typeof rows[0].display_name_with_literal).toBe("string");
    expect(typeof rows[0].nickname_or_default).toBe("string");
    expect(rows[1].maybe_label).toBeNull();
    expect(typeof rows[1].display_name).toBe("string");
    expect(typeof rows[1].display_name_with_literal).toBe("string");
    expect(typeof rows[1].nickname_or_default).toBe("string");
  });

  it("returns runtime-honest values for aggregate helpers", async () => {
    const rows = await buildAggregateFunctionQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedAggregateFunctionRows);
    expect(typeof rows[0].sample_count).toBe("string");
    expect(typeof rows[0].active_samples).toBe("string");
    expect(typeof rows[0].amount_sum).toBe("number");
    expect(typeof rows[0].big_user_id_sum).toBe("string");
    expect(typeof rows[0].active_amount_sum).toBe("number");
    expect(typeof rows[0].active_big_user_id_sum).toBe("string");
    expect(typeof rows[0].amount_avg).toBe("number");
    expect(typeof rows[0].big_user_id_avg).toBe("number");
    expect(typeof rows[0].active_amount_avg).toBe("number");
    expect(typeof rows[0].min_label).toBe("string");
    expect(typeof rows[0].max_label).toBe("string");
    expect(typeof rows[0].uniq_statuses).toBe("string");
    expect(typeof rows[0].uniq_statuses_exact).toBe("string");
    expect(typeof rows[0].uniq_active_statuses).toBe("string");
    expect(Array.isArray(rows[0].labels)).toBe(true);
    expect(rows[0].nicknames).toEqual(["bee"]);
    expect(typeof rows[0].any_label).toBe("string");
    expect(typeof rows[0].any_last_label).toBe("string");
  });

  it("returns runtime-honest values for heavy-hitter helpers", async () => {
    const rows = await buildHeavyHitterFunctionQuery().execute({ client: getContext().client });

    expect(rows).toEqual(expectedHeavyHitterRows);
    expect(typeof rows[0].ceil_amount).toBe("number");
    expect(typeof rows[0].floor_amount).toBe("number");
    expect(typeof rows[0].id_u8).toBe("number");
    expect(typeof rows[0].created_year).toBe("number");
    expect(typeof rows[0].created_month).toBe("number");
  });

  it("returns runtime-honest values for countDistinct", async () => {
    const rows = await buildCountDistinctQuery().execute({ client: getContext().client });

    expect(rows).toEqual([{ distinct_labels: "2" }]);
    expect(typeof rows[0].distinct_labels).toBe("string");
  });

  it("returns runtime-honest values for now64", async () => {
    const rows = await buildNow64Query().execute({ client: getContext().client });

    expect(typeof rows[0].current_time_precise).toBe("string");
    expect(rows[0].current_time_precise).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
  });
});
