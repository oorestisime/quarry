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

type TypedSampleRow = InferResult<ReturnType<typeof buildTypedSamplesQuery>>;
type TypeCastRow = InferResult<ReturnType<typeof buildTypeCastQuery>>;
type ArrayFunctionRow = InferResult<ReturnType<typeof buildArrayFunctionQuery>>;
type StringFunctionRow = InferResult<ReturnType<typeof buildStringFunctionQuery>>;
type NullableStringFunctionRow = InferResult<ReturnType<typeof buildNullableStringFunctionQuery>>;
type AggregateFunctionRow = InferResult<ReturnType<typeof buildAggregateFunctionQuery>>;
type JsonSampleRow = InferResult<ReturnType<typeof buildJsonSamplesQuery>>;

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
    const rows = await buildTypedSamplesQuery().execute(getContext().client);

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
    const rows = await buildJsonSamplesQuery().execute(getContext().client);

    expect(rows).toEqual(expectedJsonRows);
    expect(typeof rows[0].payload.user.id).toBe("string");
    expect(typeof rows[0].payload.traits.plan).toBe("string");
    expect(typeof rows[0].payload.traits.active).toBe("boolean");
    expect(Array.isArray(rows[0].payload.tags)).toBe(true);
    expect(typeof rows[0].payload.metrics.score).toBe("number");
    expect(typeof rows[0].payload.metrics.rank).toBe("number");
  });

  it("returns runtime-honest values for type conversion helpers", async () => {
    const rows = await buildTypeCastQuery().execute(getContext().client);

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
    const rows = await buildArrayFunctionQuery().execute(getContext().client);

    expect(rows).toEqual(expectedArrayFunctionRows);
    expect(typeof rows[0].has_trial).toBe("number");
    expect(typeof rows[0].has_overlap).toBe("number");
    expect(typeof rows[0].has_required).toBe("number");
    expect(typeof rows[0].tag_count).toBe("string");
    expect(typeof rows[0].is_empty).toBe("number");
    expect(typeof rows[0].is_not_empty).toBe("number");
  });

  it("returns runtime-honest values for string function helpers", async () => {
    const rows = await buildStringFunctionQuery().execute(getContext().client);

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
    const rows = await buildNullableStringFunctionQuery().execute(getContext().client);

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

  it("returns runtime-honest values for aggregate helpers", async () => {
    const rows = await buildAggregateFunctionQuery().execute(getContext().client);

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
});
