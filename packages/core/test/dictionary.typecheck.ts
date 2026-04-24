import {
  type ClickHouseDate,
  type ClickHouseInt64,
  createClickHouseDB,
  type InferResult,
  type TypedDictionary,
} from "../src";

interface DictionaryTypecheckDB {
  events: {
    partner_id: number;
    event_date: string;
    country_code: string;
  };
  partner_rates: TypedDictionary<{
    rate_cents: number;
    currency: string;
    effective_date: ClickHouseDate;
    signed_total: ClickHouseInt64;
  }>;
  country_names: TypedDictionary<{
    name: string;
    region: string;
  }>;
}

const dictDb = createClickHouseDB<DictionaryTypecheckDB>();

// @ts-expect-error dictionaries are not selectable query sources
dictDb.selectFrom("partner_rates");

// @ts-expect-error dictionaries are not table sources
dictDb.table("partner_rates");

// @ts-expect-error dictionaries are not insertable sources
dictDb.insertInto("partner_rates");

const dictGetQuery = dictDb
  .selectFrom("events as e")
  .selectExpr((eb) => [
    eb.fn.dictGet("partner_rates", "rate_cents", "e.partner_id").as("rate"),
    eb.fn.dictGet("country_names", "name", "e.country_code").as("country_name"),
    eb.fn.dictGet("partner_rates", "effective_date", "e.partner_id").as("effective_date"),
    eb.fn.dictGet("partner_rates", "signed_total", "e.partner_id").as("signed_total"),
    eb.fn
      .dictGet("partner_rates", "rate_cents", ["e.partner_id", "e.event_date"])
      .as("composite_rate"),
    eb.fn
      .dictGetOrDefault("country_names", "name", "e.country_code", "Unknown")
      .as("country_or_default"),
    eb.fn
      .dictGetOrDefault("partner_rates", "effective_date", "e.partner_id", "1970-01-01")
      .as("effective_date_or_default"),
    eb.fn.dictHas("partner_rates", "e.partner_id").as("has_rate"),
  ]);

type DictGetRow = InferResult<typeof dictGetQuery>;

const validDictGetRow: DictGetRow = {
  rate: 100,
  country_name: "United States",
  effective_date: "2025-01-01",
  signed_total: "42",
  composite_rate: 100,
  country_or_default: "Unknown",
  effective_date_or_default: "1970-01-01",
  has_rate: 1,
};

void validDictGetRow;

dictDb.selectFrom("events as e").selectExpr((eb) => [
  // @ts-expect-error invalid dictionary name
  eb.fn.dictGet("invalid_dict", "rate_cents", "e.partner_id").as("bad_dict"),
  // @ts-expect-error invalid attribute name for valid dictionary
  eb.fn.dictGet("partner_rates", "invalid_attr", "e.partner_id").as("bad_attr"),
  // @ts-expect-error wrong default value type for dictGetOrDefault
  eb.fn.dictGetOrDefault("partner_rates", "rate_cents", "e.partner_id", "wrong").as("bad_default"),
  eb.fn
    // @ts-expect-error wrapped dictionary attribute defaults use the unwrapped select type
    .dictGetOrDefault("partner_rates", "effective_date", "e.partner_id", 0)
    .as("bad_date_default"),
]);
