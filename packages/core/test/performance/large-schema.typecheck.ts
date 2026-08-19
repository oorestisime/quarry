import {
  createClickHouseDB,
  type InferResult,
  type TypedDictionary,
  type TypedTable,
  type TypedView,
} from "../../src";

type Digit = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";
type TwoDigits = `${Digit}${Digit}`;
type TableName = `table_${"a" | "b" | "c" | "d"}${TwoDigits}`;
type ViewName = `view_${"a" | "b" | "c"}${TwoDigits}`;
type DictionaryName = `dictionary_${Digit}`;

interface LargeRow {
  id: number;
  account_id: number;
  category: string;
  label: string | null;
  tags: string[];
  event_time: string;
}

interface DictionaryRow {
  label: string;
  category: string;
}

type LargeSchema = {
  [Name in TableName]: TypedTable<LargeRow>;
} & {
  [Name in ViewName]: TypedView<LargeRow>;
} & {
  [Name in DictionaryName]: TypedDictionary<DictionaryRow>;
};

const db = createClickHouseDB<LargeSchema>();

const directQuery = db
  .selectFrom("table_a00")
  .select("id", "account_id", "category")
  .where("account_id", "=", 42)
  .orderBy("event_time", "desc");

const aliasedQuery = db
  .selectFrom("table_b17 as source")
  .innerJoin("view_c42 as aggregate", "source.account_id", "aggregate.account_id")
  .select("source.id", "aggregate.category as aggregate_category")
  .where("source.category", "=", "active");

const dictionaryQuery = db
  .selectFrom("table_d99 as source")
  .selectExpr((eb) => [
    "source.id",
    eb.fn.dictGet("dictionary_0", "label", "source.account_id").as("account_label"),
  ]);

const finalQuery = db.selectFrom(db.table("table_c73").as("source").final()).selectAll("source");

const viewQuery = db.selectFrom("view_a11").select("id", "category");

db.insertInto("table_a00").values([
  {
    id: 1,
    account_id: 42,
    category: "active",
    label: null,
    tags: ["large-schema"],
    event_time: "2026-01-01 00:00:00",
  },
]);

// @ts-expect-error dictionaries cannot be selected as query sources
db.selectFrom("dictionary_0");

// @ts-expect-error views cannot be inserted into
db.insertInto("view_a11");

db.selectFrom("table_a00").selectExpr((eb) => [
  // @ts-expect-error tables cannot be used as dictionaries
  eb.fn.dictGet("table_a00", "label", "account_id").as("invalid_dictionary"),
]);

type DirectRow = InferResult<typeof directQuery>;
type AliasedRow = InferResult<typeof aliasedQuery>;
type DictionaryResultRow = InferResult<typeof dictionaryQuery>;
type FinalRow = InferResult<typeof finalQuery>;
type ViewRow = InferResult<typeof viewQuery>;

declare const directRow: DirectRow;
declare const aliasedRow: AliasedRow;
declare const dictionaryRow: DictionaryResultRow;
declare const finalRow: FinalRow;
declare const viewRow: ViewRow;

void directRow;
void aliasedRow;
void dictionaryRow;
void finalRow;
void viewRow;
