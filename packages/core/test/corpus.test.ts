import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  Array,
  Date,
  DateTime,
  DateTime64,
  Float64,
  Nullable,
  String,
  UInt32,
  UInt64,
  createClickHouseDB,
  defineSchema,
  table,
} from "../src";
import {
  aggregateFunctionsCase,
  arrayFunctionsCase,
  cteJoinCase,
  cteLeftJoinBaseTableCase,
  dateTimeFunctionsCase,
  finalPrewhereSettingsCase,
  groupByAggregateCase,
  groupByHavingCase,
  havingSubqueryCase,
  inSubqueryCase,
  innerJoinCase,
  joinFinalTableSourceCase,
  joinSubqueryAliasCase,
  joinSubquerySettingsCase,
  jsonExtractCase,
  multiConditionJoinCase,
  multipleCtesCase,
  nullFunctionsCase,
  selectAllCase,
  selectAllForAliasCase,
  setQueryCaseDb,
  simpleSelectCase,
  type SpikeDB,
  stringFunctionsCase,
  typeCastFunctionsCase,
  whereRefCase,
} from "./cases";

const currentDir = dirname(fileURLToPath(import.meta.url));
const queriesDir = resolve(currentDir, "../queries");

const plainDb = createClickHouseDB<SpikeDB>();

const schemaDb = createClickHouseDB({
  schema: defineSchema({
    event_logs: table.replacingMergeTree({
      user_id: UInt32(),
      event_type: String(),
      created_at: DateTime(),
      event_date: Date(),
      properties: String(),
      version: UInt32(),
    }),
    inquiry_downloads: table.replacingMergeTree({
      user_id: UInt32(),
      created_at: DateTime(),
      version: UInt32(),
    }),
    users: table({
      id: UInt32(),
      email: String(),
      status: String(),
    }),
    typed_samples: table({
      id: UInt32(),
      big_user_id: UInt64(),
      label: String(),
      status: String(),
      nickname: Nullable(String()),
      tags: Array(String()),
      amount: Float64(),
      created_at: DateTime64(3),
    }),
  }),
});

const cases = [
  simpleSelectCase,
  finalPrewhereSettingsCase,
  selectAllCase,
  innerJoinCase,
  joinSubqueryAliasCase,
  joinFinalTableSourceCase,
  whereRefCase,
  groupByAggregateCase,
  groupByHavingCase,
  joinSubquerySettingsCase,
  inSubqueryCase,
  multiConditionJoinCase,
  havingSubqueryCase,
  cteJoinCase,
  cteLeftJoinBaseTableCase,
  multipleCtesCase,
  selectAllForAliasCase,
  jsonExtractCase,
  typeCastFunctionsCase,
  arrayFunctionsCase,
  stringFunctionsCase,
  aggregateFunctionsCase,
  nullFunctionsCase,
  dateTimeFunctionsCase,
];

function normalizeSql(sql: string): string {
  return sql.replace(/\(\s+/g, "(").replace(/\s+\)/g, ")").replace(/\s+/g, " ").trim();
}

function runCorpus(label: string, db: typeof plainDb): void {
  describe(label, () => {
    for (const queryCase of cases) {
      it(queryCase.name, () => {
        setQueryCaseDb(db);

        const expectedSql = readFileSync(resolve(queriesDir, queryCase.file), "utf8");
        const compiled = queryCase.build().toSQL();

        expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
        expect(compiled.params).toEqual(queryCase.expectedParams);
      });
    }
  });
}

describe("query corpus spike", () => {
  runCorpus("plain mode", plainDb);
  runCorpus("schema mode", schemaDb);
});
