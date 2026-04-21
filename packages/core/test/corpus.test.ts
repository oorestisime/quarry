import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { createClickHouseDB } from "../src";
import {
  aggregateFunctionsCase,
  arrayFunctionsCase,
  chainedExpressionWhereCase,
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
  chainedExpressionWhereCase,
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
});
