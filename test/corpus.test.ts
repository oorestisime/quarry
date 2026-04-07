import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  cteLeftJoinBaseTableCase,
  cteJoinCase,
  finalPrewhereSettingsCase,
  groupByAggregateCase,
  groupByHavingCase,
  inSubqueryCase,
  innerJoinCase,
  joinFinalTableSourceCase,
  joinSubquerySettingsCase,
  joinSubqueryAliasCase,
  jsonExtractCase,
  havingSubqueryCase,
  multiConditionJoinCase,
  multipleCtesCase,
  arrayFunctionsCase,
  selectAllCase,
  selectAllForAliasCase,
  simpleSelectCase,
  typeCastFunctionsCase,
  whereRefCase,
} from "./cases";

const currentDir = dirname(fileURLToPath(import.meta.url));
const queriesDir = resolve(currentDir, "../queries");

function normalizeSql(sql: string): string {
  return sql.replace(/\(\s+/g, "(").replace(/\s+\)/g, ")").replace(/\s+/g, " ").trim();
}

describe("query corpus spike", () => {
  it(simpleSelectCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, simpleSelectCase.file), "utf8");
    const compiled = simpleSelectCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(simpleSelectCase.expectedParams);
  });

  it(finalPrewhereSettingsCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, finalPrewhereSettingsCase.file), "utf8");
    const compiled = finalPrewhereSettingsCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(finalPrewhereSettingsCase.expectedParams);
  });

  it(selectAllCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, selectAllCase.file), "utf8");
    const compiled = selectAllCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(selectAllCase.expectedParams);
  });

  it(innerJoinCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, innerJoinCase.file), "utf8");
    const compiled = innerJoinCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(innerJoinCase.expectedParams);
  });

  it(joinSubqueryAliasCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, joinSubqueryAliasCase.file), "utf8");
    const compiled = joinSubqueryAliasCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(joinSubqueryAliasCase.expectedParams);
  });

  it(joinFinalTableSourceCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, joinFinalTableSourceCase.file), "utf8");
    const compiled = joinFinalTableSourceCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(joinFinalTableSourceCase.expectedParams);
  });

  it(whereRefCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, whereRefCase.file), "utf8");
    const compiled = whereRefCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(whereRefCase.expectedParams);
  });

  it(groupByAggregateCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, groupByAggregateCase.file), "utf8");
    const compiled = groupByAggregateCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(groupByAggregateCase.expectedParams);
  });

  it(groupByHavingCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, groupByHavingCase.file), "utf8");
    const compiled = groupByHavingCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(groupByHavingCase.expectedParams);
  });

  it(joinSubquerySettingsCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, joinSubquerySettingsCase.file), "utf8");
    const compiled = joinSubquerySettingsCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(joinSubquerySettingsCase.expectedParams);
  });

  it(inSubqueryCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, inSubqueryCase.file), "utf8");
    const compiled = inSubqueryCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(inSubqueryCase.expectedParams);
  });

  it(multiConditionJoinCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, multiConditionJoinCase.file), "utf8");
    const compiled = multiConditionJoinCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(multiConditionJoinCase.expectedParams);
  });

  it(havingSubqueryCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, havingSubqueryCase.file), "utf8");
    const compiled = havingSubqueryCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(havingSubqueryCase.expectedParams);
  });

  it(cteJoinCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, cteJoinCase.file), "utf8");
    const compiled = cteJoinCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(cteJoinCase.expectedParams);
  });

  it(cteLeftJoinBaseTableCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, cteLeftJoinBaseTableCase.file), "utf8");
    const compiled = cteLeftJoinBaseTableCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(cteLeftJoinBaseTableCase.expectedParams);
  });

  it(multipleCtesCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, multipleCtesCase.file), "utf8");
    const compiled = multipleCtesCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(multipleCtesCase.expectedParams);
  });

  it(selectAllForAliasCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, selectAllForAliasCase.file), "utf8");
    const compiled = selectAllForAliasCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(selectAllForAliasCase.expectedParams);
  });

  it(jsonExtractCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, jsonExtractCase.file), "utf8");
    const compiled = jsonExtractCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(jsonExtractCase.expectedParams);
  });

  it(typeCastFunctionsCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, typeCastFunctionsCase.file), "utf8");
    const compiled = typeCastFunctionsCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(typeCastFunctionsCase.expectedParams);
  });

  it(arrayFunctionsCase.name, () => {
    const expectedSql = readFileSync(resolve(queriesDir, arrayFunctionsCase.file), "utf8");
    const compiled = arrayFunctionsCase.build().toSQL();

    expect(normalizeSql(compiled.query)).toBe(normalizeSql(expectedSql));
    expect(compiled.params).toEqual(arrayFunctionsCase.expectedParams);
  });
});
