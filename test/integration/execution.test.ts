import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";
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
  selectAllCase,
  selectAllForAliasCase,
  simpleSelectCase,
  whereRefCase,
} from "../cases";

let context: ClickHouseTestContext | undefined;

describe("clickhouse integration", () => {
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

  it(simpleSelectCase.name, async () => {
    const rows = await simpleSelectCase.build().execute(getContext().client);
    expect(rows).toEqual(simpleSelectCase.expectedRows);
  });

  it(finalPrewhereSettingsCase.name, async () => {
    const rows = await finalPrewhereSettingsCase.build().execute(getContext().client);
    expect(rows).toEqual(finalPrewhereSettingsCase.expectedRows);
  });

  it(selectAllCase.name, async () => {
    const rows = await selectAllCase.build().execute(getContext().client);
    expect(rows).toEqual(selectAllCase.expectedRows);
  });

  it(innerJoinCase.name, async () => {
    const rows = await innerJoinCase.build().execute(getContext().client);
    expect(rows).toEqual(innerJoinCase.expectedRows);
  });

  it(joinSubqueryAliasCase.name, async () => {
    const rows = await joinSubqueryAliasCase.build().execute(getContext().client);
    expect(rows).toEqual(joinSubqueryAliasCase.expectedRows);
  });

  it(joinFinalTableSourceCase.name, async () => {
    const rows = await joinFinalTableSourceCase.build().execute(getContext().client);
    expect(rows).toEqual(joinFinalTableSourceCase.expectedRows);
  });

  it(whereRefCase.name, async () => {
    const rows = await whereRefCase.build().execute(getContext().client);
    expect(rows).toEqual(whereRefCase.expectedRows);
  });

  it(groupByAggregateCase.name, async () => {
    const rows = await groupByAggregateCase.build().execute(getContext().client);
    expect(rows).toEqual(groupByAggregateCase.expectedRows);
  });

  it(groupByHavingCase.name, async () => {
    const rows = await groupByHavingCase.build().execute(getContext().client);
    expect(rows).toEqual(groupByHavingCase.expectedRows);
  });

  it(joinSubquerySettingsCase.name, async () => {
    const rows = await joinSubquerySettingsCase.build().execute(getContext().client);
    expect(rows).toEqual(joinSubquerySettingsCase.expectedRows);
  });

  it(inSubqueryCase.name, async () => {
    const rows = await inSubqueryCase.build().execute(getContext().client);
    expect(rows).toEqual(inSubqueryCase.expectedRows);
  });

  it(multiConditionJoinCase.name, async () => {
    const rows = await multiConditionJoinCase.build().execute(getContext().client);
    expect(rows).toEqual(multiConditionJoinCase.expectedRows);
  });

  it(havingSubqueryCase.name, async () => {
    const rows = await havingSubqueryCase.build().execute(getContext().client);
    expect(rows).toEqual(havingSubqueryCase.expectedRows);
  });

  it(cteJoinCase.name, async () => {
    const rows = await cteJoinCase.build().execute(getContext().client);
    expect(rows).toEqual(cteJoinCase.expectedRows);
  });

  it(cteLeftJoinBaseTableCase.name, async () => {
    const rows = await cteLeftJoinBaseTableCase.build().execute(getContext().client);
    expect(rows).toEqual(cteLeftJoinBaseTableCase.expectedRows);
  });

  it(multipleCtesCase.name, async () => {
    const rows = await multipleCtesCase.build().execute(getContext().client);
    expect(rows).toEqual(multipleCtesCase.expectedRows);
  });

  it(selectAllForAliasCase.name, async () => {
    const rows = await selectAllForAliasCase.build().execute(getContext().client);
    expect(rows).toEqual(selectAllForAliasCase.expectedRows);
  });

  it(jsonExtractCase.name, async () => {
    const rows = await jsonExtractCase.build().execute(getContext().client);
    expect(rows).toEqual(jsonExtractCase.expectedRows);
  });
});
