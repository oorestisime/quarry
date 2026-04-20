import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClickHouseDB } from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";
import {
  aggregateFunctionsCase,
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
  dateTimeFunctionsCase,
  nullFunctionsCase,
  selectAllCase,
  selectAllForAliasCase,
  simpleSelectCase,
  stringFunctionsCase,
  typeCastFunctionsCase,
  whereRefCase,
} from "../cases";

interface ExecutionTestDB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
    event_date: string;
    properties: string;
    version: number;
  };
  users: {
    id: number;
    email: string;
    status: string;
  };
  typed_samples: {
    id: number;
    tags: string[];
  };
}

const db = createClickHouseDB<ExecutionTestDB>();

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

  it(typeCastFunctionsCase.name, async () => {
    const rows = await typeCastFunctionsCase.build().execute(getContext().client);
    expect(rows).toEqual(typeCastFunctionsCase.expectedRows);
  });

  it(arrayFunctionsCase.name, async () => {
    const rows = await arrayFunctionsCase.build().execute(getContext().client);
    expect(rows).toEqual(arrayFunctionsCase.expectedRows);
  });

  it(stringFunctionsCase.name, async () => {
    const rows = await stringFunctionsCase.build().execute(getContext().client);
    expect(rows).toEqual(stringFunctionsCase.expectedRows);
  });

  it(aggregateFunctionsCase.name, async () => {
    const rows = await aggregateFunctionsCase.build().execute(getContext().client);
    expect(rows).toEqual(aggregateFunctionsCase.expectedRows);
  });

  it(nullFunctionsCase.name, async () => {
    const rows = await nullFunctionsCase.build().execute(getContext().client);
    expect(rows).toEqual(nullFunctionsCase.expectedRows);
  });

  it(dateTimeFunctionsCase.name, async () => {
    const rows = await dateTimeFunctionsCase.build().execute(getContext().client);
    expect(rows).toEqual(dateTimeFunctionsCase.expectedRows);
  });

  it("executes unary where expression predicates", async () => {
    const rows = await db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => ["t.id", eb.fn.length("t.tags").as("tag_count")])
      .where((eb) => eb.fn.notEmpty("t.tags"))
      .orderBy("t.id", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([{ id: 1, tag_count: "2" }]);
  });

  it("executes unary prewhere expression predicates", async () => {
    const rows = await db
      .selectFrom("event_logs as e")
      .select("e.user_id", "e.event_type")
      .prewhere((eb) => eb.raw<number>("e.event_type = 'signup'"))
      .orderBy("e.user_id", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      { user_id: 1, event_type: "signup" },
      { user_id: 3, event_type: "signup" },
    ]);
  });

  it("executes distinct selections", async () => {
    const rows = await db
      .selectFrom("event_logs as e")
      .distinct()
      .select("e.event_type")
      .orderBy("e.event_type", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      { event_type: "browse" },
      { event_type: "purchase" },
      { event_type: "signup" },
    ]);
  });

  it("executes distinct on selections", async () => {
    const rows = await db
      .selectFrom("event_logs as e")
      .distinctOn("e.user_id")
      .select("e.user_id", "e.event_type")
      .orderBy("e.user_id", "asc")
      .orderBy("e.event_type", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      { user_id: 1, event_type: "browse" },
      { user_id: 2, event_type: "purchase" },
      { user_id: 3, event_type: "signup" },
    ]);
  });

  it("executes unary having expression predicates", async () => {
    const rows = await db
      .selectFrom("event_logs as e")
      .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
      .groupBy("e.user_id")
      .having((eb) => eb.raw<number>("count() > 0"))
      .orderBy("e.user_id", "asc")
      .execute(getContext().client);

    expect(rows).toEqual([
      { user_id: 1, event_count: "2" },
      { user_id: 2, event_count: "1" },
      { user_id: 3, event_count: "1" },
    ]);
  });

  it("executes left anti joins and returns right-side defaults", async () => {
    const rows = await db
      .selectFrom("users as u")
      .leftAntiJoin("event_logs as e", "u.id", "e.user_id")
      .select("u.id", "u.email", "e.event_type")
      .where("u.status", "=", "active")
      .orderBy("u.id", "asc")
      .limit(2)
      .execute(getContext().client);

    expect(rows).toEqual([
      { id: 4, email: "user4@example.com", event_type: "" },
      { id: 5, email: "user5@example.com", event_type: "" },
    ]);
  });
});
