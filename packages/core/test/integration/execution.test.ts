import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClickHouseDB, param } from "../../src";
import { startClickHouse, stopClickHouse, type ClickHouseTestContext } from "./clickhouse";
import {
  aggregateFunctionsCase,
  chainedExpressionWhereCase,
  cteLeftJoinBaseTableCase,
  cteJoinCase,
  dictGetCase,
  dictGetOrDefaultCase,
  dictHasCase,
  finalPrewhereSettingsCase,
  groupByAggregateCase,
  groupByHavingCase,
  havingSubqueryCase,
  heavyHitterFunctionsCase,
  inSubqueryCase,
  innerJoinCase,
  joinFinalTableSourceCase,
  joinSubquerySettingsCase,
  joinSubqueryAliasCase,
  jsonExtractCase,
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
import type { TypedDictionary } from "../../src";

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
  partner_rates: TypedDictionary<{
    rate_cents: number;
    currency: string;
  }>;
  partner_country_rates: TypedDictionary<{
    rate_cents: number;
  }>;
  partner_rate_ranges: TypedDictionary<{
    rate_cents: number;
  }>;
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
    const rows = await simpleSelectCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(simpleSelectCase.expectedRows);
  });

  it(finalPrewhereSettingsCase.name, async () => {
    const rows = await finalPrewhereSettingsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(finalPrewhereSettingsCase.expectedRows);
  });

  it(selectAllCase.name, async () => {
    const rows = await selectAllCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(selectAllCase.expectedRows);
  });

  it(innerJoinCase.name, async () => {
    const rows = await innerJoinCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(innerJoinCase.expectedRows);
  });

  it(joinSubqueryAliasCase.name, async () => {
    const rows = await joinSubqueryAliasCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(joinSubqueryAliasCase.expectedRows);
  });

  it(joinFinalTableSourceCase.name, async () => {
    const rows = await joinFinalTableSourceCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(joinFinalTableSourceCase.expectedRows);
  });

  it(whereRefCase.name, async () => {
    const rows = await whereRefCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(whereRefCase.expectedRows);
  });

  it(chainedExpressionWhereCase.name, async () => {
    const rows = await chainedExpressionWhereCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(chainedExpressionWhereCase.expectedRows);
  });

  it(groupByAggregateCase.name, async () => {
    const rows = await groupByAggregateCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(groupByAggregateCase.expectedRows);
  });

  it(groupByHavingCase.name, async () => {
    const rows = await groupByHavingCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(groupByHavingCase.expectedRows);
  });

  it(joinSubquerySettingsCase.name, async () => {
    const rows = await joinSubquerySettingsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(joinSubquerySettingsCase.expectedRows);
  });

  it(inSubqueryCase.name, async () => {
    const rows = await inSubqueryCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(inSubqueryCase.expectedRows);
  });

  it(multiConditionJoinCase.name, async () => {
    const rows = await multiConditionJoinCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(multiConditionJoinCase.expectedRows);
  });

  it(havingSubqueryCase.name, async () => {
    const rows = await havingSubqueryCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(havingSubqueryCase.expectedRows);
  });

  it(cteJoinCase.name, async () => {
    const rows = await cteJoinCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(cteJoinCase.expectedRows);
  });

  it(cteLeftJoinBaseTableCase.name, async () => {
    const rows = await cteLeftJoinBaseTableCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(cteLeftJoinBaseTableCase.expectedRows);
  });

  it(multipleCtesCase.name, async () => {
    const rows = await multipleCtesCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(multipleCtesCase.expectedRows);
  });

  it(selectAllForAliasCase.name, async () => {
    const rows = await selectAllForAliasCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(selectAllForAliasCase.expectedRows);
  });

  it(jsonExtractCase.name, async () => {
    const rows = await jsonExtractCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(jsonExtractCase.expectedRows);
  });

  it(typeCastFunctionsCase.name, async () => {
    const rows = await typeCastFunctionsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(typeCastFunctionsCase.expectedRows);
  });

  it(arrayFunctionsCase.name, async () => {
    const rows = await arrayFunctionsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(arrayFunctionsCase.expectedRows);
  });

  it(stringFunctionsCase.name, async () => {
    const rows = await stringFunctionsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(stringFunctionsCase.expectedRows);
  });

  it(aggregateFunctionsCase.name, async () => {
    const rows = await aggregateFunctionsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(aggregateFunctionsCase.expectedRows);
  });

  it(nullFunctionsCase.name, async () => {
    const rows = await nullFunctionsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(nullFunctionsCase.expectedRows);
  });

  it(dateTimeFunctionsCase.name, async () => {
    const rows = await dateTimeFunctionsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(dateTimeFunctionsCase.expectedRows);
  });

  it(heavyHitterFunctionsCase.name, async () => {
    const rows = await heavyHitterFunctionsCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(heavyHitterFunctionsCase.expectedRows);
  });

  it("executes unary where expression predicates", async () => {
    const rows = await db
      .selectFrom("typed_samples as t")
      .selectExpr((eb) => ["t.id", eb.fn.length("t.tags").as("tag_count")])
      .where((eb) => eb.fn.notEmpty("t.tags"))
      .orderBy("t.id", "asc")
      .execute({ client: getContext().client });

    expect(rows).toEqual([{ id: 1, tag_count: "2" }]);
  });

  it("executes unary prewhere expression predicates", async () => {
    const rows = await db
      .selectFrom("event_logs as e")
      .select("e.user_id", "e.event_type")
      .prewhere((eb) => eb.raw<number>("e.event_type = 'signup'"))
      .orderBy("e.user_id", "asc")
      .execute({ client: getContext().client });

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
      .execute({ client: getContext().client });

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
      .execute({ client: getContext().client });

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
      .execute({ client: getContext().client });

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
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      { id: 4, email: "user4@example.com", event_type: "" },
      { id: 5, email: "user5@example.com", event_type: "" },
    ]);
  });

  it("executes CTEs built conditionally and passed as pre-built SelectQueryBuilder", async () => {
    function buildActiveUsersCte(includeSignup: boolean) {
      let query = db.selectFrom("event_logs as e").select("e.user_id").groupBy("e.user_id");
      if (includeSignup) {
        query = query.where("e.event_type", "=", "signup");
      }
      return query;
    }

    const rows = await db
      .with("active_users", buildActiveUsersCte(true))
      .selectFrom("active_users as au")
      .innerJoin("users as u", "u.id", "au.user_id")
      .select("u.id", "u.email")
      .orderBy("u.id", "asc")
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      { id: 1, email: "alice@example.com" },
      { id: 3, email: "cory@example.com" },
    ]);
  });

  it(dictGetCase.name, async () => {
    const rows = await dictGetCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(dictGetCase.expectedRows);
  });

  it(dictGetOrDefaultCase.name, async () => {
    const rows = await dictGetOrDefaultCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(dictGetOrDefaultCase.expectedRows);
  });

  it(dictHasCase.name, async () => {
    const rows = await dictHasCase.build().execute({ client: getContext().client });
    expect(rows).toEqual(dictHasCase.expectedRows);
  });

  it("executes dictGet with composite dictionary keys", async () => {
    const rows = await db
      .selectFrom("users as u")
      .selectExpr((eb) => [
        "u.id",
        eb.fn
          .dictGet("partner_country_rates", "rate_cents", ["u.id", eb.val("US")])
          .as("us_rate_cents"),
      ])
      .where("u.id", "in", [1, 2])
      .orderBy("u.id", "asc")
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      { id: 1, us_rate_cents: 110 },
      { id: 2, us_rate_cents: 210 },
    ]);
  });

  it("executes dictGet with RANGE_HASHED lookup dates", async () => {
    const rows = await db
      .selectFrom("users as u")
      .selectExpr((eb) => [
        "u.id",
        eb.fn
          .dictGet("partner_rate_ranges", "rate_cents", "u.id", eb.val(param("2025-02-10", "Date")))
          .as("rate_cents"),
      ])
      .where("u.id", "in", [1, 2])
      .orderBy("u.id", "asc")
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      { id: 1, rate_cents: 150 },
      { id: 2, rate_cents: 200 },
    ]);
  });

  it("executes dictGetOrDefault with explicit default params", async () => {
    const rows = await db
      .selectFrom("users as u")
      .selectExpr((eb) => [
        "u.id",
        eb.fn
          .dictGetOrDefault("partner_rates", "currency", "u.id", param("GBP", "String"))
          .as("currency"),
      ])
      .where("u.id", "in", [2, 3])
      .orderBy("u.id", "asc")
      .execute({ client: getContext().client });

    expect(rows).toEqual([
      { id: 2, currency: "EUR" },
      { id: 3, currency: "GBP" },
    ]);
  });
});
