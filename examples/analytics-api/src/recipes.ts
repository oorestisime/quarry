// #region setup
import { createClickHouseDB, type ClickHouseClient } from "quarry";
import type { DB } from "./generated-schema";

export function createAnalyticsDB(client: ClickHouseClient) {
  return createClickHouseDB<DB>({ client });
}

export type AnalyticsDB = ReturnType<typeof createAnalyticsDB>;
// #endregion setup

// #region dailyActivity
export function dailyActivity(db: AnalyticsDB, tenantId: number) {
  return db
    .selectFrom("events")
    .prewhere("tenant_id", "=", tenantId)
    .selectExpr((eb) => [eb.fn.toDate("created_at").as("day"), eb.fn.count().as("events")])
    .groupBy((eb) => eb.fn.toDate("created_at"))
    .orderBy("day");
}
// #endregion dailyActivity

// #region latestEvents
export function latestEvents(db: AnalyticsDB, tenantId: number) {
  return db
    .selectFrom("events")
    .prewhere("tenant_id", "=", tenantId)
    .select("user_id", "event_type", "created_at")
    .orderBy("user_id")
    .orderBy("created_at", "desc")
    .orderBy("event_type")
    .limitBy(1, "user_id");
}
// #endregion latestEvents

// #region filteredCounts
export function filteredCounts(
  db: AnalyticsDB,
  tenantId: number,
  filters: { from: string; eventType?: string },
) {
  let query = db
    .selectFrom("events")
    .prewhere("tenant_id", "=", tenantId)
    .where("created_at", ">=", filters.from);

  if (filters.eventType !== undefined) {
    query = query.where("event_type", "=", filters.eventType);
  }

  return query
    .selectExpr((eb) => ["event_type", eb.fn.count().as("events")])
    .groupBy("event_type")
    .orderBy("event_type");
}
// #endregion filteredCounts

// #region activityPage
export function activityPage(db: AnalyticsDB, tenantId: number, offset: number) {
  return dailyActivity(db, tenantId).limit(2).offset(offset);
}
// #endregion activityPage

// #region exportEvents
export function exportEvents(db: AnalyticsDB, tenantId: number) {
  return db
    .selectFrom("events")
    .prewhere("tenant_id", "=", tenantId)
    .select("user_id", "event_type", "created_at")
    .orderBy("created_at")
    .orderBy("user_id")
    .orderBy("event_type");
}
// #endregion exportEvents
