import { createClickHouseDB, type ClickHouseClient, type InferResult } from "quarry";
import type { DB } from "./generated-schema";

export interface AnalyticsFilters {
  from: string;
  to: string;
  eventType?: "signup" | "purchase";
}

export function analyticsQuery(
  client: ClickHouseClient,
  tenantId: number,
  filters: AnalyticsFilters,
) {
  const db = createClickHouseDB<DB>({ client });
  let query = db
    .selectFrom("events")
    .prewhere("tenant_id", "=", tenantId)
    .where("created_at", ">=", filters.from)
    .where("created_at", "<", filters.to);
  if (filters.eventType) query = query.where("event_type", "=", filters.eventType);
  return query
    .selectExpr((eb) => [
      "event_type",
      eb.fn.count().as("events"),
      eb.fn.uniqExact("user_id").as("users"),
    ])
    .groupBy("event_type")
    .orderBy("event_type");
}

export type AnalyticsRow = InferResult<ReturnType<typeof analyticsQuery>>;
