import type { ClickHouseClient } from "quarry";
import type { AnalyticsFilters } from "./analytics";

interface RawAnalyticsRow {
  event_type: string;
  events: string;
  users: string;
}

/** Equivalent handwritten SQL for comparing composition and result annotations. */
export async function rawAnalytics(
  client: ClickHouseClient,
  tenantId: number,
  filters: AnalyticsFilters,
): Promise<RawAnalyticsRow[]> {
  const predicates = ["created_at >= {from:String}", "created_at < {to:String}"];
  const params: Record<string, unknown> = { tenant: tenantId, from: filters.from, to: filters.to };
  if (filters.eventType) {
    predicates.push("event_type = {eventType:String}");
    params.eventType = filters.eventType;
  }
  const result = await client.query({
    query: `SELECT event_type, count() AS events, uniqExact(user_id) AS users
      FROM events PREWHERE tenant_id = {tenant:UInt32}
      WHERE ${predicates.join(" AND ")} GROUP BY event_type ORDER BY event_type`,
    query_params: params,
    format: "JSONEachRow",
    clickhouse_settings: { output_format_json_quote_64bit_integers: 1 },
  });
  return result.json<RawAnalyticsRow>();
}
