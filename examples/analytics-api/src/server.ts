import { createClient } from "@clickhouse/client";
import { createServer } from "node:http";
import { pathToFileURL } from "node:url";
import { analyticsQuery, type AnalyticsFilters } from "./analytics";

function parseDate(value: string | null, fallback: string): string {
  const date = value ?? fallback;
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(date) ||
    !Number.isFinite(Date.parse(date)) ||
    new Date(date).toISOString().slice(0, 10) !== date
  )
    throw new Error("Dates must be valid YYYY-MM-DD values.");
  return `${date} 00:00:00`;
}

export function createAnalyticsServer() {
  const client = createClient({
    url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
    username: "quarry",
    password: "quarry",
    database: "analytics",
  });
  // Replace this demo identity with the authenticated caller's tenant in an application.
  const tenantId = Number(process.env.DEMO_TENANT_ID ?? 1);
  if (!Number.isSafeInteger(tenantId) || tenantId < 1)
    throw new Error("DEMO_TENANT_ID must be a positive integer.");
  const server = createServer((request, response) => {
    const abort = new AbortController();
    response.once("close", () => {
      if (!response.writableEnded) abort.abort();
    });
    async function handle() {
      const url = new URL(request.url ?? "/", "http://localhost");
      response.setHeader("content-type", "application/json");
      if (request.method !== "GET" || !["/analytics", "/sql"].includes(url.pathname)) {
        response.writeHead(404).end(JSON.stringify({ error: "Use GET /analytics or GET /sql" }));
        return;
      }
      let filters: AnalyticsFilters;
      try {
        const eventType = url.searchParams.get("eventType");
        if (eventType !== null && eventType !== "signup" && eventType !== "purchase")
          throw new Error("eventType must be signup or purchase.");
        filters = {
          from: parseDate(url.searchParams.get("from"), "2026-08-01"),
          to: parseDate(url.searchParams.get("to"), "2026-09-01"),
          ...(eventType ? { eventType } : {}),
        };
        if (filters.from >= filters.to) throw new Error("from must precede to.");
      } catch (error) {
        response
          .writeHead(400)
          .end(
            JSON.stringify({ error: error instanceof Error ? error.message : "Invalid filters" }),
          );
        return;
      }
      const query = analyticsQuery(client, tenantId, filters);
      const result =
        url.pathname === "/sql"
          ? query.toSQL()
          : await query.execute({ abortSignal: abort.signal });
      response.end(JSON.stringify(result));
    }
    void handle().catch((error) => {
      if (abort.signal.aborted) return;
      console.error(error);
      response.writeHead(500).end(JSON.stringify({ error: "Query failed" }));
    });
  });
  return { server, client };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { server, client } = createAnalyticsServer();
  server.listen(Number(process.env.PORT ?? 3001), "127.0.0.1", () =>
    console.log("Analytics API: http://localhost:3001/analytics"),
  );
  for (const signal of ["SIGINT", "SIGTERM"] as const)
    process.once(signal, () => {
      server.close(() => {
        void client.close();
      });
    });
}
