"use client";

import Link from "next/link";
import { useState } from "react";
import { createClickHouseDB } from "quarry";

interface DB {
  events: { tenant_id: number; user_id: string; event_type: string; created_at: string };
}
const db = createClickHouseDB<DB>();

export function Playground() {
  const [eventType, setEventType] = useState("");
  const [from, setFrom] = useState("2026-08-01");
  const [limit, setLimit] = useState(20);
  const [mode, setMode] = useState("counts");
  let base = db
    .selectFrom("events as e")
    .prewhere("e.tenant_id", "=", 1)
    .where("e.created_at", ">=", from);
  if (eventType) base = base.where("e.event_type", "=", eventType);
  const query =
    mode === "counts"
      ? base
          .selectExpr((eb) => ["e.event_type", eb.fn.count().as("events")])
          .groupBy("e.event_type")
          .orderBy("events", "desc")
          .limit(limit)
      : base
          .select("e.user_id", "e.event_type", "e.created_at")
          .orderBy("e.created_at", "desc")
          .limit(limit);
  const compiled = query.toSQL();
  const code = `const db = createClickHouseDB<DB>();

let query = db
  .selectFrom("events as e")
  .prewhere("e.tenant_id", "=", 1)
  .where("e.created_at", ">=", ${JSON.stringify(from)});
${eventType ? `\nquery = query.where("e.event_type", "=", ${JSON.stringify(eventType)});\n` : ""}
const result = query
${
  mode === "counts"
    ? `  .selectExpr(eb => ["e.event_type", eb.fn.count().as("events")])
  .groupBy("e.event_type")
  .orderBy("events", "desc")`
    : `  .select("e.user_id", "e.event_type", "e.created_at")
  .orderBy("e.created_at", "desc")`
}
  .limit(${limit});

result.toSQL();`;
  return (
    <main className="mx-auto w-full max-w-7xl px-6 py-12">
      <Link href="/" className="text-sm text-fd-muted-foreground hover:text-fd-foreground">
        ← Quarry
      </Link>
      <div className="mt-8 mb-10 max-w-3xl">
        <p className="text-xs uppercase tracking-widest text-fd-muted-foreground mb-3">
          Interactive example
        </p>
        <h1 className="text-4xl font-semibold tracking-tight mb-4">
          See the SQL before connecting.
        </h1>
        <p className="text-fd-muted-foreground text-lg">
          Change the filters and inspect the TypeScript, SQL, and parameters. These queries compile
          in your browser; no database connection is needed.
        </p>
      </div>
      <div className="grid gap-6 lg:grid-cols-[260px_minmax(0,1fr)]">
        <aside className="rounded-xl border border-fd-border p-5 h-fit space-y-6 bg-fd-card">
          <label className="block text-sm font-medium">
            Query
            <select
              value={mode}
              onChange={(event) => setMode(event.target.value)}
              className="mt-2 block w-full rounded-md border border-fd-border bg-fd-background p-2"
            >
              <option value="counts">Events by type</option>
              <option value="recent">Recent events</option>
            </select>
          </label>
          <label className="block text-sm font-medium">
            Event filter
            <select
              value={eventType}
              onChange={(event) => setEventType(event.target.value)}
              className="mt-2 block w-full rounded-md border border-fd-border bg-fd-background p-2"
            >
              <option value="">All events</option>
              <option value="signup">Signup</option>
              <option value="purchase">Purchase</option>
            </select>
          </label>
          <label className="block text-sm font-medium">
            From
            <input
              type="date"
              value={from}
              onChange={(event) => {
                if (event.target.value) setFrom(event.target.value);
              }}
              className="mt-2 block w-full min-w-0 rounded-md border border-fd-border bg-fd-background p-2"
            />
          </label>
          <label className="block text-sm font-medium">
            Row limit: {limit}
            <input
              type="range"
              min="1"
              max="100"
              value={limit}
              onChange={(event) => setLimit(Number(event.target.value))}
              className="mt-3 block w-full"
            />
          </label>
          <p className="text-xs leading-relaxed text-fd-muted-foreground">
            The example always scopes reads to tenant 1. In your application, use the authenticated
            caller’s tenant.
          </p>
          <Link
            href="https://github.com/oorestisime/quarry/tree/main/examples/analytics-api"
            className="block rounded-md bg-fd-primary px-4 py-3 text-center text-sm font-medium text-black"
          >
            Run the complete API →
          </Link>
        </aside>
        <div className="min-w-0 space-y-5">
          <CodePanel label="TypeScript" code={code} />
          <div aria-live="polite" className="space-y-5">
            <CodePanel
              label="Generated SQL"
              code={compiled.query.replace(
                / (FROM|PREWHERE|WHERE|GROUP BY|ORDER BY|LIMIT) /g,
                "\n$1 ",
              )}
            />
            <div className="grid gap-5 md:grid-cols-2">
              <CodePanel label="Bound parameters" code={JSON.stringify(compiled.params, null, 2)} />
              <CodePanel
                label="Result shape"
                code={
                  mode === "counts"
                    ? "{\n  event_type: string;\n  events: string; // UInt64\n}[]"
                    : "{\n  user_id: string; // UInt64\n  event_type: string;\n  created_at: string;\n}[]"
                }
              />
            </div>
          </div>
          <p className="text-sm text-fd-muted-foreground">
            Values remain separate from SQL.{" "}
            <Link href="/docs/guides/getting-started" className="underline underline-offset-4">
              Connect your client
            </Link>{" "}
            to execute the query, or{" "}
            <Link href="/docs/guides/introspection" className="underline underline-offset-4">
              generate types from your schema
            </Link>
            .
          </p>
        </div>
      </div>
    </main>
  );
}

function CodePanel({ label, code }: { label: string; code: string }) {
  return (
    <section className="min-w-0 overflow-hidden rounded-xl border border-fd-border bg-fd-card">
      <h2 className="border-b border-fd-border px-4 py-2 text-xs font-semibold uppercase tracking-wider text-fd-muted-foreground">
        {label}
      </h2>
      <pre className="overflow-x-auto p-4 text-sm leading-relaxed">
        <code>{code}</code>
      </pre>
    </section>
  );
}
