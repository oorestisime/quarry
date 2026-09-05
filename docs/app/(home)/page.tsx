import Link from "next/link";
import { ReleaseNotice } from "@/components/release-notice";
import { HighlightedCode } from "@/components/highlighted-code";

const recipes = [
  ["daily-activity", "Daily activity", "Group events into chart-ready daily counts.", "GROUP BY"],
  ["latest-event", "Latest event per user", "Find one recent event for each user.", "LIMIT BY"],
  ["optional-filters", "Dashboard filters", "Compose optional filters with typed inputs.", "WHERE"],
  [
    "pagination",
    "Paginated results",
    "Page through a consistently ordered report.",
    "LIMIT / OFFSET",
  ],
  [
    "streaming-export",
    "Streaming exports",
    "Process rows as they arrive from ClickHouse.",
    "stream()",
  ],
];

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col">
      <section className="mx-auto w-full max-w-6xl px-6 pt-10 pb-16 md:pt-16 md:pb-24">
        <p className="mb-5 text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
          TypeScript + ClickHouse · Pre-1.0
        </p>
        <div className="grid gap-10 lg:grid-cols-[1.1fr_1fr] lg:gap-16 items-start">
          <div>
            <h1 className="max-w-xl text-4xl sm:text-5xl lg:text-6xl font-semibold leading-[1.08] tracking-tight">
              Your analytics queries.
              <br />
              <span className="text-fd-muted-foreground">Now with types.</span>
            </h1>
            <p className="mt-6 max-w-lg text-lg leading-relaxed text-fd-muted-foreground">
              Keep your ClickHouse client. Compose filters, joins, and aggregations in TypeScript,
              see the SQL you send, and infer the rows you receive.
            </p>
            <div className="mt-8 flex flex-wrap gap-3">
              <Link
                href="/docs/guides/getting-started"
                className="rounded-lg bg-fd-foreground px-5 py-3 font-medium text-fd-background hover:opacity-85 transition"
              >
                Run the quickstart <span aria-hidden="true">→</span>
              </Link>
              <Link
                href="/playground"
                className="rounded-lg border border-fd-border px-5 py-3 font-medium hover:bg-fd-accent transition"
              >
                Try the playground
              </Link>
            </div>
            <p className="mt-4 text-sm text-fd-muted-foreground">
              A seeded database, generated types, and a working endpoint.
            </p>
          </div>
          <div className="min-w-0 overflow-hidden rounded-xl border border-fd-border bg-fd-background shadow-sm">
            <div className="border-b border-fd-border px-5 py-3 text-xs font-medium text-fd-muted-foreground">
              Write a query
            </div>
            <HighlightedCode
              language="typescript"
              className="px-5 py-5 text-[13px] leading-7"
              code={`const query = db.selectFrom("events")
  .selectExpr(eb => [
    "event_type",
    eb.fn.count().as("events"),
  ])
  .where("tenant_id", "=", tenantId)
  .groupBy("event_type");

const rows = await query.execute();`}
            />
            <div className="border-y border-fd-border bg-fd-muted/30 px-5 py-3 text-xs font-medium text-fd-muted-foreground">
              Know what comes back
            </div>
            <HighlightedCode
              language="typescript"
              className="px-5 py-4 text-[13px] leading-7"
              code={`// Inferred result
{ event_type: string; events: string }[]`}
            />
            <p className="px-5 pb-5 text-xs leading-relaxed text-fd-muted-foreground">
              Counts are strings because ClickHouse returns UInt64 that way in JSON.
            </p>
          </div>
        </div>
        <div className="mt-10">
          <ReleaseNotice />
        </div>
      </section>

      <section className="border-y border-fd-border bg-fd-muted/20">
        <div className="mx-auto grid max-w-6xl gap-8 px-6 py-10 md:grid-cols-3">
          <Benefit number="01" title="Use your existing connection">
            Quarry works with @clickhouse/client and plain TypeScript schema types.
          </Benefit>
          <Benefit number="02" title="Keep SQL visible">
            Inspect generated SQL and bound parameters before executing a query.
          </Benefit>
          <Benefit number="03" title="Build on ClickHouse">
            Use PREWHERE, FINAL, arrays, dictionaries, and LIMIT BY directly.
          </Benefit>
        </div>
      </section>

      <section className="mx-auto w-full max-w-6xl px-6 py-16 md:py-24">
        <div className="mb-8 flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              Learn by querying
            </p>
            <h2 className="text-3xl font-semibold tracking-tight">Start with a result you need.</h2>
          </div>
          <Link href="/docs/recipes" className="text-sm underline underline-offset-4">
            All recipes <span aria-hidden="true">→</span>
          </Link>
        </div>
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {recipes.map(([slug, title, description, clause]) => (
            <Link
              key={slug}
              href={`/docs/recipes/${slug}`}
              className="group rounded-xl border border-fd-border p-6 hover:bg-fd-accent/40 transition"
            >
              <span className="text-xs font-mono text-fd-muted-foreground">{clause}</span>
              <h3 className="mt-5 mb-2 text-lg font-semibold">
                {title}{" "}
                <span
                  aria-hidden="true"
                  className="inline-block transition-transform group-hover:translate-x-1"
                >
                  →
                </span>
              </h3>
              <p className="text-sm leading-relaxed text-fd-muted-foreground">{description}</p>
            </Link>
          ))}
          <div className="rounded-xl border border-dashed border-fd-border p-6 flex flex-col justify-center">
            <h3 className="font-semibold">Code. SQL. Expected rows.</h3>
            <p className="mt-2 text-sm leading-relaxed text-fd-muted-foreground">
              Every recipe uses the same seeded database. CI checks the displayed SQL and results
              against ClickHouse.
            </p>
          </div>
        </div>
      </section>

      <section className="mx-auto grid w-full max-w-6xl gap-8 px-6 pb-16 md:grid-cols-2 md:pb-24">
        <div className="rounded-xl border border-fd-border p-8">
          <h2 className="text-2xl font-semibold tracking-tight">Already using ClickHouse?</h2>
          <p className="mt-3 mb-5 text-fd-muted-foreground leading-relaxed">
            Keep your connection and replace one query at a time. Generate types from your database
            as your use grows.
          </p>
          <Link
            href="/docs/guides/existing-project"
            className="font-medium underline underline-offset-4"
          >
            Add Quarry to your project →
          </Link>
        </div>
        <div className="rounded-xl border border-fd-border p-8">
          <h2 className="text-2xl font-semibold tracking-tight">Deciding whether it fits?</h2>
          <p className="mt-3 mb-5 text-fd-muted-foreground leading-relaxed">
            Compare a real endpoint built with raw SQL, Quarry, Kysely, and hypequery. See the
            tradeoffs and supported clauses.
          </p>
          <Link
            href="/docs/concepts/choosing-quarry"
            className="font-medium underline underline-offset-4"
          >
            Compare approaches →
          </Link>
        </div>
      </section>
      <section className="border-t border-fd-border px-6 py-12 text-center">
        <p className="text-fd-muted-foreground">
          Have a query that is difficult to compose or type?
        </p>
        <Link
          href="https://github.com/oorestisime/quarry/issues/new?template=query.yml"
          className="mt-3 inline-block font-medium underline underline-offset-4"
        >
          Bring a real query →
        </Link>
      </section>
    </main>
  );
}

function Benefit({
  number,
  title,
  children,
}: {
  number: string;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <span className="text-xs font-mono text-fd-muted-foreground">{number}</span>
      <h2 className="mt-3 mb-2 font-semibold">{title}</h2>
      <p className="text-sm leading-relaxed text-fd-muted-foreground">{children}</p>
    </div>
  );
}
