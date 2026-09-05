import Link from "next/link";

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col">
      {/* Hero */}
      <section className="flex flex-col items-center text-center px-6 pt-24 pb-20">
        <span className="inline-block mb-6 px-3 py-1 text-xs font-semibold uppercase tracking-wider rounded-full bg-fd-muted text-fd-muted-foreground border border-fd-border">
          Pre-1.0 · ESM · TypeScript 5.9–7
        </span>
        <h1 className="text-5xl md:text-6xl font-bold mb-6 tracking-tight">
          Type-safe ClickHouse queries.
        </h1>
        <p className="text-fd-muted-foreground max-w-2xl mb-10 text-lg leading-relaxed">
          Keep your existing client. Compose filters, joins, and aggregations in TypeScript. Infer
          the rows you receive and inspect the SQL you send.
        </p>
        <div className="flex gap-3">
          <Link
            href="/playground"
            className="bg-fd-primary text-fd-primary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition"
          >
            Try the playground
          </Link>
          <Link
            href="https://github.com/oorestisime/quarry/tree/main/examples/analytics-api"
            className="bg-fd-secondary text-fd-secondary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition border border-fd-border"
          >
            Run the example
          </Link>
        </div>
        <div className="mt-10 w-full max-w-2xl text-left rounded-xl border border-fd-border bg-fd-card overflow-hidden">
          <div className="px-4 py-2 border-b border-fd-border text-xs text-fd-muted-foreground">
            Install and query
          </div>
          <pre className="p-4 overflow-x-auto text-sm leading-relaxed">
            <code>{`pnpm add quarry @clickhouse/client

const rows = await db
  .selectFrom("events as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("events")])
  .groupBy("e.user_id")
  .execute();`}</code>
          </pre>
        </div>
      </section>

      <section className="px-6 pb-20">
        <div className="max-w-4xl mx-auto grid grid-cols-1 md:grid-cols-3 gap-6 text-center">
          <ValueCard
            title="Compose changing filters"
            description="Reuse a query across routes and jobs, adding optional filters while keeping typed inputs and results."
          />
          <ValueCard
            title="Runtime-honest types"
            description="UInt64, Decimal, nullable values, and aggregates are typed as the driver returns them."
          />
          <ValueCard
            title="Just a query builder"
            description="No ORM, migrations, entities, or generic SQL dialect hiding ClickHouse semantics."
          />
        </div>
      </section>

      {/* Path grid */}
      <section className="px-6 pb-24">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-3xl md:text-4xl font-bold text-center mb-3 tracking-tight">
            Choose your path
          </h2>
          <p className="text-fd-muted-foreground text-center max-w-2xl mx-auto mb-12">
            The docs are split by the job you are trying to do, not just by the code structure
            underneath.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <PathCard
              href="/docs/guides/getting-started"
              title="Build your first query"
              description="Start in plain TypeScript mode, connect @clickhouse/client, and run a typed SELECT in a few minutes."
            />
            <PathCard
              href="/docs/guides/introspection"
              title="Bootstrap DB types"
              description="Generate plain TypeScript Tables, Views, and DB types from an existing ClickHouse database."
            />
            <PathCard
              href="/docs/reference"
              title="Look up the API"
              description="Jump straight to SelectQueryBuilder, InsertQueryBuilder, ExpressionBuilder, and live type tables."
            />
            <PathCard
              href="/docs/concepts"
              title="Read the deep dive"
              description="Understand scope rules, runtime semantics, ClickHouse quirks, and the architecture behind the builder."
            />
          </div>
        </div>
      </section>

      {/* Bottom CTA */}
      <section className="px-6 pb-24">
        <div className="max-w-2xl mx-auto text-center">
          <h2 className="text-2xl font-bold mb-3">Bring a query you already use.</h2>
          <p className="text-fd-muted-foreground mb-6">
            Share the SQL, a reduced schema, and what changes at runtime. Real application queries
            guide Quarry’s next improvements.
          </p>
          <div className="flex gap-3 justify-center">
            <Link
              href="https://github.com/oorestisime/quarry/issues/new?template=query.yml"
              className="bg-fd-primary text-fd-primary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition"
            >
              Bring a query
            </Link>
            <Link
              href="/docs/guides/introspection"
              className="bg-fd-secondary text-fd-secondary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition border border-fd-border"
            >
              Introspection
            </Link>
          </div>
        </div>
      </section>
    </main>
  );
}

function ValueCard({ title, description }: { title: string; description: string }) {
  return (
    <div className="rounded-xl border border-fd-border p-5">
      <h2 className="font-semibold mb-2">{title}</h2>
      <p className="text-sm leading-relaxed text-fd-muted-foreground">{description}</p>
    </div>
  );
}

function PathCard({
  href,
  title,
  description,
}: {
  href: string;
  title: string;
  description: string;
}) {
  return (
    <Link
      href={href}
      className="block bg-fd-card border border-fd-border rounded-xl p-6 hover:border-fd-primary/40 transition"
    >
      <h3 className="font-semibold text-lg mb-2">{title}</h3>
      <p className="text-fd-muted-foreground leading-relaxed text-sm">{description}</p>
    </Link>
  );
}
