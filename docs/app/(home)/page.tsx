import Link from "next/link";

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col">
      {/* Hero */}
      <section className="flex flex-col items-center text-center px-6 pt-24 pb-20">
        <span className="inline-block mb-6 px-3 py-1 text-xs font-semibold uppercase tracking-wider rounded-full bg-fd-muted text-fd-muted-foreground border border-fd-border">
          Alpha
        </span>
        <h1 className="text-5xl md:text-6xl font-bold mb-6 tracking-tight">
          Quarry
        </h1>
        <p className="text-fd-muted-foreground max-w-2xl mb-10 text-lg leading-relaxed">
          A ClickHouse-native query builder for TypeScript. Type-safe, explicit
          about ClickHouse semantics, and honest about what your driver
          actually returns at runtime.
        </p>
        <div className="flex gap-3">
          <Link
            href="/docs/guides/getting-started"
            className="bg-fd-primary text-fd-primary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition"
          >
            Getting started
          </Link>
          <Link
            href="/docs/reference"
            className="bg-fd-secondary text-fd-secondary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition border border-fd-border"
          >
            API reference
          </Link>
        </div>
      </section>

      {/* Path grid */}
      <section className="px-6 pb-24">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-3xl md:text-4xl font-bold text-center mb-3 tracking-tight">
            Choose your path
          </h2>
          <p className="text-fd-muted-foreground text-center max-w-2xl mx-auto mb-12">
            The docs are split by the job you are trying to do, not just by
            the code structure underneath.
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
          <h2 className="text-2xl font-bold mb-3">Need the deeper model?</h2>
          <p className="text-fd-muted-foreground mb-6">
            Quarry is explicit about ClickHouse behavior. If you want to see
            where the design is headed or why the semantics look the way they
            do, the deep-dive docs are the next stop.
          </p>
          <div className="flex gap-3 justify-center">
            <Link
              href="/docs/concepts"
              className="bg-fd-primary text-fd-primary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition"
            >
              Deep dive
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
      <p className="text-fd-muted-foreground leading-relaxed text-sm">
        {description}
      </p>
    </Link>
  );
}
