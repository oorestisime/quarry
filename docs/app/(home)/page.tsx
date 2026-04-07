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
            href="/docs"
            className="bg-fd-primary text-fd-primary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition"
          >
            Read the docs
          </Link>
          <a
            href="https://github.com/oorestisime/quarry"
            className="bg-fd-secondary text-fd-secondary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition border border-fd-border"
          >
            GitHub
          </a>
        </div>
      </section>

      {/* Feature grid */}
      <section className="px-6 pb-24">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-3xl md:text-4xl font-bold text-center mb-3 tracking-tight">
            Built for ClickHouse, not adapted to it
          </h2>
          <p className="text-fd-muted-foreground text-center max-w-2xl mx-auto mb-12">
            Quarry models ClickHouse concepts directly instead of pretending
            every database behaves like Postgres.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Feature
              title="ClickHouse-first API"
              description="First-class FINAL, PREWHERE, SETTINGS, and JOIN semantics. No leaky abstractions over a pretend Postgres."
            />
            <Feature
              title="Runtime-honest types"
              description="execute() returns the types your driver actually produces. UInt64 is a string, Decimal is a number, no surprises."
            />
            <Feature
              title="Type-safe joins and CTEs"
              description="Column references, aliases, and selected output are all checked against your schema. Subqueries and CTEs contribute their columns to the surrounding scope."
            />
            <Feature
              title="No ORM, no magic"
              description="Just a query builder. The compiled SQL is always inspectable via toSQL(). Around 1700 lines of source you can read end-to-end."
            />
          </div>
        </div>
      </section>

      {/* Bottom CTA */}
      <section className="px-6 pb-24">
        <div className="max-w-2xl mx-auto text-center">
          <h2 className="text-2xl font-bold mb-3">Ready to dig in?</h2>
          <p className="text-fd-muted-foreground mb-6">
            Get started in under five minutes, or read the architecture page
            to see how it all fits together.
          </p>
          <div className="flex gap-3 justify-center">
            <Link
              href="/docs/guides/getting-started"
              className="bg-fd-primary text-fd-primary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition"
            >
              Getting started
            </Link>
            <Link
              href="/docs/concepts/architecture"
              className="bg-fd-secondary text-fd-secondary-foreground rounded-full font-medium px-6 py-2.5 hover:opacity-90 transition border border-fd-border"
            >
              Architecture
            </Link>
          </div>
        </div>
      </section>
    </main>
  );
}

function Feature({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="bg-fd-card border border-fd-border rounded-xl p-6 hover:border-fd-primary/40 transition">
      <h3 className="font-semibold text-lg mb-2">{title}</h3>
      <p className="text-fd-muted-foreground leading-relaxed text-sm">
        {description}
      </p>
    </div>
  );
}
