<p align="center">
  <img src="https://raw.githubusercontent.com/oorestisime/quarry/main/.github/assets/logo.png" width="120" alt="Quarry logo" />
</p>

<h1 align="center">Quarry</h1>

<p align="center">
  <a href="https://www.npmjs.com/package/quarry"><img src="https://img.shields.io/npm/v/quarry" alt="npm version" /></a>
  <a href="https://github.com/oorestisime/quarry/actions/workflows/ci.yml"><img src="https://github.com/oorestisime/quarry/actions/workflows/ci.yml/badge.svg" alt="CI status" /></a>
  <a href="https://ch-quarry.vercel.app"><img src="https://img.shields.io/badge/docs-online-0f172a" alt="docs" /></a>
</p>

<p align="center">
  Type-safe ClickHouse queries for TypeScript services that have outgrown raw SQL.
</p>

<p align="center">
  Keep <code>@clickhouse/client</code>. Add composable queries, schema-aware results,
  and ClickHouse-native syntax without adopting an ORM or semantic layer.
</p>

<p align="center">
  <a href="https://ch-quarry.vercel.app/docs/guides/getting-started"><strong>Get started</strong></a>
  ·
  <a href="https://ch-quarry.vercel.app/docs/guides/introspection"><strong>Generate DB types</strong></a>
  ·
  <a href="https://ch-quarry.vercel.app/docs/reference"><strong>API reference</strong></a>
  ·
  <a href="https://github.com/oorestisime/quarry/issues/new?template=query.yml"><strong>Bring a real query</strong></a>
</p>

Quarry is for TypeScript backends, jobs, and internal tools that query
ClickHouse directly. It becomes useful when SQL strings start spreading across
route handlers, filters need to be composed conditionally, result interfaces
drift from what ClickHouse actually returns, or a generic SQL builder stops at
ClickHouse-specific syntax.

It stays deliberately close to SQL: queries are immutable, parameters remain
visible, generated SQL is inspectable, and execution still goes through the
official ClickHouse client.

## Try it in five minutes

- [Open the playground](https://ch-quarry.vercel.app/playground) to change filters
  and inspect SQL and parameters without a database.
- [Run the analytics API](https://github.com/oorestisime/quarry/tree/main/examples/analytics-api) for seeded ClickHouse data,
  schema generation, optional filters, cancellation, and inferred result rows.
- [Compare approaches](https://ch-quarry.vercel.app/docs/concepts/choosing-quarry)
  using a real analytics endpoint and explicit tradeoffs.

## When Quarry helps

- **Application-facing analytics APIs** with filters assembled from typed
  request parameters.
- **Product and event analytics** that use `PREWHERE`, `FINAL`, arrays,
  dictionaries, percentiles, and ClickHouse aggregate combinators.
- **Scheduled aggregation and data jobs** that should share query logic with
  the application instead of duplicating SQL strings.
- **ClickHouse-to-ClickHouse transformations** using typed
  `INSERT INTO ... SELECT` without moving rows through the application.
- **Large TypeScript codebases** where schema changes should surface as type
  errors rather than production query failures.

Quarry is not a dashboard framework, semantic layer, migration system, or
multi-database ORM. It focuses on one boundary: composing and executing
ClickHouse queries safely from TypeScript.

## Install

```bash
npm install quarry @clickhouse/client
```

Quarry expects you to bring your own `@clickhouse/client` instance. Define the
database shape as plain TypeScript, or generate a first version from a live
ClickHouse database with the
[introspection CLI](https://ch-quarry.vercel.app/docs/guides/introspection).

## Quick look

```ts
import { createClient } from "@clickhouse/client";
import { createClickHouseDB } from "quarry";

interface DB {
  event_logs: {
    user_id: number;
    event_type: string;
    event_date: string;
    created_at: string;
  };
}

const db = createClickHouseDB<DB>({
  client: createClient({ url: "http://localhost:8123" }),
});

const query = db
  .selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.event_type", eb.fn.count().as("events")])
  .prewhere("e.event_date", ">=", "2026-08-01")
  .where("e.event_type", "in", ["signup", "purchase"])
  .groupBy("e.event_type")
  .orderBy("events", "desc")
  .limit(20);

const compiled = query.toSQL();
const rows = await query.execute();
// Array<{ event_type: string; events: string }>
```

`events` is a `string` because ClickHouse returns `UInt64` that way through
`@clickhouse/client`. Quarry models the value your program receives rather than
giving it a more convenient but incorrect TypeScript type.

## Why Quarry

- **ClickHouse-native query composition.** `FINAL`, `PREWHERE`, `SETTINGS`,
  `ARRAY JOIN`, `LIMIT BY`, `GROUP BY WITH TOTALS`, dictionaries,
  `INSERT INTO ... SELECT`, and typed ClickHouse functions are part of the
  builder rather than raw-SQL afterthoughts.
- **Types that follow the query.** Selections, aliases, joins, subqueries,
  array joins, aggregate expressions, and insert columns update the accepted
  inputs and inferred result shape.
- **Runtime-honest database types.** Read, insert, and predicate values can
  differ where ClickHouse and `@clickhouse/client` require it. Wide integers,
  dates, nullable values, dictionaries, and decimals are modeled explicitly.
- **Plain TypeScript schema definitions.** Use interfaces you already own or
  generate them from ClickHouse; Quarry does not require a second schema DSL.
- **Inspectable output.** Every select and insert can be compiled before it is
  executed, including typed ClickHouse placeholders and parameter values.

## Compatibility

Quarry is pre-1.0. Minor releases may change the API with a changelog entry;
patch releases preserve it. The package is ESM and supports Node 22/24 and
TypeScript 5.9, 6, and 7 under NodeNext or Bundler resolution. CI checks consumers
of the packed package and runs integration tests against ClickHouse 24.8 and 25.8
using `@clickhouse/client` 1.18.2.

Queries explicitly preserve JSON serialization settings so UInt64 and other
result types agree with the values received. Use `leftJoinNullable()` for nullable
right-side join results; ordinary `leftJoin()` uses ClickHouse's type defaults.
See [runtime semantics](https://ch-quarry.vercel.app/docs/concepts/runtime-semantics)
for the supported settings and limitations.

## Current scope

- Typed selects and inserts, including `INSERT INTO ... SELECT`
- Joins, nullable left joins, subqueries, CTEs, `UNION ALL`, and multi-condition predicates
- Parameter-aware `sql` fragments, quoted identifiers, and window expressions
- `FINAL`, `PREWHERE`, `ARRAY JOIN`, `GROUP BY WITH TOTALS`, and `LIMIT BY`
- Typed dictionary, array, string, date/time, null, cast, conditional, and
  aggregate helpers
- Buffered execution, first-row helpers, totals-aware execution, retries, and
  streaming
- Schema introspection for tables, views, and dictionaries, including generated insert columns

The complete support matrix, limitations, and ClickHouse-specific behavior are
documented in
[ClickHouse quirks](https://ch-quarry.vercel.app/docs/concepts/clickhouse-quirks).

## Try it on a real query

Quarry's roadmap is meant to grow from production queries rather than a generic
SQL feature checklist. If you use ClickHouse from TypeScript, bring a query that
is difficult to compose, type, or reuse.

[Open an issue](https://github.com/oorestisime/quarry/issues/new?template=query.yml) with the SQL
or a reduced example and describe what changes at runtime. Complex joins,
conditional filters, unusual ClickHouse functions, and queries that expose a
bad type are especially useful. I will help translate it into Quarry and use
anything the builder cannot represent to guide the next improvements.

## Project status

Quarry is pre-1.0. It is ready to evaluate and use in early projects, but minor
releases may still refine the API. Supported behavior is covered by compile,
type, and integration tests against a real ClickHouse instance. Release changes
are documented in the
[changelog](https://github.com/oorestisime/quarry/blob/main/CHANGELOG.md).

## Documentation

- [Getting started](https://ch-quarry.vercel.app/docs/guides/getting-started)
- [Introspection](https://ch-quarry.vercel.app/docs/guides/introspection)
- [Task-oriented guides](https://ch-quarry.vercel.app/docs/guides)
- [API reference](https://ch-quarry.vercel.app/docs/reference)
- [Design and runtime semantics](https://ch-quarry.vercel.app/docs/concepts)
- [Roadmap](https://ch-quarry.vercel.app/docs/roadmap)

The docs source lives in
[`docs/`](https://github.com/oorestisime/quarry/tree/main/docs). To run it locally:

```bash
pnpm install
pnpm --dir docs install
pnpm --dir docs dev
```

## Naming

The npm package name is `quarry`. It moved from `@oorestisime/quarry` in
version `0.9.0`; existing scoped-package users can update to the `0.8.1`
compatibility wrapper before changing their dependency and imports.

## Contributing

See [CONTRIBUTING.md](https://github.com/oorestisime/quarry/blob/main/CONTRIBUTING.md)
for local checks, query regression tests, compiler performance budgets, and release preparation.
