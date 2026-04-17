<p align="center">
  <img src="./.github/assets/logo.png" width="120" alt="Quarry logo" />
</p>

<h1 align="center">Quarry</h1>

<p align="center">
  <a href="https://www.npmjs.com/package/@oorestisime/quarry"><img src="https://img.shields.io/npm/v/%40oorestisime%2Fquarry" alt="npm version" /></a>
  <a href="https://github.com/oorestisime/quarry/actions/workflows/ci.yml"><img src="https://github.com/oorestisime/quarry/actions/workflows/ci.yml/badge.svg" alt="CI status" /></a>
  <a href="https://ch-quarry.vercel.app"><img src="https://img.shields.io/badge/docs-online-0f172a" alt="docs" /></a>
</p>

<p align="center">
  ClickHouse-native query builder for TypeScript.
</p>

<p align="center">
  Type-safe, explicit about ClickHouse semantics, and honest about what your driver returns at runtime.
</p>

<p align="center">
  <a href="https://ch-quarry.vercel.app/docs/guides/getting-started"><strong>Getting started</strong></a>
  ·
  <a href="https://ch-quarry.vercel.app/docs/guides/introspection"><strong>Introspection</strong></a>
  ·
  <a href="https://ch-quarry.vercel.app/docs/reference"><strong>API reference</strong></a>
  ·
  <a href="https://ch-quarry.vercel.app/docs/concepts"><strong>Deep dive</strong></a>
</p>

Quarry is a query builder first. It is not trying to be an ORM or hide ClickHouse behind a generic multi-dialect SQL abstraction.

## Why Quarry

- ClickHouse-first surface area: `FINAL`, `PREWHERE`, `SETTINGS`, typed joins,
  `INSERT INTO ... SELECT`, and explicit table sources.
- Runtime-honest types: `UInt64` comes back as `string`, `Decimal` comes back
  as `number`, and the docs are explicit about those semantics.
- Plain TypeScript DB types: Quarry stays focused on query building instead of
  asking you to maintain a second schema DSL.
- Inspectable output: every query can be compiled with `toSQL()` before you
  execute it.

## Install

```bash
pnpm add @oorestisime/quarry @clickhouse/client
```

Quarry expects you to bring your own `@clickhouse/client` instance.

## Quick look

```ts
import { createClient } from "@clickhouse/client";
import { createClickHouseDB } from "@oorestisime/quarry";

interface DB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
  };
}

const db = createClickHouseDB<DB>({
  client: createClient({ url: "http://localhost:8123" }),
});

const query = db
  .selectFrom("event_logs as e")
  .select("e.user_id", "e.event_type")
  .where("e.event_type", "=", "signup")
  .orderBy("e.created_at", "desc")
  .limit(50);

const compiled = query.toSQL();
const rows = await query.execute();
```

## Choose a path

- [Getting started](https://ch-quarry.vercel.app/docs/guides/getting-started)
  &mdash; install Quarry, create a typed `db`, run a first query, inspect SQL.
- [Introspection](https://ch-quarry.vercel.app/docs/guides/introspection)
  &mdash; bootstrap plain `Tables`, `Views`, and `DB` types from ClickHouse.
- [API reference](https://ch-quarry.vercel.app/docs/reference) &mdash; exact
  builder methods, helper surfaces, and selected live type tables.
- [Deep dive](https://ch-quarry.vercel.app/docs/concepts) &mdash; runtime
  semantics, scope rules, ClickHouse quirks, and architecture.

## Docs

Public docs are available at [ch-quarry.vercel.app](https://ch-quarry.vercel.app).

The docs source lives in [`docs/`](./docs) and is built with
[Fumadocs](https://fumadocs.dev). To run it locally:

```bash
pnpm install
pnpm --dir docs install
pnpm --dir docs dev
```

The site will be available on http://localhost:3000.

## Status

This package is still in the early alpha stage. The current API is already validated against a real ClickHouse instance in integration tests, but the surface may still change before a stable release.

## Current Scope

- typed selects and inserts, including `INSERT INTO ... SELECT`
- joins, subqueries, and CTEs
- `FINAL`, `PREWHERE`, `GROUP BY`, and `HAVING`
- `toSQL()` and `execute()`

See the [docs site](https://ch-quarry.vercel.app) for the full supported API, runtime semantics, and concept guides.

## Naming

The project name is Quarry. The npm package name is `@oorestisime/quarry`.
