<p align="center">
  <img src="./.github/assets/logo.png" width="120" alt="Quarry logo" />
</p>

<h1 align="center">Quarry</h1>

<p align="center">
  <a href="https://www.npmjs.com/package/@oorestisime/quarry"><img src="https://img.shields.io/npm/v/%40oorestisime%2Fquarry" alt="npm version" /></a>
</p>

Experimental ClickHouse-native query builder for TypeScript.

Quarry is a query builder first. It is not trying to be an ORM or hide ClickHouse behind a generic multi-dialect SQL abstraction.

## Docs

The full documentation site lives in [`docs/`](./docs) and is built with
[Fumadocs](https://fumadocs.dev). To run it locally:

```bash
cd docs
pnpm install
pnpm dev
```

The site will be available on http://localhost:3000.

Highlights:

- [Approach](./docs/content/docs/concepts/approach.mdx) &mdash; what Quarry is and is not
- [Getting started](./docs/content/docs/guides/getting-started.mdx) &mdash; install and run your first query
- [Roadmap](./docs/content/docs/roadmap.mdx) &mdash; what is likely to land next

## Status

This package is still in the early alpha stage. The current API is already validated against a real ClickHouse instance in integration tests, but the surface may still change before a stable release.

## Current Scope

- typed selects and inserts
- joins, subqueries, and CTEs
- `FINAL`, `PREWHERE`, `GROUP BY`, and `HAVING`
- `toSQL()` and `execute()`

See the [docs site](./docs) for the full supported API, runtime semantics, and concept guides.

## Example

```ts
const rows = await db
  .selectFrom("event_logs as e")
  .select("e.user_id", "e.event_type")
  .where("e.event_type", "=", "signup")
  .orderBy("e.created_at", "desc")
  .limit(50)
  .execute()
```

## Naming

The project name is Quarry. The npm package name is `@oorestisime/quarry`.
