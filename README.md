# Chqry

Experimental ClickHouse-native query builder for TypeScript.

Chqry is a query builder first. It is not trying to be an ORM or hide ClickHouse behind a generic multi-dialect SQL abstraction.

## Docs

- [Approach](./docs/approach.md)
- [Usage](./docs/usage.md)

## Status

This package is still in the early alpha stage. The current API is already validated against a real ClickHouse instance in integration tests, but the surface may still change before a stable release.

## Current Scope

- typed selects and inserts
- joins, subqueries, and CTEs
- `FINAL`, `PREWHERE`, `GROUP BY`, and `HAVING`
- `toSQL()` and `execute()`

See [Usage](./docs/usage.md) for the current supported API and runtime semantics.

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

The project and package name are `chqry`.
