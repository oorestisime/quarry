# Approach

## Goals

This project is a ClickHouse-native TypeScript query builder.

It is not trying to be:

- a generic SQL abstraction across many databases
- an ORM

The current focus is a query builder that compiles to ClickHouse SQL cleanly and stays honest about runtime behavior.

## Design Principles

### ClickHouse first

The API should model ClickHouse concepts directly instead of pretending every database behaves like Postgres.

That is why the current surface already includes things like:

- `final()`
- `prewhere(...)`
- `settings(...)`

### Runtime-honest typing

The builder should type `execute()` based on what `@clickhouse/client` actually returns.

Examples from the current integration tests:

- `UInt64` -> `string`
- `count()` -> `string`
- `DateTime64(3)` -> `string`
- `Decimal(18, 2)` -> `number`

The goal is to avoid pretending values are safer or more transformed than they really are.

### Builder and compiler stay separate

Internally, the library is still organized around:

`builder -> AST -> compiler -> SQL + params`

The AST is an implementation detail. The public API should feel like a query builder, not like AST construction.

### Small explicit API over magical inference

When ClickHouse semantics are ambiguous, prefer explicit APIs over guessing.

Examples:

- `whereNull(...)` / `whereNotNull(...)` instead of raw `null` equality
- `param(value, "Type")` when the placeholder type should be explicit
- first-class table sources like `db.table("event_logs").as("e").final()` for advanced cases

## Current Scope

The current implementation covers:

- typed `selectFrom(...)`
- typed `select(...)`, `selectExpr(...)`, `selectAll()` and `selectAll("alias")`
- joins, including multi-condition joins via callback
- subqueries and aliased subqueries
- CTEs via `.with(name, callback)`
- `where(...)`, `whereRef(...)`, `whereNull(...)`, `whereNotNull(...)`
- `prewhere(...)`, `prewhereRef(...)`
- `groupBy(...)`, `having(...)`
- inserts via `insertInto(...).values([...])`
- `toSQL()` and `execute()`

## What Is Deliberately Out Of Scope Right Now

- migrations
- schema diffing
- a full raw SQL API beyond the current typed builder surface

Those may come later, but the current goal is to keep the core query builder small and solid.

## Schema Story Today

Today, the query builder uses a plain TypeScript interface for the database shape.

```ts
interface DB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
  };
}
```

That means the schema is type-only for now.

A richer schema/runtime metadata system may come later, but it should feed into the query builder rather than replace this query-first mode.
