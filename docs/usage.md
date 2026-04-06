# Usage

## Create A Typed DB

```ts
import { createClient } from "@clickhouse/client";
import { createClickHouseDB } from "chqry";

interface DB {
  event_logs: {
    user_id: number;
    event_type: string;
    created_at: string;
    event_date: string;
    properties: string;
  };
  users: {
    id: number;
    email: string;
    status: string;
  };
}

const client = createClient({
  url: "http://localhost:8123",
});

const db = createClickHouseDB<DB>({ client });
```

## Build And Execute A Query

```ts
const rows = await db
  .selectFrom("event_logs as e")
  .select("e.user_id", "e.event_type")
  .where("e.event_type", "=", "signup")
  .orderBy("e.created_at", "desc")
  .limit(50)
  .execute();
```

## Inspect SQL Without Executing

```ts
const compiled = db
  .selectFrom("event_logs as e")
  .select("e.user_id", "e.event_type")
  .where("e.event_type", "=", "signup")
  .toSQL();

compiled.query;
compiled.params;
```

## Use Explicit Parameter Types

Use `param(...)` when you need to control the ClickHouse placeholder type directly.

```ts
import { param } from "chqry";

const rows = await db
  .selectFrom("event_logs as e")
  .select("e.user_id", "e.created_at")
  .where("e.created_at", ">=", param("2025-01-01 00:00:00", "DateTime"))
  .execute();
```

## Null Checks

Use the dedicated helpers instead of comparing against `null` directly.

```ts
const rows = await db
  .selectFrom("users as u")
  .select("u.id", "u.email")
  .whereNull("u.email")
  .execute();
```

```ts
const rows = await db
  .selectFrom("users as u")
  .select("u.id", "u.email")
  .whereNotNull("u.email")
  .execute();
```

## Joins

### Simple join

```ts
const rows = await db
  .selectFrom("users as u")
  .innerJoin("event_logs as e", "u.id", "e.user_id")
  .select("u.id", "u.email", "e.event_type")
  .execute();
```

### Multi-condition join

```ts
const rows = await db
  .selectFrom("users as a")
  .innerJoin("users as b", (eb) =>
    eb.and([eb.cmpRef("a.id", "=", "b.id"), eb.cmpRef("a.email", "=", "b.email")]),
  )
  .select("a.id", "a.email")
  .execute();
```

### `FINAL` on a joined table source

```ts
const rows = await db
  .selectFrom("users as u")
  .innerJoin(db.table("event_logs").as("e").final(), "u.id", "e.user_id")
  .select("u.id", "u.email", "e.event_type")
  .execute();
```

## CTEs

```ts
const rows = await db
  .with("active_users", (db) =>
    db
      .selectFrom("event_logs as e")
      .select("e.user_id")
      .where("e.event_type", "=", "signup")
      .groupBy("e.user_id"),
  )
  .selectFrom("active_users as au")
  .innerJoin("users as u", "u.id", "au.user_id")
  .select("u.id", "u.email")
  .execute();
```

## Subqueries

### In `WHERE`

```ts
const activeUsers = db
  .selectFrom("event_logs as e")
  .select("e.user_id")
  .where("e.event_type", "=", "signup");

const rows = await db
  .selectFrom("users as u")
  .select("u.id", "u.email")
  .where("u.id", "in", activeUsers)
  .execute();
```

### In joins

```ts
const downloads = db
  .selectFrom(db.table("event_logs").as("e").final())
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
  .groupBy("e.user_id")
  .as("downloads");

const rows = await db
  .selectFrom("users as u")
  .leftJoin(downloads, "downloads.user_id", "u.id")
  .select("u.id", "u.email", "downloads.event_count")
  .execute();
```

## Grouping And Having

```ts
const rows = await db
  .selectFrom("event_logs as e")
  .selectExpr((eb) => ["e.user_id", eb.fn.count().as("event_count")])
  .groupBy("e.user_id")
  .having("event_count", ">", param(1, "Int64"))
  .orderBy("event_count", "desc")
  .execute();
```

## Inserts

Inserts always take an array of rows.

```ts
await db
  .insertInto("users")
  .values([
    {
      id: 1,
      email: "alice@example.com",
      status: "active",
    },
  ])
  .execute();
```

## `selectAll()` Rules

- `selectAll()` with no argument is for single-source queries
- in joined queries, prefer `selectAll("alias")`

```ts
db.selectFrom("users as u").selectAll("u");
```

## Runtime Semantics To Know About

### LEFT JOIN

ClickHouse does not default unmatched `LEFT JOIN` columns to `null` unless `join_use_nulls = 1` is enabled.

Without that setting, unmatched right-side columns come back as type defaults such as:

- `0`
- `''`
- `false`

### Date values

The current integration tests show that raw JS `Date` values work with `@clickhouse/client` query params when the placeholder type is explicitly `DateTime`.

Even so, explicit strings or `param(...)` are still the clearest way to control query-side date/time values.
