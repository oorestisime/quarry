# Compare the same analytics query

This fixture runs tenant-scoped event counts and distinct-user counts, both with
and without an optional event filter, through four implementations. It asserts
identical results from seeded ClickHouse data and prints compiled SQL and
parameters. It checks the result types as part of `typecheck`.

From the repository root:

```sh
pnpm install
pnpm build:cli
docker compose -f examples/analytics-api/compose.yaml up -d --wait
pnpm --filter @quarry/example-comparison typecheck
pnpm --filter @quarry/example-comparison test
docker compose -f examples/analytics-api/compose.yaml down -v
```

Use the Compose credentials only for this local example. `CLICKHOUSE_URL` can
override the local endpoint, but the fixture assumes the example's schema and data.

Tested on ClickHouse 25.8 with TypeScript 6.0.2:

| Implementation | Version | Count result type in this fixture | SQL inspection |
| --- | --- | --- | --- |
| Raw `@clickhouse/client` | 1.18.2 | Explicit result interface | SQL and parameter map in `raw-analytics.ts` |
| Quarry | Current workspace | Inferred `string` | `toSQL()` returns named typed placeholders and parameter map |
| Kysely + `@founderpath/kysely-clickhouse` | 0.28.5 + 1.7.0 | Explicit `countAll<string>()` and `sql<string>` | `compile()` returns `?` placeholders and a parameter array |
| `@hypequery/clickhouse` | 2.9.1 | Inferred `string` | `toSQLWithParams()` returns `?` placeholders and a parameter array |

All four return the expected rows. The raw and Quarry implementations are shared
with the analytics API example. Kysely uses `WHERE` for tenant scope in this
fixture; Quarry and hypequery use `PREWHERE`. The Kysely example uses its SQL tag
for `uniqExact`. Hypequery's `COUNT(DISTINCT user_id)` is equivalent on these seeded
rows; `user_id` is non-nullable, so its `COUNT(user_id)` matches `count()` too.

The three SQL inspection APIs expose different intermediate formats. That alone
does not establish how a driver transmits values or how safely it handles every
input. This test measures correctness for these inputs, not throughput, compiler
speed, full feature coverage, security, or the best library for every application.

Try your actual joins, windows, settings, unusual identifiers, and schema size
before choosing. See the [Kysely dialect](https://github.com/founderpathcom/kysely-clickhouse)
and [hypequery's standalone builder](https://hypequery.com/docs/introduction) for their
own documentation. Hypequery's additional semantic/API packages are optional.
