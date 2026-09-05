# A ClickHouse analytics API with Quarry

Run a complete example with seeded events, generated database types, optional filters, request cancellation, and inferred result rows. Requires Node 22+ and Docker.

From the repository root:

```sh
corepack enable
pnpm install
pnpm build:cli
docker compose -f examples/analytics-api/compose.yaml up -d --wait
pnpm --filter @quarry/example-analytics db:generate
pnpm --filter @quarry/example-analytics start
```

In another terminal:

```sh
curl 'http://localhost:3001/analytics'
curl 'http://localhost:3001/analytics?eventType=purchase&from=2026-08-04'
curl 'http://localhost:3001/sql?eventType=purchase'
```

The first response is:

```json
[
  { "event_type": "purchase", "events": "2", "users": "1" },
  { "event_type": "signup", "events": "2", "users": "2" }
]
```

The second returns one purchase. Counts remain strings because they are UInt64 values in ClickHouse's JSON output. `/sql` returns the parameterized SQL and separate values without executing it.

The [docs recipes](https://ch-quarry.vercel.app/docs/recipes) render functions from `src/recipes.ts` directly. Their tests check the displayed SQL and results against this seed data.

Read [`src/analytics.ts`](src/analytics.ts) for the reusable query and [`src/server.ts`](src/server.ts) for input validation and cancellation. Filters are composed without changing the inferred result type. `DEMO_TENANT_ID` defaults to 1; the seed includes another tenant to verify that the query scopes its reads. This example uses a fixed demo identity; connect the tenant ID to your application's authenticated caller when adapting it.

The checked-in generated schema lets you inspect types before starting Docker. Regenerate it after schema changes; CI regenerates and checks the diff so stale types do not silently accumulate. The introspection config contains local demo credentials.

```sh
pnpm --filter @quarry/example-analytics typecheck
pnpm --filter @quarry/example-analytics test
docker compose -f examples/analytics-api/compose.yaml down -v
```
