# Smoke App

This is a minimal consumer app that installs the published `@oorestisime/quarry` package from npm and runs it against a temporary ClickHouse instance started with Testcontainers.

## Run

```bash
pnpm install
pnpm start
```

This example is useful as a release smoke test because it exercises the package as an external consumer rather than as local source code.
