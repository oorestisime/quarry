# Contributing to Quarry

Start with a real ClickHouse query and the behavior you need. The [query issue form](https://github.com/oorestisime/quarry/issues/new?template=query.yml) asks for the SQL, a reduced schema, versions, and expected results. Use synthetic data.

## Local development

Use Node 22 or 24 and the pnpm version recorded in `package.json`:

```sh
corepack enable
pnpm install
pnpm build:cli
pnpm test
pnpm typecheck
pnpm typecheck:native
```

Docker is required for `pnpm test:integration`. Tests create and remove their own ClickHouse containers. CI covers ClickHouse 24.8 and 25.8, while package-consumer checks cover TypeScript 5.9, 6, and 7 under ESM NodeNext and Bundler resolution.

## Changes and tests

For a query feature, include the expected SQL/parameters, a negative type case, and a real ClickHouse test for behavior that depends on the server. Keep ordered selections and result serialization in mind: INSERT SELECT and UNION ALL are positional, and JSON format settings can change runtime types.

Run `pnpm test:performance` when changing exported types. It compiles consumers of the built declarations with large schemas, joins, array operations, and CTEs. Review compiler traces before raising an instantiation budget. `node scripts/check-type-performance.mjs --native` reports the same workloads under TS 7.

Before opening a PR, run:

```sh
pnpm release:check
```

Explain the concrete before/after behavior, compatibility changes, and validation in the PR. The docs source is in `docs/`; `pnpm --dir docs dev` starts it locally.

## Release preparation

Quarry is pre-1.0. Minor versions can contain documented breaking changes; patch versions should preserve the API. Releases should include:

1. A changelog entry and matching core/CLI version updates when both changed. Document the core version required by newly generated CLI types.
2. Identical root and core-package READMEs, with absolute asset links for npm.
3. Passing release checks and packed-package checks under all supported compilers.
4. npm publication followed by a GitHub release using the same version and changelog text.

Publishing is a maintainer action. A successful build or PR does not automatically publish packages.
