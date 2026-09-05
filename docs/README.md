# Quarry documentation

The Fumadocs site uses Next.js and the workspace `quarry` package. From the repository root:

```bash
pnpm install
pnpm --dir docs dev
```

Open http://localhost:3000. `predev` and `prebuild` rebuild the core package automatically.

## Reader journey

- `content/docs/index.mdx`: start page and task navigation.
- `content/docs/guides/getting-started.mdx`: seeded database through a running API.
- `content/docs/guides/existing-project.mdx`: incremental adoption with the published API.
- `content/docs/recipes/`: working queries, SQL, and expected results.
- `content/docs/reference/`: API lookup and generated type tables.
- `content/docs/concepts/`: design and runtime semantics.
- `content/docs/releases.mdx`: published versus development behavior and upgrading.

Keep existing page URLs when reorganizing content. Sidebar order lives in each section's `meta.json`.

## Examples that stay in sync

Recipe query functions live in `examples/analytics-api/src/recipes.ts`. Mark a named region there with `// #region name` and `// #endregion name`, then reference it from an MDX code block:

````md
```ts recipe="dailyActivity"
```
````

`lib/remark-examples.mjs` fills that block during the docs build. The example's typecheck validates the functions, and `examples/analytics-api/test/docs.test.ts` checks the SQL and JSON blocks in the recipe pages against ClickHouse. Update the source function and its expected documentation output together. Do not duplicate the query body in MDX.

Self-contained `ts twoslash` blocks are checked during the site build and show editor hovers. Ordinary code blocks are explanatory snippets, not automatically typechecked; keep their setup explicit. Runtime claims need integration checks as well as type checks.

## Verify a change

```bash
pnpm --dir docs build
pnpm typecheck:docs
pnpm --filter @quarry/example-analytics typecheck
docker compose -f examples/analytics-api/compose.yaml up -d --wait
pnpm --filter @quarry/example-analytics test
docker compose -f examples/analytics-api/compose.yaml down
```

Check desktop and mobile navigation, search, code copying, and light/dark readability. CI runs the production build, type checks, schema regeneration, and example tests.

## Release maintenance

Before publishing a docs update for a release:

1. Update `content/docs/releases.mdx` and `components/release-notice.tsx` to identify the published version and any remaining development-only APIs.
2. Change the quickstart checkout from the development PR to the release tag and test the instructions from that checkout.
3. Update the homepage version label and the pinned installation command in the existing-project guide.
4. Check release-specific prose in recipes and reference links.

Do not label unreleased examples as available from npm just because the workspace package version is unchanged.

## Vercel

Use the existing project with Root Directory `docs`. Install the workspace with `pnpm install --dir .. --frozen-lockfile`; build with `pnpm build`. The full repository must be available because docs import core types and recipe source. The workspace lockfile controls all packages. No docs-framework migration is needed.
