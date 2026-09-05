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
- `content/docs/releases.mdx`: release status, version differences, and upgrading.

Keep existing page URLs when reorganizing content. Sidebar order lives in each section's `meta.json`.

## Visual system

`app/docs/docs.css` styles the handbook separately from the marketing pages.
`app/docs/[[...slug]]/page.tsx` owns article headers and section navigation.
`DocCard` and `DocCards` are available in MDX for task navigation; reserve tables
for comparisons and structured data. Code blocks receive language labels during
build, while explicit `title="..."` metadata takes precedence. Search, mobile
navigation, copying, and type hovers remain Fumadocs components.

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

The 0.10.0 docs already use versioned install commands, the `v0.10.0` example
checkout, and release-specific migration notes. `lib/release.ts` is the single
source for the displayed version and publication status.

Keep `docsRelease.published` false while preparing the release. This displays
preview notices on the site and beside the install/checkout instructions. It
does not imply that the npm packages or tag already exist.

After **both npm packages** and the **matching Git tag** are published, verify
the versioned install and quickstart commands, set `docsRelease.published` to
true, and deploy the docs. That one flag removes the preview notices throughout
the site; no prose rewrite is needed at publication.

For a later release, update the version, commands, source links, and migration
notes together and reset the flag while preparing it.

## Vercel

Use the existing project with Root Directory `docs`. Install the workspace with `pnpm install --dir .. --frozen-lockfile`; build with `pnpm build`. The full repository must be available because docs import core types and recipe source. The workspace lockfile controls all packages. No docs-framework migration is needed.
