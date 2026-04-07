# Quarry Documentation Site

The public documentation site for [`@oorestisime/quarry`](../README.md), built
with [Fumadocs](https://fumadocs.dev) on Next.js 16.

## Local development

The docs site links the parent library via a `file:..` dependency, so the
parent's `dist/` must exist before the docs build. You only need to do the
install dance once:

```bash
# from the repo root
pnpm install            # installs library deps
cd docs
pnpm install            # installs docs deps and links the parent package
pnpm dev                # predev rebuilds the parent, then starts Next.js
```

The dev server runs on http://localhost:3000.

The `predev` and `prebuild` scripts call `pnpm --dir .. build` automatically,
so as long as the parent's dependencies are installed, you never have to
remember to rebuild it manually.

## Project layout

```
docs/
├── app/                      Next.js App Router
│   ├── (home)/               Marketing/landing layout
│   ├── api/search/route.ts   Search endpoint (Orama)
│   ├── docs/                 Docs route group
│   ├── global.css            Tailwind v4 + Fumadocs CSS
│   └── layout.tsx            Root layout + RootProvider
├── components/
│   └── mdx.tsx               MDX component registry (Twoslash, TypeTable, ...)
├── content/docs/             MDX source content
├── lib/
│   ├── layout.shared.tsx     Shared header/nav config
│   └── source.ts             Fumadocs source loader
├── source.config.ts          Fumadocs MDX config (Twoslash + AutoTypeTable)
├── next.config.mjs
├── postcss.config.mjs
├── tsconfig.json
└── package.json
```

## Authoring conventions

- Source files live in `content/docs/**/*.mdx`.
- Sidebar order is controlled by `meta.json` files in each section.
- Code blocks marked ` ```ts twoslash ` are type-checked at build time using
  the real `@oorestisime/quarry` types via the tsconfig path mapping in
  `tsconfig.json`. This means examples cannot lie about types.
- The `<auto-type-table />` MDX component renders a live type table from the
  TypeScript source. Example:

  ```mdx
  <auto-type-table path="../../src/query/db.ts" name="CreateClickHouseDBOptions" />
  ```

  Paths are resolved relative to the MDX file.

## Deploying to Vercel

1. Import the repository in Vercel.
2. Set **Root Directory** to `docs`.
3. Framework preset: Next.js (autodetected).
4. Override the **Install Command** to:
   ```bash
   pnpm install --dir .. --frozen-lockfile && pnpm install --frozen-lockfile
   ```
   This installs the parent library's dependencies first, so the `prebuild`
   step that runs `pnpm --dir .. build` has everything it needs.
5. Build command: `pnpm build` (default &mdash; triggers `prebuild` → builds
   the parent → `next build`).
6. Output directory: `.next` (default).

The Vercel build runs from `/docs` but the deployment checks out the whole
repository, so the `file:..` link to the parent package and the
`auto-type-table` lookups that point at `../src/...` all resolve correctly.

## TypeScript reference content

We use [`fumadocs-typescript`](https://fumadocs.dev/docs/integrations/typescript)
to render type information directly from the library source. This is preferred
over running TypeDoc and committing generated markdown, because:

- the rendered tables only include what we explicitly opt into,
- everything stays in sync with the source automatically,
- there is no separate generation step to forget.

If you ever want bulk auto-generated reference (e.g. one page per exported
symbol), `typedoc` + `typedoc-plugin-markdown` can be wired up as a pre-build
step that writes into `content/docs/reference/api/`.
