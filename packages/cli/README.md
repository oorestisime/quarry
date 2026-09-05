# @oorestisime/quarry-cli

CLI for Quarry DB type introspection.

CLI 0.10.0 requires Node 22 or newer. Its generated `Generated` and
`GeneratedAlways` metadata requires `quarry` 0.10.0 or newer in your application.

## Usage

```bash
npx @oorestisime/quarry-cli introspect \
  --url http://localhost:8123 \
  --database analytics \
  --out db.ts
```

The generated file exports plain TypeScript `Tables`, `Views`, and `DB` types.

## Docs

- Guide: https://ch-quarry.vercel.app/docs/guides/introspection
- Main package: https://www.npmjs.com/package/quarry
