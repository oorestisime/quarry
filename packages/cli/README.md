# @oorestisime/quarry-cli

CLI for Quarry DB type introspection.

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
- Main package: https://www.npmjs.com/package/@oorestisime/quarry
