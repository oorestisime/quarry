# @oorestisime/quarry-cli

CLI for Quarry schema introspection.

## Usage

```bash
npx @oorestisime/quarry-cli introspect \
  --url http://localhost:8123 \
  --database analytics \
  --out schema.ts
```

The generated schema module imports runtime helpers from `@oorestisime/quarry`.

## Docs

- Guide: https://ch-quarry.vercel.app/docs/guides/introspection
- Main package: https://www.npmjs.com/package/@oorestisime/quarry
