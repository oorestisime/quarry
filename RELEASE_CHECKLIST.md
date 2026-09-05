# Quarry release checklist

## Prepare 0.10.0

- Set `packages/core/package.json` and `packages/cli/package.json` to `0.10.0`.
- Refresh the lockfile with `pnpm install --lockfile-only`. Workspace links may
  already resolve correctly without a lockfile diff.
- Date the 0.10.0 changelog and update its comparison links. Include the Node 22
  minimum, positional `fromSelect` migration, result-setting and join-policy
  restrictions, and the core version required by generated CLI types.
- Leave the legacy `@oorestisime/quarry@0.8.1` wrapper pinned to core 0.9.0.
- Keep `docs/lib/release.ts` marked `published: false` until publication.
- Run `bash scripts/release-example.sh` with Node 22/24, pnpm 10, and Docker.
  This runs the release checks, installs both core and CLI tarballs in a fresh
  project, generates types against ClickHouse, checks them on TS 5.9/6/7, and
  executes a typed insert/select. It does not publish anything.
- Merge the release PR and wait for all CI checks on the merge commit.

## Publish manually

From a clean, up-to-date `main` checkout of the verified release commit, publish
core first and then the CLI. Complete the npm authentication prompts yourself.

```bash
pnpm --filter quarry publish --access public
pnpm --filter @oorestisime/quarry-cli publish --access public
npm view quarry@0.10.0 version
npm view @oorestisime/quarry-cli@0.10.0 version
```

Publish these two packages individually. The compatibility wrapper is unchanged.
If only one package publishes successfully, finish publishing the missing one
before announcing the release or removing the docs preview notice.

## Tag, verify, and announce

- Create and push `v0.10.0` pointing to the exact commit used for publication.
- Create the GitHub release from that tag using the dated changelog entry.
- Verify `npm install quarry@0.10.0 @clickhouse/client` and
  `npx @oorestisime/quarry-cli@0.10.0 introspect --help` in a fresh project.
- Follow the docs quickstart from a fresh `v0.10.0` checkout, including schema
  generation and the seeded API.
- Set `docsRelease.published` to `true` in a separate docs commit, merge it,
  and verify the production docs deployment.
- Announce the release with a query example, measured type-performance results,
  the upgrade guide, and links to the quickstart and playground.
