# Anti-slop provenance

- Source: https://github.com/dmmulroy/anti-slop
- Commit: `c44ef22ca116d0ba62a3ff663a0bd13a3f3fa40b`
- Source assets: `skills/install-anti-slop/assets/anti-slop/`
- Installed at: `packages/core/tools/oxlint/anti-slop/`
- Entry point: `index.ts`, registered by `packages/core/.oxlintrc.json`.
- Dependencies: repository Oxlint `1.57.0`; core development dependency `@oxlint/plugins` pinned to `1.57.0`.

Assets are copied unchanged from the source commit. This provenance file is the
only addition. The nested ESLint Stylistic license and provenance are preserved.

All 18 generic plugin rules and the native `oxc/no-accumulating-spread` companion
rule are enabled as errors for core's source and Vitest configuration files. Core's
`test/**` directory is excluded from Oxlint at the user's request; tests remain
part of the normal typecheck and test commands. Effect rules are
included in the upstream assets but are not enabled because core does not declare
an Effect dependency. Other packages do not load this configuration.

The vendored directory is excluded from linting and formatting. After installation,
core's `require-readable-spacing` findings were fixed with blank-line-only edits.
Other findings remain for review; vendored plugin assets are unchanged.
