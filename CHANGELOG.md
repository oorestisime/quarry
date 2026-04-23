# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-04-23

### Changed

- **Breaking:** `execute()`, `executeTakeFirst()`, and `executeTakeFirstOrThrow()` now accept `ClickHouseExecutionOptions` (an options bag) instead of an optional `ClickHouseClient`. If you were passing a client instance directly, pass it as `{ client }` instead.

### Added

- Pass ClickHouse execution options (`queryId`, `clickhouse_settings`) through `execute()` on select and insert builders.
- Accept pre-built `Expression` objects in `where`, `prewhere`, and `having`.
- Add heavy-hitter expression helpers: `if`, `least`, `greatest`, `ceil`, `floor`, `countDistinct`, `now64`, `toUInt8`, `toYear`, `toMonth`.
- Allow `.with()` to accept a pre-built `SelectQueryBuilder` for CTEs.

## [0.6.0] - 2025-04-11

### Added

- Initial release with typed selects and inserts, including `INSERT INTO ... SELECT`.
- Joins, subqueries, and CTEs.
- `FINAL`, `PREWHERE`, `GROUP BY`, and `HAVING`.
- `toSQL()` and `execute()`.

[Unreleased]: https://github.com/oorestisime/quarry/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/oorestisime/quarry/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/oorestisime/quarry/releases/tag/v0.6.0
