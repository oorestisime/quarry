# Roadmap

This roadmap reflects the current direction of the project. It is not a strict release contract, but it does show what is likely to be prioritized next.

## Current State

The current alpha already covers:

- typed selects and inserts
- joins, subqueries, and CTEs
- `FINAL`, `PREWHERE`, `GROUP BY`, and `HAVING`
- `toSQL()` and `execute()`
- integration tests against a real ClickHouse instance


### 1. More ClickHouse functions

The next major area is expanding the function surface.

The current expression builder only exposes a very small subset of ClickHouse functions. Near-term work should focus on the functions users actually reach for in analytics queries, especially where typed helpers improve ergonomics meaningfully.

Examples of likely candidates:

- aggregation helpers
- JSON helpers
- date/time helpers
- conditional helpers
- common string/array helpers

### 2. More ClickHouse-specific AST and builder support

The second priority is broadening support for ClickHouse-specific SQL features where they clearly improve the query-building experience.

This includes features that are awkward to represent as plain generic SQL, but common enough to deserve first-class support.

Examples:

- more clause-level ClickHouse features
- more source-level ClickHouse features
- more expression-level ClickHouse-specific syntax

The goal is still to stay corpus-driven and avoid implementing every possible ClickHouse feature up front.


### 3. Schema structure

The next larger system after the query builder is a richer schema model.

That schema work should support:

- runtime schema metadata
- introspection
- code generation
- migration support

The likely direction is:

- keep query-only usage lightweight
- add a richer schema-first path later
- let schema definitions feed into query typing, not replace the plain interface-based mode


### 4. Introspection and migrations

Schema work should eventually lead to:

- introspecting existing ClickHouse databases
- generating TypeScript-facing schema definitions
- migration support

The current expectation is that migration support should be ClickHouse-aware and pragmatic, not a naive generic schema diff engine.

### 5. Better public API documentation

The docs are still early. A later docs pass should add:

- fuller API reference material
- more end-to-end examples
- more ClickHouse-specific examples
- clearer guidance around runtime semantics and typed parameters

