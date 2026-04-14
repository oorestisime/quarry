# Schema Normalization Exploration

## Executive Summary

This package is at the point where a richer schema model is justified, but the right move is not a big replacement of the current plain interface mode.

My current read is:

- A richer schema can and should coexist with the current `createClickHouseDB<DB>()` path.
- The core pressure is not only source capabilities like `view` vs `table`. The bigger issue is that Quarry currently uses one row type for three different jobs: select result typing, insert typing, and predicate typing.
- A proper schema story should model source kind and capabilities, but it should also model per-column operation types such as `select`, `insert`, and `where`.
- The most realistic first target is tables, views, and materialized views, with engine metadata and `FINAL` capability. Dictionaries should be accounted for in the design, but probably not treated as first-class `selectFrom(...)` sources in the first implementation.
- A runtime normalizer is only possible for a rich schema object. The current plain interface mode has no runtime schema value, so its "normalization" has to stay type-level.
- The rich path should probably be a compact ClickHouse-native DSL, not a verbose object where users repeatedly spell out `select`, `insert`, and `where` for every column.

I do not think the next step should be "ship every ClickHouse SQL feature first, then schema". The current type model is already the limiting factor for real queries.

## Decisions From This Review

These are the directions that seem settled enough to treat as working assumptions for the plan.

- Choose Option C: a rich schema object plus a normalizer, while keeping plain interface mode.
- Choose API Direction 1: a standalone schema-first path, with the schema object as the source of truth.
- Keep the existing plain interface mode for now. Even at `0.2.0`, I do not think we need to remove it to make progress.
- Through phases 1 to 3, the expectation should be that existing tests and `typecheck` continue to pass unchanged. New schema work should add new tests, not require rewriting the current plain-mode suite.
- Start with tables and views. Do not make materialized views or dictionaries part of the initial public schema surface.
- Prefer a structured engine API such as `table.rmt(...)` over opaque engine strings as the main DX.
- Do not start with a metadata-overlay API on top of plain interface mode. The rich schema path should be source-of-truth based.

One nuance on breaking changes: the package is early enough that internal refactors and public API iteration are fine, but I still would not spend that flexibility on dropping the lightweight plain-mode path unless maintaining both modes becomes clearly too costly.

## What I Reviewed

Core package internals:

- `src/query/db.ts`
- `src/query/types.ts`
- `src/query/select-query-builder.ts`
- `src/query/expression-builder.ts`
- `src/query/insert-query-builder.ts`
- `src/query/source-builder.ts`
- `src/type-utils.ts`
- `src/ast/query.ts`
- `src/compiler/query-compiler.ts`
- `src/query/helpers.ts`
- `src/schema/index.ts`

Tests and docs:

- `test/typecheck.ts`
- `test/cases.ts`
- `test/insert.test.ts`
- `test/integration/insert.test.ts`
- `docs/content/docs/roadmap.mdx`
- `docs/content/docs/concepts/approach.mdx`
- `docs/content/docs/concepts/architecture.mdx`
- `docs/content/docs/concepts/clickhouse-quirks.mdx`

ClickHouse reference material:

- `https://clickhouse.com/docs/en/sql-reference/statements/create/view`
- `https://clickhouse.com/docs/en/sql-reference/statements/select/from`
- `https://clickhouse.com/docs/en/sql-reference/statements/insert-into`
- `https://clickhouse.com/docs/en/engines/table-engines/special/dictionary`
- `https://clickhouse.com/docs/en/sql-reference/functions/ext-dict-functions`

## Current State in Quarry

### 1. The schema is still type-only

Quarry does not yet have a real schema subsystem.

- `src/schema/index.ts:1` is empty.
- `DatabaseSchema` is just `object`: `src/type-utils.ts:1`
- `TableRow<DB, Table>` assumes every source is just a row object: `src/type-utils.ts:16-20`

The docs already acknowledge this:

- `docs/content/docs/concepts/approach.mdx:104-122`
- `docs/content/docs/roadmap.mdx:65-81`

### 2. Named sources are all treated as table-like row objects

Today, the public source space is:

- string table expressions such as `"users as u"`
- explicit table source builders such as `db.table("users").as("u").final()`
- aliased subqueries such as `query.as("q")`

Relevant files:

- `src/query/types.ts:15-53`
- `src/query/source-builder.ts:4-42`
- `src/query/helpers.ts:65-84`

There is no distinction between:

- table
- view
- materialized view
- dictionary
- temporary view
- table function

Everything named in the `DB` generic is effectively treated as a table-shaped source.

### 3. Select, insert, and predicate typing are coupled to the same row shape

This is the main architectural constraint.

`selectFrom()` and `insertInto()` both derive from the same schema row model:

- `selectFrom`: `src/query/db.ts:37-45`
- `insertInto`: `src/query/db.ts:47-50`

Predicate typing also comes from the same row shape:

- `ResolveColumnType`: `src/query/types.ts:87-100`
- `PredicateValue`: `src/query/types.ts:202-210`
- `where(...)`: `src/query/select-query-builder.ts:142-180`
- `prewhere(...)`: `src/query/select-query-builder.ts:198-236`

This means Quarry currently assumes one column type is sufficient for:

- what `execute()` returns
- what `values([...])` accepts
- what `.where(...)` accepts

That is often not true for ClickHouse.

Examples:

- `DateTime64(3)` often wants `string` for selected rows under `JSONEachRow`, but `Date | string` for predicates and inserts.
- `UInt64` often wants `string` for selected rows, but may want `number | bigint | string` or explicit typed params on the input side.
- nullable/default/materialized/generated columns may be selectable but not user-insertable.

### 4. `fromSelect(...)` is not target-checked today

`InsertQueryBuilder.fromSelect(...)` accepts any select builder:

- `src/query/insert-query-builder.ts:49-60`

Tests cover compilation and execution, but not target-shape correctness by position:

- `test/insert.test.ts:117-139`
- `test/integration/insert.test.ts:150-209`

This matters because ClickHouse maps `INSERT INTO ... SELECT ...` by column position, not by alias name:

- `https://clickhouse.com/docs/en/sql-reference/statements/insert-into`

A richer schema helps here, but it does not automatically solve it because the select builder currently tracks output as an object, not as an ordered tuple of output columns.

### 5. The AST is still intentionally small

AST source kinds today:

- `table`
- `subquery`

Files:

- `src/ast/query.ts:50-63`
- `src/compiler/query-compiler.ts:95-103`

This is good news. It means a first schema pass does not need to explode the AST immediately. Many schema distinctions can remain builder/type-level and compile to the same SQL.

### 6. `FINAL` is currently too permissive for real ClickHouse semantics

Current behavior:

- `TableSourceBuilder.final()` just sets a boolean: `src/query/source-builder.ts:19-29`
- query-level `.final()` only checks whether the `FROM` source is a table node, not whether the engine supports `FINAL`: `src/query/select-query-builder.ts:567-579`

But ClickHouse only supports `FINAL` for specific engine families such as `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree`, and `VersionedCollapsingMergeTree`:

- `https://clickhouse.com/docs/en/sql-reference/statements/select/from`

So if schema work adds engine metadata, `FINAL` is one of the first places that should consume it.

## Requirements for a Proper Schema

Any serious schema system for Quarry should meet these requirements.

### 1. Coexist with plain interface mode

The current lightweight path is valuable and matches the docs' stated direction.

Good:

```ts
interface DB {
  event_logs: {
    user_id: number;
    created_at: string;
  };
}

const db = createClickHouseDB<DB>();
```

This should remain supported.

### 2. Model source kind and source capabilities

At minimum:

- selectable
- insertable
- finalCapable

And probably also:

- queryable-by-name
- source kind / object kind
- engine metadata for table-like sources

### 3. Model per-column operation types

The package needs a place to say things like:

- selected value type
- inserted value type
- predicate value type

This is the smallest useful split that addresses current real-world issues.

### 4. Account for ephemeral sources

Subqueries and CTEs are not catalog objects, but they do participate in the query typing system.

So any normalized model needs to handle:

- persistent named catalog sources
- derived in-query sources

### 5. Leave room for introspection and codegen

Even if codegen and migrations are later, schema shape decisions made now should not fight that future.

The docs already point there:

- `docs/content/docs/roadmap.mdx:67-92`

### 6. Avoid turning simple queries into ceremony

Quarry's value proposition is still a query builder, not a giant schema-first ORM. A richer schema needs to improve correctness without making the default path heavy.

## ClickHouse Source Taxonomy

This is the part that needs to be honest.

### Tables

Tables are the easy case.

They are usually:

- selectable
- insertable
- joinable

But they are not uniform because of engine families.

Engine metadata matters for:

- `FINAL`
- future engine-specific validation
- possible later source-level features

Important point: engine metadata should be part of the schema model, but it does not need to be fully modeled from day one. An engine name plus a few derived capabilities may be enough initially.

### Views

Normal views are essentially saved queries. ClickHouse documents them as equivalent to a subquery in `FROM`.

- `https://clickhouse.com/docs/en/sql-reference/statements/create/view`

For Quarry purposes, a normal view is best thought of as:

- selectable: yes
- insertable: no, in a conservative model
- finalCapable: no

One subtle point: views are not just a table with different permissions. They are query helpers.

I think there is a strong case for supporting two view modeling styles eventually:

- structural views, where the schema just declares the view's exposed columns
- derived views, where the view is declared as being based on another source and inherits its column metadata by default

That second form seems especially useful for cases like:

```sql
CREATE VIEW final_users AS SELECT * FROM users FINAL
```

In Quarry terms, that suggests a richer schema may want a compact form in the family of:

```ts
final_users: view.from("users").final()
```

or:

```ts
final_users: view.query("SELECT * FROM users FINAL").inherits("users")
```

The exact syntax is open, but the design point matters: a lot of views should probably inherit column behavior from another source rather than restating every column by hand.

If Quarry supports derived views, I think the most useful extra configuration points are probably:

- inherited column behavior from the base source by default
- optional baked-in `FINAL`
- optional baked-in `WHERE`
- maybe optional projection / column selection

That suggests something in the family of:

```ts
active_final_users: view.from("users").final().where(/* ... */)
```

or a callback-based derive form if that composes better with the query builder internals.

### Materialized Views

This category is trickier than it looks.

ClickHouse currently has several view-like objects under the broader `CREATE VIEW` space:

- normal views
- materialized views
- refreshable materialized views
- window views

Relevant doc:

- `https://clickhouse.com/docs/en/sql-reference/statements/create/view`

Observations:

- materialized views store data or route inserts into a target table
- refreshable materialized views are operationally different from insert-trigger materialized views
- window views are even more specialized and likely outside Quarry's near-term scope

For schema design, this means `materialized_view` is not a single simple thing. The schema model should at least leave room for sub-kinds, even if the first release collapses them into one `kind`.

Conservative first-pass behavior could be:

- selectable: yes
- insertable: opt-in or deferred
- finalCapable: engine-dependent or false by default

I would avoid hard-coding strong assumptions about materialized view insertability until Quarry has a firmer DDL or introspection story.

### Dictionaries

Dictionaries need especially careful treatment.

There are at least two different concepts here:

1. Real ClickHouse dictionaries created with `CREATE DICTIONARY`, usually accessed through `dictGet*` and related functions.
2. Tables using the `Dictionary(...)` table engine, which expose dictionary data in a table-like way.

Relevant docs:

- `https://clickhouse.com/docs/en/sql-reference/functions/ext-dict-functions`
- `https://clickhouse.com/docs/en/engines/table-engines/special/dictionary`

This has an important design consequence:

- A dictionary object is not automatically the same thing as a normal `FROM some_name` source in Quarry.
- But a table using the `Dictionary` engine is table-like and can fit under `kind: table` plus engine metadata.

So if Quarry wants to "support dicts" honestly, it should probably distinguish:

- dictionary catalog objects used by functions
- dictionary-engine tables used as named sources

That strongly argues against a single naive `kind: "dictionary"` if the intent is "this should work everywhere a table works".

### Table Functions and Parameterized Views

ClickHouse `FROM` supports table functions.

- `https://clickhouse.com/docs/en/sql-reference/statements/select/from`

Parameterized views are used as table functions.

- `https://clickhouse.com/docs/en/sql-reference/statements/create/view`

Quarry does not currently support table functions in the AST or source builder surface. This matters because a future schema system might want to describe parameterized views or dictionary-backed table functions, but the query surface is not ready yet.

This is another reason to keep first-pass schema support focused on named sources that already fit the current builder surface.

### Temporary Views and Other Session-Scoped Objects

ClickHouse also has temporary views.

- `https://clickhouse.com/docs/en/sql-reference/statements/create/view`

These are real object kinds, but they are session-scoped and not good first candidates for Quarry schema modeling. They matter mostly as a reminder that "catalog object" and "query source" are not identical concepts.

## Design Options

I do not think there is one obvious implementation. Here are the main directions and their tradeoffs.

### Option A: Add source capabilities on top of the current row-only model

Idea:

- keep `DB` as a map of row objects
- add metadata only for source kind / engine / capabilities
- keep column typing as it is today

Example direction:

```ts
createClickHouseDB<DB>({
  client,
  metadata: {
    event_logs: { kind: "table", engine: "ReplacingMergeTree", finalCapable: true },
    signup_view: { kind: "view", insertable: false },
  },
});
```

Pros:

- smallest change
- solves view/table/materialized-view capability gating
- gives a place for engine metadata and `FINAL`

Cons:

- does not solve the `select` vs `insert` vs `where` type split
- does not help with typed predicate values for date/time and other ClickHouse-specific runtime mismatches
- likely not enough on its own to justify the architectural churn

Verdict:

- I do not like this direction as the main next step
- it is too partial to be satisfying
- it solves some source-level correctness while leaving the more important column typing problem untouched

### Option B: Stay in plain interface mode, but add type wrappers for source and column roles

Idea:

- keep schema entirely type-level
- let users annotate columns or sources with wrapper types

Conceptually:

```ts
interface DB {
  events: Table<{
    created_at: Column<{
      select: string;
      insert: Date | string;
      where: Date | string;
    }>;
  }>;
}
```

Pros:

- no runtime schema object required
- preserves the generic `createClickHouseDB<DB>()` call shape
- can express per-column operation types in theory

Cons:

- pushes a lot of complexity into the type layer
- awkward ergonomics for users
- weak story for runtime metadata, introspection, or code generation
- not a great fit for engine metadata and source kind modeling unless the wrappers become a de facto schema DSL anyway

Verdict:

- I do not think this is good DX
- it is possible in theory, but it feels too verbose and too type-driven for the value it gives
- I would only choose this if Quarry wanted to remain strictly type-only forever

### Option C: Add a rich schema object plus a normalizer, while keeping plain mode

Idea:

- keep `createClickHouseDB<DB>()` as-is
- add a richer schema path using a schema object and normalizer
- internally normalize the rich schema into source descriptors
- keep plain mode as a permissive adapter at the type level

Conceptually:

```ts
const schema = defineSchema({
  events: table.rmt({
    created_at: quarry.DateTime64(3),
    amount: quarry.UInt64(),
    user_id: quarry.UInt32(),
  }),
  daily_view: view({
    event_date: quarry.Date(),
    total_amount: quarry.UInt64(),
  }),
});

const db = createClickHouseDB({ schema, client });
```

The important design idea here is that `quarry.DateTime64(3)` already knows the default Quarry semantics for that ClickHouse type. In other words, the user should not usually have to spell out:

- selected/runtime type
- insert/input type
- predicate/input type

Those defaults should be inferred from:

- the ClickHouse type itself
- Quarry's runtime contract around `JSONEachRow`
- Quarry's input conventions for params and inserts

The fully explicit `column({ select, insert, where })` shape may still exist internally, or as an advanced escape hatch, but it should not be the main DX.

Pros:

- best path for source kind, capabilities, and per-column operation types
- clean home for engine metadata
- room for later introspection/codegen
- can coexist with plain interface mode

Cons:

- more public API surface
- needs a real normalization and type-derivation layer
- plain mode can share the same internal model, but not the same runtime metadata path, because `createClickHouseDB<DB>()` does not receive a schema value at runtime
- needs a careful escape hatch for cases where the default inferred mapping is not what the user wants

Verdict:

- this is the strongest long-term direction
- but it should be introduced carefully, not all at once

### Option D: Replace plain interface mode with schema-first only

Idea:

- deprecate `createClickHouseDB<DB>()`
- require a rich schema descriptor for all use

Pros:

- simplest mental model internally
- strongest consistency between runtime metadata and types

Cons:

- breaks current ergonomics
- contradicts the docs' stated direction
- too heavy for many query-only use cases

Verdict:

- not recommended
- plain interface mode is too useful to remove, especially for query-only use cases

## Internal Representation Options

Even if Option C is the public direction, there is still an internal design choice to make.

First, what do I mean by `Scope` here?

In Quarry today, `Scope` is the type-level map of aliases and columns that are currently available while building a query.

Example:

```ts
db
  .selectFrom("users as u")
  .innerJoin("event_logs as e", "u.id", "e.user_id")
```

At that point the current scope is roughly:

```ts
{
  u: users columns,
  e: event_logs columns,
}
```

This is what powers things like:

- `"u.id"` being a valid column ref
- `"missing.id"` being a type error
- `selectAll("u")` knowing which columns belong to `u`

The internal design question is whether that scope should continue to store plain row shapes only, or whether it should become a richer structure that also knows predicate types and source capabilities.

### Internal Model 1: Keep `Scope` as selected-row data and add parallel metadata maps

Idea:

- `Scope` stays close to what it is today: alias -> selected row shape
- add a parallel alias/source metadata map used for predicate typing and capability checks
- catalog metadata lives elsewhere

Pros:

- preserves a lot of existing `ResolveColumnType`-style behavior for selection and output typing
- expression-builder code that uses selected/runtime row shape may need less churn
- easier to keep `selectAll()` semantics stable

Cons:

- more generic parameters across the query builder types
- harder to reason about because alias data is split across multiple type maps
- more room for type-level drift between row map and metadata map

When this is attractive:

- if the primary goal is minimizing immediate disruption to the current type helpers

### Internal Model 2: Make `Scope` entries richer and derive all column behavior from them

Idea:

- `Scope` no longer maps alias -> plain row object
- instead it maps alias -> structured scope entry with:
  - select row
  - predicate row
  - source kind / capabilities
  - maybe source identity

Then helper types become:

- column names from `select` row
- selection output from `select` row
- predicate typing from `where` row

Pros:

- one main source of truth for in-scope columns
- cleaner capability checks for `.final()`, `where`, joins, and future source-level features
- aligns well with a normalized schema object

Cons:

- larger rewrite of `src/query/types.ts`
- `ScopeMap` changes shape, so many helper types need rewriting
- more up-front churn

When this is attractive:

- if the goal is a coherent long-term foundation rather than the smallest patch

### My Lean on the Internal Model

I lean slightly toward Internal Model 2.

Reason:

- the current type system is centralized enough that a single coherent change may be better than accumulating parallel maps and additional generic plumbing
- the existing `ScopeMap` abstraction is package-internal, so changing it is disruptive but contained

That said, Internal Model 1 is safer if the immediate goal is to land a thin internal foundation first and keep public behavior almost unchanged.

## How Both Modes Can Share One Internal Model

This section is really answering a simpler question: can the AST, compiler, and builders still work against one shared internal schema abstraction if Quarry supports both plain interface mode and rich schema mode?

I think the answer is yes.

The core idea is:

- there should be one internal source model that the builders and type helpers are designed around
- rich schema mode can provide both runtime metadata and compile-time types for that model
- plain interface mode can provide only the compile-time side of that model

What changes between the two modes is not the AST or the compiler. What changes is how much source metadata Quarry has available.

### Rich schema mode

In rich schema mode, the user passes a real schema value.

That means Quarry can:

- normalize that value into runtime source metadata
- infer TypeScript types from the same schema definition
- use source metadata for things like engine-aware `FINAL` behavior

### Plain interface mode

In plain interface mode, the user writes:

```ts
createClickHouseDB<DB>()
```

There is no runtime schema object there. Quarry only gets type information.

That means:

- Quarry can still synthesize the same internal source model in the type system
- but there is no runtime metadata value to inspect for capabilities unless the user also opts into metadata somehow

This does not mean Quarry needs two different builders or two different compilers.

It means:

- one builder/compiler pipeline
- one internal source abstraction
- two ways of arriving at it:
  - rich schema mode: value + types
  - plain mode: types only

The practical implication is simple:

- when rich metadata exists, Quarry can be stricter
- when only plain interface types exist, Quarry should stay more permissive for things that require runtime/source metadata

So yes, the AST, compiler, and builder can absolutely still use one shared internal model. The difference is just whether Quarry has a runtime value describing that model, or only compile-time types.

## A More Honest Normalized Model

This is not a final API proposal. It is a sketch of the shape Quarry likely needs internally.

```ts
type SourceKind =
  | "table"
  | "view"
  | "materialized_view"
  | "dictionary"
  | "cte"
  | "subquery";

interface NormalizedColumn<Select, Insert = Select, Where = Select> {
  clickhouseType?: string;
  select: Select;
  insert: Insert;
  where: Where;
  nullable?: boolean;
}

interface NormalizedSource {
  kind: SourceKind;
  capabilities: {
    selectable: boolean;
    insertable: boolean;
    finalCapable: boolean | "unknown";
  };
  engine?: {
    name: string;
    family?: string;
  };
  columns: Record<string, NormalizedColumn<any, any, any>>;
}
```

Notes:

- `kind` should capture the broad object category.
- `capabilities` should drive what the query builder allows.
- `engine` should be metadata, not a separate source kind.
- `columns` need distinct operation types.

The biggest question is where the selected row and predicate row live during query building.

One clarification: `NormalizedColumn<Select, Insert, Where>` is the internal shape I expect Quarry to need. I do not think that exact shape should be the primary public API.

## Public API Directions Worth Considering

These are the two main public API shapes I would evaluate further.

### API Direction 1: Standalone schema-first path

```ts
const schema = defineSchema({
  events: table.rmt({
    created_at: quarry.DateTime64(3),
    amount: quarry.UInt64(),
  }),
  signup_view: view({
    created_at: quarry.DateTime64(3),
  }),
});

const db = createClickHouseDB({ schema, client });
```

Pros:

- clean separation between plain mode and rich mode
- easiest to normalize cleanly
- best long-term home for codegen/introspection

Cons:

- requires a new schema DSL
- more new concepts at once

Important note:

- I think this schema DSL should be compact and ClickHouse-first
- engine helpers like `table.rmt(...)` are a better fit than forcing users to repeat engine strings and explicit column role objects
- column constructors like `quarry.DateTime64(3)` or `quarry.UInt64()` should infer default Quarry semantics automatically

### API Direction 2: Metadata overlay on top of plain mode

```ts
interface DB {
  events: {
    created_at: string;
  };
}

const db = createClickHouseDB<DB>({
  client,
  metadata: {
    events: {
      kind: "table",
      engine: "ReplacingMergeTree",
    },
  },
});
```

Pros:

- less disruptive to current users
- a softer migration path

Cons:

- duplicates names and structure
- easy for type-only schema and metadata to drift apart
- awkward for expressing richer per-column type roles unless the metadata object itself becomes a schema DSL

### My Lean on Public API

I would start with API Direction 1.

I do not think API Direction 2 should be part of the initial implementation plan.

If a gentler adoption path is ever needed later, it can be revisited, but I would not make it part of the first schema rollout.

## File-by-File Impact

Below is the rough impact if Quarry adds a serious normalized schema layer.

| File | Why it changes | Expected impact |
| --- | --- | --- |
| `src/schema/index.ts` | new schema types, builders, normalizer | high |
| `src/type-utils.ts` | row derivation helpers need to become source/column derivation helpers | high |
| `src/query/types.ts` | the main type core; source kinds, column refs, predicate typing, selection output | very high |
| `src/query/db.ts` | `createClickHouseDB`, `selectFrom`, `insertInto`, `with` need catalog awareness | high |
| `src/query/source-builder.ts` | source builders need kind/capability metadata and maybe richer source identity | medium |
| `src/query/select-query-builder.ts` | capability gating for sources and `FINAL`; where/having overloads may need new type helpers | medium |
| `src/query/expression-builder.ts` | depends on how scope entries are modeled; could be medium-to-high | medium |
| `src/query/insert-query-builder.ts` | insertable-source gating and maybe future `fromSelect` typing improvements | medium |
| `src/query/helpers.ts` | source parsing/normalization helpers | low-to-medium |
| `src/ast/query.ts` | probably unchanged in phase 1 | low |
| `src/compiler/query-compiler.ts` | probably unchanged in phase 1 unless new source SQL forms land | low |

The most important point is that this is not just a new `schema/` module. `query/types.ts` is where the real architectural change lives.

## Risks and Tradeoffs

### 1. Type complexity risk

The current type system is still readable. A richer schema model can make it much harder to maintain if too much is introduced at once.

### 2. Backward-compatibility risk

If schema-first mode adds strict capability checks, current plain mode should probably stay permissive where metadata is unknown. Otherwise existing users will see surprising breakages.

For the planned phases, I do not think Quarry should need to change or weaken any of the existing tests. The bar should be:

- existing runtime tests continue to pass unchanged
- existing `typecheck` coverage continues to pass unchanged
- new schema behavior is covered by additive tests only

### 3. `with(...)` and CTEs are a hidden sharp edge

Today `with(...)` literally extends the `Sources` generic with the result row type of the query:

- `src/query/db.ts:25-35`

That works because `Sources` is just a row map. If `Sources` becomes a catalog of descriptors, CTEs need to be turned into synthetic source descriptors, not raw rows.

### 4. `fromSelect(...)` remains its own problem

Even with a better schema, exact `fromSelect(...)` typing is hard because ClickHouse maps insert columns by position and Quarry's select builder exposes an object result type, not an ordered output tuple.

Schema work helps but does not make this free.

### 5. Dictionaries are easy to over-model badly

If Quarry jumps straight to `kind: "dictionary"` and treats dictionaries as normal named sources, it risks encoding the wrong abstraction. The function-oriented dictionary story and the queryable dictionary-engine table story are different.

### 6. Materialized views should be modeled conservatively

There are enough variants and operational semantics here that the schema should leave room for nuance instead of hard-coding simplistic behavior.

## A Realistic Incremental Path

This is the phased path I currently think is most defensible.

### Phase 1: Introduce internal normalized source descriptors

Goals:

- create internal source descriptor types
- keep plain interface mode working
- do not expose a big public schema DSL yet
- prepare for source kind and per-column operation types
- keep all existing tests and `typecheck` passing unchanged

Possible scope:

- define internal source and column descriptor types
- adapt current plain mode into permissive descriptor types in the type system
- do not require runtime metadata yet
- add no public rich-schema surface yet, or only internal scaffolding hidden behind package internals

Why this phase exists:

- it lets the internal typing model evolve before the public API grows

### Phase 2: Add public schema-first support for tables and views

Goals:

- add `defineSchema(...)`
- support source kind and capabilities
- support per-column `select` / `insert` / `where` types
- add structured engine helpers for tables such as `table.rmt(...)`
- support both structural views and inherited/derived views

At this point Quarry can address:

- `FINAL` capability checks
- view non-insertability
- date/time and other predicate/input typing mismatches
- inherited view definitions such as `view.from("users").final()`

### Phase 3: Improve insert-side typing where realistic

Goals:

- tighten `insertInto(...)` to insertable sources only
- improve column-list-aware `fromSelect(...)` validation if practical
- keep the standalone rich schema object as the only public rich-schema source of truth

This phase may need a separate design if exact positional typing is desired.

### Phase 4: Future expansion after the first schema release

Possible directions:

- decide whether to add materialized views as selectable schema objects
- decide whether to add dictionary metadata for `dictGet*` functions
- add query support for dictionary-engine tables via normal table metadata if it proves useful
- add table-function support if parameterized views or dictionary table functions become important

I would not promise any of this in the first schema release.

## Decisions and Remaining Open Questions

Directions that now seem decided:

1. Choose Option C.
2. Start with the standalone schema-first API.
3. Keep plain interface mode in place.
4. Keep materialized views and dictionaries out of the initial public schema release.
5. Use a structured engine API.
6. Keep the rich schema object as the public source of truth.

Questions that still feel meaningfully open:

1. How strict should plain interface mode become when no metadata is available?
2. Is `fromSelect(...)` exact positional typing a requirement for the first schema pass, or explicitly a later step?
3. What is the best public shape for inherited views: chained helpers like `view.from("users").final().where(...)`, or a callback-based derive form?
4. How much of the structured engine API should ship in the first cut beyond obvious helpers like `table.rmt(...)`?

## My Current Recommendation

If I had to choose a direction today, I would do this:

1. Keep plain interface mode.
2. Add a richer schema-first path rather than replacing the current one.
3. Make the core internal concept a normalized source descriptor with:
   - source kind
   - capabilities
   - engine metadata
   - per-column `select` / `insert` / `where` types
4. Make the public rich path compact and ClickHouse-native. I would actively prefer something in the family of:

```ts
const schema = defineSchema({
  events: table.rmt({
    created_at: quarry.DateTime64(3),
    amount: quarry.UInt64(),
  }),
});
```

with inference of the default `select` / `insert` / `where` behavior from the ClickHouse type constructor itself.
5. Support tables and views first. Treat inherited views as an important part of the initial design.
6. Omit materialized views and dictionaries from the first public schema release.
7. Keep existing tests and `typecheck` passing unchanged across phases 1 to 3, and add new schema tests alongside them.
8. Avoid a first pass that only adds source capabilities but keeps the one-row-type-for-everything model. That would solve part of the problem while leaving the most important type mismatch untouched.

## Bottom Line

Quarry is ready for a proper schema direction, but the hardest part is not adding a `schema/` package. The hard part is replacing the current assumption that a source is just a row object.

The right target is a normalized model that can express:

- what a source is
- what it can do
- what each column looks like when selected
- what each column accepts when inserted
- what each column accepts in predicates

That model can coexist with plain interface mode, but only if we are honest that plain mode and rich-schema mode will share one internal type model, not one shared runtime normalizer.
