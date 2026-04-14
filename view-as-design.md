# Query-Backed Views Design

## Goal

Make views in Quarry schema mode represent real ClickHouse views, not just inherited column bags.

The target use case is something like:

```sql
CREATE OR REPLACE VIEW some_view_name AS
SELECT *
FROM xyz_table FINAL
```

and also richer views with:

- joins
- projections
- aliases
- casts and formatting
- aggregates

## Proposed Public API

The main API should be query-backed, but staged so that the view builder receives a `db` that already knows the base schema:

```ts
const schema = defineSchema({
  xyz_table: table.replacingMergeTree({
    id: UInt32(),
    created_at: DateTime64(3),
  }),
}).views((db) => ({
  some_view: view.as(
    db
      .selectFrom(db.table("xyz_table").final().as("x"))
      .selectAll("x")
  ),
}));
```

And a richer example:

```ts
const schema = defineSchema({
  users: table.mergeTree({
    id: UInt32(),
    email: String(),
  }),

  events: table.replacingMergeTree({
    user_id: UInt32(),
    created_at: DateTime64(3),
  }),
}).views((db) => ({
  user_activity_view: view.as(
    db
      .selectFrom("users as u")
      .leftJoin("events as e", "u.id", "e.user_id")
      .selectExpr((eb) => [
        "u.id",
        "u.email",
        eb.fn.max("e.created_at").as("last_seen_at"),
      ])
      .groupBy("u.id", "u.email")
  ),
}));
```

This staged form matters because it solves the sibling-schema problem cleanly: by the time `.views((db) => ...)` runs, `db` already knows about the tables declared in the base schema.

## Why This Direction

This is the right fit because a real ClickHouse view is defined by a query.

That means the schema API should center query-backed definitions instead of a smaller inherited-shape abstraction.

## What `view.as(...)` Should Produce

A normalized view source should retain:

- kind: `view`
- the view query AST
- the output column names
- the output TypeScript/runtime types
- the output ClickHouse types
- source dependencies

That gives Quarry what it needs for:

- query-builder selectability from the view
- future DDL generation
- future introspection/migration work

## Key Implementation Problem

Today Quarry tracks output TypeScript types reasonably well, but it does not yet track output ClickHouse types deeply enough for arbitrary query-backed views.

To support `view.as(...)`, selected expressions need enough metadata to answer two different questions:

1. What JS/TS type comes back from `JSONEachRow`?
2. What ClickHouse type does this expression produce?

For example:

- `u.id` may be `number` at runtime and `UInt32` in ClickHouse
- `toDate(e.created_at)` may be `string` at runtime and `Date` in ClickHouse
- `count()` may be `string` at runtime and `UInt64` in ClickHouse

## Metadata Model

I do not think the entire builder should become heavy or noisy.

The likely minimal model is that selection/expression internals carry additive metadata, something conceptually like:

```ts
type ExpressionMeta = {
  runtimeType: unknown;
  clickhouseType?: string;
};
```

This should stay package-internal if possible.

The public builder API should not suddenly require users to annotate every expression.

## What Can Be Inferred Automatically

These should be inferable in the first pass:

- source refs such as `"u.id"`
- `selectAll()` / `selectAll("u")`
- aliases
- direct column selections
- built-in cast functions like `toDate`, `toDateTime`, `toDateTime64`, `toUInt32`, `toUInt64`
- obvious aggregates like `count`, `sum`, `avg`, `min`, `max`

That already covers a large amount of realistic view definitions.

## What Needs an Escape Hatch

Some expressions will not be inferable safely from day 1:

- `raw(...)`
- custom functions
- mixed arithmetic expressions
- less common ClickHouse functions whose return types Quarry does not know yet

For those cases, Quarry should have a local annotation escape hatch on expressions, not a second explicit view schema.

Conceptually:

```ts
eb.raw("someCustomExpr(...)")
  .typed<number>("UInt32")
  .as("foo")
```

The important design point is that the annotation is local to the ambiguous expression.

## Suggested Phases

### Phase 1: Design and internal plumbing

- add internal expression metadata for ClickHouse output types
- make `selectAll` and direct refs preserve source column ClickHouse types

### Phase 2: Minimal staged `.views((db) => ({ ... }))`

- support query-backed views whose output metadata can be fully inferred
- normalize the view query AST and output columns into the schema
- make those views selectable in schema mode

### Phase 3: Local annotation escape hatch

- add expression-level `.typed<Runtime>("ClickHouseType")`
- support ambiguous/raw/custom expressions in view definitions

### Phase 4: Future DDL generation

- compile stored view AST back into `CREATE OR REPLACE VIEW ... AS ...`
- use normalized dependencies and output metadata for migration tooling

## What I Would Not Do

- introduce raw SQL view definitions as the preferred path
- require a second explicit column shape next to the view query

Those all create duplicate sources of truth.

## Open Questions

1. Should `view.as(...)` accept only a prebuilt select query, or also a callback in some cases?
2. How much function return metadata should ship in the first pass?
3. Should `selectAll()` in a view definition preserve source ClickHouse metadata exactly, including wrappers like `Nullable(...)`?
4. How should CTEs inside a view definition be represented in normalized schema metadata?
5. Should the local expression annotation be called `.typed(...)`, `.returns(...)`, or something else?

## Recommendation

Make staged query-backed views the next real view direction:

```ts
defineSchema({ ...tables... }).views((db) => ({
  some_view: view.as(db.selectFrom(...).select(...)),
}))
```

The main architectural work should be adding just enough internal expression metadata to normalize view query outputs without bloating the public query-builder API.
