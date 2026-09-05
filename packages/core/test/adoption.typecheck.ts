import {
  createClickHouseDB,
  sql,
  type Generated,
  type GeneratedAlways,
  type InferResult,
  type TypedDictionary,
  type TypedTable,
  type TypedView,
  type ColumnType,
  type ClickHouseUInt64,
} from "../src";

interface DB {
  users: {
    id: number;
    email: string;
    created: Generated<string>;
    computed: GeneratedAlways<number>;
  };
  events: { user_id: number; event_type: string };
  dict: TypedDictionary<{ label: string }>;
}
const db = createClickHouseDB<DB>();
// @ts-expect-error unknown aliased source
db.selectFrom("missing as m");
// @ts-expect-error dictionaries are not query sources
db.selectFrom("dict as d");
// @ts-expect-error unknown joined source
db.selectFrom("users as u").innerJoin("missing as m", "u.id", "m.id");
// @ts-expect-error invalid joined column
db.selectFrom("users as u").innerJoin("events as e", "u.id", "e.missing");

const nullable = db
  .selectFrom("users as u")
  .leftJoinNullable("events as e", "u.id", "e.user_id")
  .select("u.id", "e.event_type");
type NullableRow = InferResult<typeof nullable>;
({ id: 1, event_type: null }) satisfies NullableRow;
declare const nullableRow: NullableRow;
// @ts-expect-error right-side results require a null check
nullableRow.event_type.toUpperCase();
nullableRow.id.toFixed();

// @ts-expect-error explicit targets are required to prove SQL positional compatibility
db.insertInto("events").fromSelect(db.selectFrom("events").select("user_id", "event_type"));
const inserted = db.insertInto("events").columns("user_id", "event_type");
inserted.fromSelect(db.selectFrom("users").select("id", "email"));
// @ts-expect-error selection arity must match target
inserted.fromSelect(db.selectFrom("users").select("id"));
// @ts-expect-error SQL uses position, not aliases
inserted.fromSelect(db.selectFrom("users").select("email", "id"));
// @ts-expect-error table stars have no known selection order
inserted.fromSelect(db.selectFrom("events").selectAll());

const first = db.selectFrom("users").select("id", "email");
const union = first.unionAll(db.selectFrom("events").select("user_id", "event_type"));
({ id: 1, email: "a" }) satisfies InferResult<typeof union>;
// @ts-expect-error mismatched branch order
first.unionAll(db.selectFrom("events").select("event_type", "user_id"));
// @ts-expect-error mismatched branch count
first.unionAll(db.selectFrom("events").select("user_id"));

const fragment = db.selectFrom("users").select(sql<number>`42`.as("answer"));
({ answer: 42 }) satisfies InferResult<typeof fragment>;
// @ts-expect-error SQL fragment preserves its declared value type
({ answer: "42" }) satisfies InferResult<typeof fragment>;
const ranked = db.selectFrom("users").selectExpr((eb) => [
  eb.fn
    .rowNumber()
    .over({ orderBy: [{ by: eb.ref("id") }] })
    .as("rank"),
]);
({ rank: "1" }) satisfies InferResult<typeof ranked>;

db.insertInto("users").values([{ id: 1, email: "a" }]);
db.insertInto("users").values([{ id: 1, email: "a", created: "2026-01-01" }]);
// @ts-expect-error MATERIALIZED/ALIAS columns cannot be inserted
db.insertInto("users").values([{ id: 1, email: "a", computed: 1 }]);
// @ts-expect-error generated columns cannot be explicit insert targets
db.insertInto("users").columns("computed");
const selected = db.selectFrom("users").select("created", "computed");
({ created: "2026-01-01", computed: 1 }) satisfies InferResult<typeof selected>;

const dottedDB = createClickHouseDB<{ events: { "metrics.name": string } }>;
const dotted = dottedDB()
  .selectFrom("events")
  .select("metrics.name")
  .where("metrics.name", "=", "a");
({ "metrics.name": "a" }) satisfies InferResult<typeof dotted>;
// @ts-expect-error literal dotted columns retain their value types
dotted.where("metrics.name", "=", 1);

const scoped = createClickHouseDB<Pick<DB, "users" | "events">>();
scoped.selectFrom("users as u").innerJoin("events as e", "u.id", "e.user_id").select("u.email");
// @ts-expect-error service schemas expose only their chosen tables
createClickHouseDB<Pick<DB, "users">>().selectFrom("events");
// @ts-expect-error aliases cannot bypass the selected service schema
createClickHouseDB<Pick<DB, "users">>().selectFrom("events as e");

const typed = createClickHouseDB<{
  users: TypedTable<{ id: number; metric: ColumnType<string, number, number> }>;
  totals: TypedView<{ id: number; total: ClickHouseUInt64 }>;
}>();
const joined = typed
  .selectFrom("users as u")
  .select("u.metric")
  .innerJoin("totals as t", (eb) => eb.cmpRef("u.id", "=", "t.id"))
  .leftAntiJoin(typed.table("users").as("duplicate"), "u.id", "duplicate.id")
  .select("t.total");
({ metric: "5", total: "12" }) satisfies InferResult<typeof joined>;
// @ts-expect-error joined columns retain their select representation
({ metric: 5, total: 12 }) satisfies InferResult<typeof joined>;
const subquery = typed.selectFrom(joined.as("joined"));
subquery.where("metric", "=", 5).where("total", "=", 12n);
// @ts-expect-error a selection made before joining keeps its predicate type through a subquery
subquery.where("metric", "=", "5");
const cte = typed.with("joined", joined).selectFrom("joined").select("metric", "total");
({ metric: "5", total: "12" }) satisfies InferResult<typeof cte>;
typed.selectFrom("users as u").innerJoin("totals as t", (eb) =>
  // @ts-expect-error the join callback must not lose schema validation
  eb.cmpRef("u.missing", "=", "t.id"),
);
