import {
  createClickHouseDB,
  type AliasedExpression,
  type Expression,
  type InferResult,
  type ClickHouseDateTime64,
  type TypedDictionary,
} from "../src";

const db = createClickHouseDB<{
  users: { id: number; email: string };
  typed_samples: { nickname: string | null; created_at: ClickHouseDateTime64 };
  labels: TypedDictionary<{ label: string }>;
}>();

// Finding 4: typed expressions must preserve value compatibility.
db.selectFrom("users").selectExpr((eb) => {
  // @ts-expect-error lower cannot accept a numeric expression
  eb.fn.lower(eb.ref("id"));

  // @ts-expect-error numeric expressions cannot be assigned to string expressions
  const text: Expression<string> = eb.ref("id");

  // @ts-expect-error aliasing must also preserve the expression's value type
  const aliasedText: AliasedExpression<string, "value"> = eb.ref("id").as("value");

  void text;
  void aliasedText;
  eb.fn.has(eb.val(["1"]), eb.fn.toInt64(eb.val(1)));
  eb.fn.dictGetOrDefault("labels", "label", "id", eb.fn.toInt64(eb.val(1)));

  return [eb.fn.lower(eb.ref("email")).as("lowercase_email")];
});

// Finding 5: casts propagate a nullable input to their result.
const nullableCasts = db
  .selectFrom("typed_samples")
  .selectExpr((eb) => [
    eb.fn.toUInt32("nickname").as("numeric_value"),
    eb.fn.toString("nickname").as("text_value"),
  ]);

type NullableCastRow = InferResult<typeof nullableCasts>;

({ numeric_value: null, text_value: null }) satisfies NullableCastRow;

({ numeric_value: 1, text_value: "1" }) satisfies NullableCastRow;

function consumeNullableCasts(row: NullableCastRow) {
  // @ts-expect-error a nullable numeric result requires a null check
  row.numeric_value.toFixed(2);
  // @ts-expect-error a nullable string result requires a null check
  row.text_value.toUpperCase();
}

void consumeNullableCasts;

const castSubquery = db
  .selectFrom(nullableCasts.as("c"))
  .selectExpr((eb) => [
    eb.fn.toFloat64("c.numeric_value").as("number"),
    eb.fn.lower(eb.ref("c.text_value")).as("text"),
  ]);

({ number: null, text: null }) satisfies InferResult<typeof castSubquery>;

// Predicate inputs such as Date must not leak into selected string values.
const castValues = db
  .selectFrom("typed_samples")
  .selectExpr((eb) => [
    eb.fn.toInt64("nickname").as("integer"),
    eb.fn.toDate("nickname").as("date"),
    eb.fn.toDateTime64("nickname", 3).as("timestamp"),
    eb.fn.toString(eb.ref("created_at")).as("text"),
    eb.fn.lower(eb.fn.toInt64(eb.val(1))).as("lower"),
    eb.fn.min(eb.fn.now()).as("now"),
  ]);

type CastValueRow = InferResult<typeof castValues>;

({
  integer: null,
  date: null,
  timestamp: null,
  text: "date",
  lower: "1",
  now: "date",
}) satisfies CastValueRow;

function consumeCastValues(row: CastValueRow) {
  row.integer?.toUpperCase();
  row.date?.toUpperCase();
  row.timestamp?.toUpperCase();
  row.text.toUpperCase();
  row.now.toUpperCase();
}

void consumeCastValues;

// Finding 6: empty averages and quantiles serialize as null under JSONEachRow.
const emptyAggregates = db
  .selectFrom("users")
  .where("id", "=", 0)
  .selectExpr((eb) => [
    eb.fn.avg("id").as("average"),
    eb.fn.avgIf("id", eb.cmp("id", "<", 0)).as("conditional_average"),
    eb.fn.quantile(0.5, "id").as("median"),
  ]);

type EmptyAggregateRow = InferResult<typeof emptyAggregates>;

({ average: null, conditional_average: null, median: null }) satisfies EmptyAggregateRow;

({ average: 1, conditional_average: 1, median: 1 }) satisfies EmptyAggregateRow;

function consumeEmptyAggregates(row: EmptyAggregateRow) {
  // @ts-expect-error the serialized average can be null
  row.average.toFixed(2);
  // @ts-expect-error the serialized conditional average can be null
  row.conditional_average.toFixed(2);
  // @ts-expect-error the serialized quantile can be null
  row.median.toFixed(2);
}

void consumeEmptyAggregates;

// Finding 8: an empty predicate group must be rejected before execution.
db.selectFrom("users").where((eb) => {
  // @ts-expect-error AND requires at least one condition
  eb.and([]);
  // @ts-expect-error OR requires at least one condition
  eb.or([]);

  const condition = eb.cmp("id", ">", 0);
  eb.or([condition]);

  return eb.and([condition]);
});
