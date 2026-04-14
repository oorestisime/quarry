import type { ExprNode } from "../ast/query";
import type { ClickHouseParam } from "../param";
import type { NormalizedSchemaColumn } from "../schema";
import type { ScopeMap } from "../type-utils";
import { escapeSingleQuotedString } from "../utils/string";
import { createValueNode, isQueryLike, toSubqueryExpr } from "./helpers";
import type {
  ArrayColumnRef,
  ColumnRef,
  EmptyableColumnRef,
  ExpressionPredicateValue,
  ParamLike,
  PredicateOperator,
  QueryLike,
  RefPredicateOperator,
  ResolveArrayElementType,
  ResolveColumnType,
  ResolvePredicateColumnType,
  StringColumnRef,
} from "./types";

type ScopeColumnMap = Record<string, Record<string, NormalizedSchemaColumn>>;

export class Expression<T, Where = T> {
  constructor(
    readonly node: ExprNode,
    readonly clickhouseType?: string,
  ) {}

  as<Alias extends string>(alias: Alias): AliasedExpression<T, Alias, Where> {
    return new AliasedExpression(this.node, alias, this.clickhouseType);
  }
}

export class AliasedExpression<_Value, Alias extends string, _Where = _Value> {
  constructor(
    readonly node: ExprNode,
    readonly alias: Alias,
    readonly clickhouseType?: string,
  ) {}
}

type ArrayInput<Scope extends ScopeMap> = ArrayColumnRef<Scope> | Expression<readonly unknown[]>;
type EmptyableInput<Scope extends ScopeMap> =
  | EmptyableColumnRef<Scope>
  | Expression<string>
  | Expression<string | null>
  | Expression<readonly unknown[]>
  | Expression<readonly unknown[] | null>;
type StringInput<Scope extends ScopeMap> =
  | StringColumnRef<Scope>
  | Expression<string>
  | Expression<string | null>;
type StringValueInput =
  | ParamLike<string>
  | ClickHouseParam<string | null>
  | Expression<string>
  | Expression<string | null>;
type NumericValueInput = ParamLike<number> | Expression<number>;
type ExpressionInput<Scope extends ScopeMap> = ColumnRef<Scope> | Expression<unknown>;
const DATE_TIME_UNITS = {
  NANOSECOND: {
    literal: "nanosecond",
    add: "addNanoseconds",
    subtract: "subtractNanoseconds",
  },
  MICROSECOND: {
    literal: "microsecond",
    add: "addMicroseconds",
    subtract: "subtractMicroseconds",
  },
  MILLISECOND: {
    literal: "millisecond",
    add: "addMilliseconds",
    subtract: "subtractMilliseconds",
  },
  SECOND: {
    literal: "second",
    add: "addSeconds",
    subtract: "subtractSeconds",
  },
  MINUTE: {
    literal: "minute",
    add: "addMinutes",
    subtract: "subtractMinutes",
  },
  HOUR: {
    literal: "hour",
    add: "addHours",
    subtract: "subtractHours",
  },
  DAY: {
    literal: "day",
    add: "addDays",
    subtract: "subtractDays",
  },
  WEEK: {
    literal: "week",
    add: "addWeeks",
    subtract: "subtractWeeks",
  },
  MONTH: {
    literal: "month",
    add: "addMonths",
    subtract: "subtractMonths",
  },
  QUARTER: {
    literal: "quarter",
    add: "addQuarters",
    subtract: "subtractQuarters",
  },
  YEAR: {
    literal: "year",
    add: "addYears",
    subtract: "subtractYears",
  },
} as const;
type DateTimeUnit = keyof typeof DATE_TIME_UNITS;
type DateTimeUnitInput = DateTimeUnit | Lowercase<DateTimeUnit>;
type ValueInput<Scope extends ScopeMap, Value = unknown> = ColumnRef<Scope> | Expression<Value>;
type ResolveValueInput<Scope extends ScopeMap, Value extends ValueInput<Scope>> =
  Value extends ColumnRef<Scope>
    ? ResolveColumnType<Scope, Value>
    : Value extends Expression<infer Result>
      ? Result
      : never;
type ResolveRefOrExpressionInput<Scope extends ScopeMap, Value> =
  Value extends ColumnRef<Scope>
    ? ResolveColumnType<Scope, Value>
    : Value extends Expression<infer Result>
      ? Result
      : never;
type ResolveStringValueInput<Value> =
  Value extends ClickHouseParam<infer Result>
    ? Result
    : Value extends Expression<infer Result>
      ? Result
      : Value extends string
        ? string
        : never;
type MaybeNullable<Result, Output> = null extends Result ? Output | null : Output;
type MaybeNullableFromStringValues<Values extends readonly unknown[], Output> = MaybeNullable<
  { [Index in keyof Values]: ResolveStringValueInput<Values[Index]> }[number],
  Output
>;
type NonNullValue<T> = Exclude<T, null>;
type ComparableValueInput<Value> = NonNullValue<Value> | ClickHouseParam<Value> | Expression<Value>;
type FallbackValueInput<Value> = ParamLike<NonNullValue<Value>> | Expression<NonNullValue<Value>>;
type ResolveValueInputs<Scope extends ScopeMap, Values extends readonly ValueInput<Scope>[]> = {
  [Index in keyof Values]: ResolveValueInput<Scope, Values[Index]>;
};
type AllNullable<Types extends readonly unknown[]> = Types extends readonly [
  infer Head,
  ...infer Tail,
]
  ? null extends Head
    ? AllNullable<Tail>
    : false
  : true;
type CoalesceResult<Types extends readonly unknown[]> =
  | Exclude<Types[number], null>
  | (AllNullable<Types> extends true ? null : never);

interface ExpressionBuilderFunctions<Scope extends ScopeMap> {
  count(): Expression<string>;
  countIf(condition: Expression<unknown>): Expression<string>;
  now(): Expression<string>;
  today(): Expression<string>;
  jsonExtractString(
    column: ColumnRef<Scope> | Expression<unknown>,
    key: string,
  ): Expression<string>;
  sum(value: ValueInput<Scope>): Expression<number | string>;
  sumIf(value: ValueInput<Scope>, condition: Expression<unknown>): Expression<number | string>;
  avg(value: ValueInput<Scope>): Expression<number>;
  avgIf(value: ValueInput<Scope>, condition: Expression<unknown>): Expression<number>;
  min<Value extends ValueInput<Scope>>(value: Value): Expression<ResolveValueInput<Scope, Value>>;
  max<Value extends ValueInput<Scope>>(value: Value): Expression<ResolveValueInput<Scope, Value>>;
  uniq(value: ValueInput<Scope>): Expression<string>;
  uniqExact(value: ValueInput<Scope>): Expression<string>;
  uniqIf(value: ValueInput<Scope>, condition: Expression<unknown>): Expression<string>;
  groupArray<Value extends ValueInput<Scope>>(
    value: Value,
  ): Expression<NonNullValue<ResolveValueInput<Scope, Value>>[]>;
  any<Value extends ValueInput<Scope>>(value: Value): Expression<ResolveValueInput<Scope, Value>>;
  anyLast<Value extends ValueInput<Scope>>(
    value: Value,
  ): Expression<ResolveValueInput<Scope, Value>>;
  toInt32(value: ColumnRef<Scope> | Expression<unknown>): Expression<number>;
  toInt64(value: ColumnRef<Scope> | Expression<unknown>): Expression<string>;
  toUInt32(value: ColumnRef<Scope> | Expression<unknown>): Expression<number>;
  toUInt64(value: ColumnRef<Scope> | Expression<unknown>): Expression<string>;
  toFloat32(value: ColumnRef<Scope> | Expression<unknown>): Expression<number>;
  toFloat64(value: ColumnRef<Scope> | Expression<unknown>): Expression<number>;
  toDate(value: ColumnRef<Scope> | Expression<unknown>): Expression<string>;
  toDateTime(value: ColumnRef<Scope> | Expression<unknown>): Expression<string>;
  toDateTime64(
    value: ColumnRef<Scope> | Expression<unknown>,
    precision: number,
  ): Expression<string>;
  toStartOfMonth(value: ExpressionInput<Scope>): Expression<string>;
  toStartOfWeek(value: ExpressionInput<Scope>): Expression<string>;
  toStartOfDay(value: ExpressionInput<Scope>): Expression<string>;
  toStartOfYear(value: ExpressionInput<Scope>): Expression<string>;
  formatDateTime(value: ExpressionInput<Scope>, format: string): Expression<string>;
  dateDiff(
    unit: DateTimeUnitInput,
    start: ExpressionInput<Scope>,
    end: ExpressionInput<Scope>,
  ): Expression<string>;
  dateAdd(
    unit: DateTimeUnitInput,
    amount: NumericValueInput,
    value: ExpressionInput<Scope>,
  ): Expression<string>;
  dateSub(
    unit: DateTimeUnitInput,
    amount: NumericValueInput,
    value: ExpressionInput<Scope>,
  ): Expression<string>;
  toYYYYMM(value: ExpressionInput<Scope>): Expression<number>;
  toYYYYMMDD(value: ExpressionInput<Scope>): Expression<number>;
  toString(value: ColumnRef<Scope> | Expression<unknown>): Expression<string>;
  toDecimal64(value: ColumnRef<Scope> | Expression<unknown>, scale: number): Expression<number>;
  toDecimal128(value: ColumnRef<Scope> | Expression<unknown>, scale: number): Expression<number>;
  has<Ref extends ArrayColumnRef<Scope>>(
    array: Ref,
    element:
      | ParamLike<ResolveArrayElementType<Scope, Ref>>
      | Expression<ResolveArrayElementType<Scope, Ref>>,
  ): Expression<number>;
  has<Element>(
    array: Expression<readonly Element[]>,
    element: ParamLike<Element> | Expression<Element>,
  ): Expression<number>;
  hasAny<Ref extends ArrayColumnRef<Scope>>(
    array: Ref,
    elements:
      | ParamLike<readonly ResolveArrayElementType<Scope, Ref>[]>
      | Expression<readonly ResolveArrayElementType<Scope, Ref>[]>,
  ): Expression<number>;
  hasAny<Element>(
    array: Expression<readonly Element[]>,
    elements: ParamLike<readonly Element[]> | Expression<readonly Element[]>,
  ): Expression<number>;
  hasAll<Ref extends ArrayColumnRef<Scope>>(
    array: Ref,
    elements:
      | ParamLike<readonly ResolveArrayElementType<Scope, Ref>[]>
      | Expression<readonly ResolveArrayElementType<Scope, Ref>[]>,
  ): Expression<number>;
  hasAll<Element>(
    array: Expression<readonly Element[]>,
    elements: ParamLike<readonly Element[]> | Expression<readonly Element[]>,
  ): Expression<number>;
  length<Ref extends ArrayColumnRef<Scope>>(array: Ref): Expression<string>;
  length<Element>(array: Expression<readonly Element[]>): Expression<string>;
  isNull<Value extends ValueInput<Scope>>(value: Value): Expression<number>;
  isNotNull<Value extends ValueInput<Scope>>(value: Value): Expression<number>;
  nullIf<Value extends ValueInput<Scope>>(
    value: Value,
    nullValue: ComparableValueInput<ResolveValueInput<Scope, Value>>,
  ): Expression<ResolveValueInput<Scope, Value> | null>;
  coalesce<Values extends readonly [ValueInput<Scope>, ValueInput<Scope>, ...ValueInput<Scope>[]]>(
    ...values: Values
  ): Expression<CoalesceResult<ResolveValueInputs<Scope, Values>>>;
  ifNull<Value extends ValueInput<Scope>>(
    value: Value,
    defaultValue: FallbackValueInput<ResolveValueInput<Scope, Value>>,
  ): Expression<NonNullValue<ResolveValueInput<Scope, Value>>>;
  empty<Value extends EmptyableInput<Scope>>(
    value: Value,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, number>>;
  notEmpty<Value extends EmptyableInput<Scope>>(
    value: Value,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, number>>;
  like<Value extends StringInput<Scope>, Pattern extends StringValueInput>(
    value: Value,
    pattern: Pattern,
  ): Expression<
    MaybeNullable<
      ResolveRefOrExpressionInput<Scope, Value> | ResolveStringValueInput<Pattern>,
      number
    >
  >;
  ilike<Value extends StringInput<Scope>, Pattern extends StringValueInput>(
    value: Value,
    pattern: Pattern,
  ): Expression<
    MaybeNullable<
      ResolveRefOrExpressionInput<Scope, Value> | ResolveStringValueInput<Pattern>,
      number
    >
  >;
  concat<Parts extends readonly [StringValueInput, StringValueInput, ...StringValueInput[]]>(
    ...parts: Parts
  ): Expression<MaybeNullableFromStringValues<Parts, string>>;
  lower<Value extends StringInput<Scope>>(
    value: Value,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>;
  upper<Value extends StringInput<Scope>>(
    value: Value,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>;
  substring<Value extends StringInput<Scope>>(
    value: Value,
    offset: NumericValueInput,
    length: NumericValueInput,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>;
  trimBoth<Value extends StringInput<Scope>>(
    value: Value,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>;
  trimLeft<Value extends StringInput<Scope>>(
    value: Value,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>;
  trimRight<Value extends StringInput<Scope>>(
    value: Value,
  ): Expression<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>;
}

export class ExpressionBuilder<Scope extends ScopeMap> {
  constructor(private readonly scopeColumns?: ScopeColumnMap) {}

  ref<Ref extends ColumnRef<Scope>>(
    ref: Ref,
  ): Expression<ResolveColumnType<Scope, Ref>, ResolvePredicateColumnType<Scope, Ref>> {
    return new Expression({ kind: "ref", name: ref }, this.resolveClickHouseType(ref));
  }

  val<T>(value: ParamLike<T>): Expression<T> {
    const node = createValueNode(value);
    return new Expression(node, node.clickhouseType);
  }

  raw<T = unknown>(sql: string): Expression<T> {
    return new Expression({ kind: "raw", sql });
  }

  cmp<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    left: Ref,
    operator: Operator,
    right:
      | ExpressionPredicateValue<ResolvePredicateColumnType<Scope, Ref>, Operator>
      | Expression<ResolvePredicateColumnType<Scope, Ref>>
      | QueryLike,
  ): Expression<number>;
  cmp<Value, Operator extends PredicateOperator>(
    left: Expression<Value>,
    operator: Operator,
    right: ExpressionPredicateValue<Value, Operator> | Expression<Value> | QueryLike,
  ): Expression<number>;
  cmp(
    left: ColumnRef<Scope> | Expression<unknown>,
    operator: PredicateOperator,
    right: unknown,
  ): Expression<number> {
    return new Expression({
      kind: "binary",
      left: this.toExpr(left),
      op: operator.toUpperCase(),
      right: this.toPredicateRightExpr(right),
    });
  }

  readonly fn: ExpressionBuilderFunctions<Scope> = {
    count: () => this.callFunction<string, string | number | bigint>("count", [], "UInt64"),
    countIf: (condition: Expression<unknown>) =>
      this.callFunction<string, string | number | bigint>("countIf", [condition.node], "UInt64"),
    now: () => this.callFunction<string, string | Date>("now", [], "DateTime"),
    today: () => this.callFunction<string, string | Date>("today", [], "Date"),
    jsonExtractString: (column: ColumnRef<Scope> | Expression<unknown>, key: string) =>
      this.callFunction<string>("JSONExtractString", [
        this.toExpr(column),
        this.toStringLiteral(key),
      ], "String"),
    sum: (value: ValueInput<Scope>) =>
      this.callFunction<number | string>("sum", [this.toExpr(value)]),
    sumIf: (value: ValueInput<Scope>, condition: Expression<unknown>) =>
      this.callFunction<number | string>("sumIf", [this.toExpr(value), condition.node]),
    avg: (value: ValueInput<Scope>) =>
      this.callFunction<number>("avg", [this.toExpr(value)], "Float64"),
    avgIf: (value: ValueInput<Scope>, condition: Expression<unknown>) =>
      this.callFunction<number>("avgIf", [this.toExpr(value), condition.node], "Float64"),
    min: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<
        ResolveValueInput<Scope, Value>,
        ResolveValueInput<Scope, Value>
      >("min", [this.toExpr(value)], this.resolveValueClickHouseType(value)),
    max: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<
        ResolveValueInput<Scope, Value>,
        ResolveValueInput<Scope, Value>
      >("max", [this.toExpr(value)], this.resolveValueClickHouseType(value)),
    uniq: (value: ValueInput<Scope>) =>
      this.callFunction<string, string | number | bigint>("uniq", [this.toExpr(value)], "UInt64"),
    uniqExact: (value: ValueInput<Scope>) =>
      this.callFunction<string, string | number | bigint>("uniqExact", [this.toExpr(value)], "UInt64"),
    uniqIf: (value: ValueInput<Scope>, condition: Expression<unknown>) =>
      this.callFunction<string, string | number | bigint>(
        "uniqIf",
        [this.toExpr(value), condition.node],
        "UInt64",
      ),
    groupArray: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<NonNullValue<ResolveValueInput<Scope, Value>>[]>(
        "groupArray",
        [this.toExpr(value)],
        this.resolveArrayClickHouseType(value),
      ),
    any: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<
        ResolveValueInput<Scope, Value>,
        ResolveValueInput<Scope, Value>
      >("any", [this.toExpr(value)], this.resolveValueClickHouseType(value)),
    anyLast: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<
        ResolveValueInput<Scope, Value>,
        ResolveValueInput<Scope, Value>
      >("anyLast", [this.toExpr(value)], this.resolveValueClickHouseType(value)),
    toInt32: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toInt32", [this.toExpr(value)], "Int32"),
    toInt64: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string, string | number | bigint>("toInt64", [this.toExpr(value)], "Int64"),
    toUInt32: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toUInt32", [this.toExpr(value)], "UInt32"),
    toUInt64: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string, string | number | bigint>("toUInt64", [this.toExpr(value)], "UInt64"),
    toFloat32: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toFloat32", [this.toExpr(value)], "Float32"),
    toFloat64: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toFloat64", [this.toExpr(value)], "Float64"),
    toDate: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string, string | Date>("toDate", [this.toExpr(value)], "Date"),
    toDateTime: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string, string | Date>("toDateTime", [this.toExpr(value)], "DateTime"),
    toDateTime64: (value: ColumnRef<Scope> | Expression<unknown>, precision: number) =>
      this.callFunction<string, string | Date>(
        "toDateTime64",
        [this.toExpr(value), this.toIntegerLiteral(precision, "DateTime64 precision", 9)],
        `DateTime64(${precision})`,
      ),
    toStartOfMonth: (value: ExpressionInput<Scope>) =>
      this.callFunction<string>("toStartOfMonth", [this.toExpr(value)]),
    toStartOfWeek: (value: ExpressionInput<Scope>) =>
      this.callFunction<string>("toStartOfWeek", [this.toExpr(value)]),
    toStartOfDay: (value: ExpressionInput<Scope>) =>
      this.callFunction<string>("toStartOfDay", [this.toExpr(value)]),
    toStartOfYear: (value: ExpressionInput<Scope>) =>
      this.callFunction<string>("toStartOfYear", [this.toExpr(value)]),
    formatDateTime: (value: ExpressionInput<Scope>, format: string) =>
      this.callFunction<string>("formatDateTime", [
        this.toExpr(value),
        this.toStringLiteral(format),
      ], "String"),
    dateDiff: (
      unit: DateTimeUnitInput,
      start: ExpressionInput<Scope>,
      end: ExpressionInput<Scope>,
    ) =>
      this.callFunction<string>("dateDiff", [
        this.toDateTimeUnitLiteral(unit),
        this.toExpr(start),
        this.toExpr(end),
      ], "Int64"),
    dateAdd: (unit: DateTimeUnitInput, amount: NumericValueInput, value: ExpressionInput<Scope>) =>
      this.callFunction<string>(
        this.toDateTimeUnit(unit).add,
        [this.toExpr(value), this.toValueExpr(amount)],
        this.resolveExpressionClickHouseType(value),
      ),
    dateSub: (unit: DateTimeUnitInput, amount: NumericValueInput, value: ExpressionInput<Scope>) =>
      this.callFunction<string>(
        this.toDateTimeUnit(unit).subtract,
        [this.toExpr(value), this.toValueExpr(amount)],
        this.resolveExpressionClickHouseType(value),
      ),
    toYYYYMM: (value: ExpressionInput<Scope>) =>
      this.callFunction<number>("toYYYYMM", [this.toExpr(value)], "UInt32"),
    toYYYYMMDD: (value: ExpressionInput<Scope>) =>
      this.callFunction<number>("toYYYYMMDD", [this.toExpr(value)], "UInt32"),
    toString: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string>("toString", [this.toExpr(value)], "String"),
    toDecimal64: (value: ColumnRef<Scope> | Expression<unknown>, scale: number) =>
      this.callFunction<number>(
        "toDecimal64",
        [this.toExpr(value), this.toIntegerLiteral(scale, "Decimal64 scale", 18)],
        `Decimal64(${scale})`,
      ),
    toDecimal128: (value: ColumnRef<Scope> | Expression<unknown>, scale: number) =>
      this.callFunction<number>(
        "toDecimal128",
        [this.toExpr(value), this.toIntegerLiteral(scale, "Decimal128 scale", 38)],
        `Decimal128(${scale})`,
      ),
    has: (array: ArrayInput<Scope>, element: ParamLike<unknown> | Expression<unknown>) =>
      this.callFunction<number>("has", [this.toExpr(array), this.toValueExpr(element)], "UInt8"),
    hasAny: (
      array: ArrayInput<Scope>,
      elements: ParamLike<readonly unknown[]> | Expression<readonly unknown[]>,
    ) => this.callFunction<number>("hasAny", [this.toExpr(array), this.toValueExpr(elements)], "UInt8"),
    hasAll: (
      array: ArrayInput<Scope>,
      elements: ParamLike<readonly unknown[]> | Expression<readonly unknown[]>,
    ) => this.callFunction<number>("hasAll", [this.toExpr(array), this.toValueExpr(elements)], "UInt8"),
    length: (array: ArrayInput<Scope>) => this.callFunction<string>("length", [this.toExpr(array)], "UInt64"),
    isNull: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<number>("isNull", [this.toExpr(value)], "UInt8"),
    isNotNull: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<number>("isNotNull", [this.toExpr(value)], "UInt8"),
    nullIf: <Value extends ValueInput<Scope>>(
      value: Value,
      nullValue: ComparableValueInput<ResolveValueInput<Scope, Value>>,
    ) =>
      this.callFunction<ResolveValueInput<Scope, Value> | null>(
        "nullIf",
        [this.toExpr(value), this.toValueExpr(nullValue)],
        this.resolveNullableClickHouseType(value),
      ),
    coalesce: <
      Values extends readonly [ValueInput<Scope>, ValueInput<Scope>, ...ValueInput<Scope>[]],
    >(
      ...values: Values
    ) =>
      this.callFunction<CoalesceResult<ResolveValueInputs<Scope, Values>>>(
        "coalesce",
        values.map((value) => this.toExpr(value)),
      ),
    ifNull: <Value extends ValueInput<Scope>>(
      value: Value,
      defaultValue: FallbackValueInput<ResolveValueInput<Scope, Value>>,
    ) =>
      this.callFunction<NonNullValue<ResolveValueInput<Scope, Value>>>("ifNull", [
        this.toExpr(value),
        this.toValueExpr(defaultValue),
      ]),
    empty: <Value extends EmptyableInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, number>>(
        "empty",
        [this.toExpr(value)],
        this.resolveMaybeNullableUInt8Type(value),
      ),
    notEmpty: <Value extends EmptyableInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, number>>(
        "notEmpty",
        [this.toExpr(value)],
        this.resolveMaybeNullableUInt8Type(value),
      ),
    like: <Value extends StringInput<Scope>, Pattern extends StringValueInput>(
      value: Value,
      pattern: Pattern,
    ) =>
      this.callFunction<
        MaybeNullable<
          ResolveRefOrExpressionInput<Scope, Value> | ResolveStringValueInput<Pattern>,
          number
        >
      >("like", [this.toExpr(value), this.toValueExpr(pattern)], this.resolveMaybeNullableUInt8Type(value)),
    ilike: <Value extends StringInput<Scope>, Pattern extends StringValueInput>(
      value: Value,
      pattern: Pattern,
    ) =>
      this.callFunction<
        MaybeNullable<
          ResolveRefOrExpressionInput<Scope, Value> | ResolveStringValueInput<Pattern>,
          number
        >
      >("ilike", [this.toExpr(value), this.toValueExpr(pattern)], this.resolveMaybeNullableUInt8Type(value)),
    concat: <Parts extends readonly [StringValueInput, StringValueInput, ...StringValueInput[]]>(
      ...parts: Parts
    ) =>
      this.callFunction<MaybeNullableFromStringValues<Parts, string>>(
        "concat",
        parts.map((part) => this.toValueExpr(part)),
        "String",
      ),
    lower: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "lower",
        [this.toExpr(value)],
        this.resolveMaybeNullableStringType(value),
      ),
    upper: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "upper",
        [this.toExpr(value)],
        this.resolveMaybeNullableStringType(value),
      ),
    substring: <Value extends StringInput<Scope>>(
      value: Value,
      offset: NumericValueInput,
      length: NumericValueInput,
    ) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "substring",
        [this.toExpr(value), this.toValueExpr(offset), this.toValueExpr(length)],
        this.resolveMaybeNullableStringType(value),
      ),
    trimBoth: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "trimBoth",
        [this.toExpr(value)],
        this.resolveMaybeNullableStringType(value),
      ),
    trimLeft: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "trimLeft",
        [this.toExpr(value)],
        this.resolveMaybeNullableStringType(value),
      ),
    trimRight: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "trimRight",
        [this.toExpr(value)],
        this.resolveMaybeNullableStringType(value),
      ),
  };

  cmpRef<Left extends ColumnRef<Scope>, Right extends ColumnRef<Scope>>(
    left: Left,
    operator: RefPredicateOperator,
    right: Right,
  ): Expression<unknown> {
    return new Expression({
      kind: "binary",
      left: { kind: "ref", name: left },
      op: operator,
      right: { kind: "ref", name: right },
    });
  }

  and(expressions: readonly Expression<unknown>[]): Expression<unknown> {
    return new Expression({
      kind: "logical",
      op: "AND",
      conditions: expressions.map((expression) => expression.node),
    });
  }

  or(expressions: readonly Expression<unknown>[]): Expression<unknown> {
    return new Expression({
      kind: "logical",
      op: "OR",
      conditions: expressions.map((expression) => expression.node),
    });
  }

  private callFunction<T, Where = T>(
    name: string,
    args: readonly ExprNode[] = [],
    clickhouseType?: string,
  ): Expression<T, Where> {
    return new Expression({ kind: "function", name, args: [...args] }, clickhouseType);
  }

  private toIntegerLiteral(value: number, name: string, max: number): ExprNode {
    if (!Number.isSafeInteger(value) || value < 0 || value > max) {
      throw new Error(`${name} must be an integer between 0 and ${max}`);
    }

    return {
      kind: "raw",
      sql: String(value),
    };
  }

  private toStringLiteral(value: string): ExprNode {
    return {
      kind: "raw",
      sql: `'${escapeSingleQuotedString(value)}'`,
    };
  }

  private toDateTimeUnit(unit: DateTimeUnitInput): (typeof DATE_TIME_UNITS)[DateTimeUnit] {
    const normalizedUnit = unit.toUpperCase() as DateTimeUnit;
    const resolvedUnit = DATE_TIME_UNITS[normalizedUnit];

    if (!resolvedUnit) {
      throw new Error(`Unsupported date/time unit: ${unit}`);
    }

    return resolvedUnit;
  }

  private toDateTimeUnitLiteral(unit: DateTimeUnitInput): ExprNode {
    return this.toStringLiteral(this.toDateTimeUnit(unit).literal);
  }

  private toExpr(value: ExpressionInput<Scope>): ExprNode {
    if (value instanceof Expression) {
      return value.node;
    }

    return {
      kind: "ref",
      name: value,
    };
  }

  private toValueExpr<T>(value: ParamLike<T> | Expression<T>): ExprNode {
    if (value instanceof Expression) {
      return value.node;
    }

    return createValueNode(value);
  }

  private toPredicateRightExpr(value: unknown): ExprNode {
    if (isQueryLike(value)) {
      return toSubqueryExpr(value);
    }

    return this.toValueExpr(value as ParamLike<unknown> | Expression<unknown>);
  }

  private resolveClickHouseType(ref: ColumnRef<Scope>): string | undefined {
    if (!this.scopeColumns) {
      return undefined;
    }

    if (ref.includes(".")) {
      const [alias, column] = ref.split(".");
      return alias && column ? this.scopeColumns[alias]?.[column]?.clickhouseType : undefined;
    }

    const aliases = Object.keys(this.scopeColumns);
    if (aliases.length !== 1) {
      return undefined;
    }

    return this.scopeColumns[aliases[0]]?.[ref]?.clickhouseType;
  }

  private resolveValueClickHouseType(value: ValueInput<Scope>): string | undefined {
    if (value instanceof Expression) {
      return value.clickhouseType;
    }

    return this.resolveClickHouseType(value);
  }

  private resolveExpressionClickHouseType(value: ExpressionInput<Scope>): string | undefined {
    if (value instanceof Expression) {
      return value.clickhouseType;
    }

    return this.resolveClickHouseType(value);
  }

  private resolveArrayClickHouseType(value: ValueInput<Scope>): string | undefined {
    const innerType = this.resolveValueClickHouseType(value);
    return innerType ? `Array(${innerType})` : undefined;
  }

  private resolveNullableClickHouseType(value: ValueInput<Scope>): string | undefined {
    const clickhouseType = this.resolveValueClickHouseType(value);
    if (!clickhouseType) {
      return undefined;
    }

    return clickhouseType.startsWith("Nullable(") ? clickhouseType : `Nullable(${clickhouseType})`;
  }

  private resolveMaybeNullableStringType(value: ExpressionInput<Scope>): string {
    const clickhouseType = this.resolveExpressionClickHouseType(value);
    return clickhouseType?.startsWith("Nullable(") ? "Nullable(String)" : "String";
  }

  private resolveMaybeNullableUInt8Type(value: ExpressionInput<Scope>): string {
    const clickhouseType = this.resolveExpressionClickHouseType(value);
    return clickhouseType?.startsWith("Nullable(") ? "Nullable(UInt8)" : "UInt8";
  }
}
