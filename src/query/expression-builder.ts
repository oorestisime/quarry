import type { ExprNode } from "../ast/query";
import type { ClickHouseParam } from "../param";
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
  StringColumnRef,
} from "./types";

export class Expression<T> {
  constructor(readonly node: ExprNode) {}

  as<Alias extends string>(alias: Alias): AliasedExpression<T, Alias> {
    return new AliasedExpression(this.node, alias);
  }
}

export class AliasedExpression<_Value, Alias extends string> {
  constructor(
    readonly node: ExprNode,
    readonly alias: Alias,
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

interface ExpressionBuilderFunctions<Scope extends ScopeMap> {
  count(): Expression<string>;
  countIf(condition: Expression<unknown>): Expression<string>;
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
  ref<Ref extends ColumnRef<Scope>>(ref: Ref): Expression<ResolveColumnType<Scope, Ref>> {
    return new Expression({ kind: "ref", name: ref });
  }

  val<T>(value: ParamLike<T>): Expression<T> {
    return new Expression(createValueNode(value));
  }

  raw<T = unknown>(sql: string): Expression<T> {
    return new Expression({ kind: "raw", sql });
  }

  cmp<Ref extends ColumnRef<Scope>, Operator extends PredicateOperator>(
    left: Ref,
    operator: Operator,
    right:
      | ExpressionPredicateValue<ResolveColumnType<Scope, Ref>, Operator>
      | Expression<ResolveColumnType<Scope, Ref>>
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
    count: () => this.callFunction<string>("count"),
    countIf: (condition: Expression<unknown>) =>
      this.callFunction<string>("countIf", [condition.node]),
    jsonExtractString: (column: ColumnRef<Scope> | Expression<unknown>, key: string) =>
      this.callFunction<string>("JSONExtractString", [
        this.toExpr(column),
        { kind: "raw", sql: `'${escapeSingleQuotedString(key)}'` },
      ]),
    sum: (value: ValueInput<Scope>) =>
      this.callFunction<number | string>("sum", [this.toExpr(value)]),
    sumIf: (value: ValueInput<Scope>, condition: Expression<unknown>) =>
      this.callFunction<number | string>("sumIf", [this.toExpr(value), condition.node]),
    avg: (value: ValueInput<Scope>) => this.callFunction<number>("avg", [this.toExpr(value)]),
    avgIf: (value: ValueInput<Scope>, condition: Expression<unknown>) =>
      this.callFunction<number>("avgIf", [this.toExpr(value), condition.node]),
    min: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<ResolveValueInput<Scope, Value>>("min", [this.toExpr(value)]),
    max: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<ResolveValueInput<Scope, Value>>("max", [this.toExpr(value)]),
    uniq: (value: ValueInput<Scope>) => this.callFunction<string>("uniq", [this.toExpr(value)]),
    uniqExact: (value: ValueInput<Scope>) =>
      this.callFunction<string>("uniqExact", [this.toExpr(value)]),
    uniqIf: (value: ValueInput<Scope>, condition: Expression<unknown>) =>
      this.callFunction<string>("uniqIf", [this.toExpr(value), condition.node]),
    groupArray: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<NonNullValue<ResolveValueInput<Scope, Value>>[]>("groupArray", [
        this.toExpr(value),
      ]),
    any: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<ResolveValueInput<Scope, Value>>("any", [this.toExpr(value)]),
    anyLast: <Value extends ValueInput<Scope>>(value: Value) =>
      this.callFunction<ResolveValueInput<Scope, Value>>("anyLast", [this.toExpr(value)]),
    toInt32: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toInt32", [this.toExpr(value)]),
    toInt64: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string>("toInt64", [this.toExpr(value)]),
    toUInt32: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toUInt32", [this.toExpr(value)]),
    toUInt64: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string>("toUInt64", [this.toExpr(value)]),
    toFloat32: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toFloat32", [this.toExpr(value)]),
    toFloat64: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<number>("toFloat64", [this.toExpr(value)]),
    toDate: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string>("toDate", [this.toExpr(value)]),
    toDateTime: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string>("toDateTime", [this.toExpr(value)]),
    toDateTime64: (value: ColumnRef<Scope> | Expression<unknown>, precision: number) =>
      this.callFunction<string>("toDateTime64", [
        this.toExpr(value),
        this.toIntegerLiteral(precision, "DateTime64 precision", 9),
      ]),
    toString: (value: ColumnRef<Scope> | Expression<unknown>) =>
      this.callFunction<string>("toString", [this.toExpr(value)]),
    toDecimal64: (value: ColumnRef<Scope> | Expression<unknown>, scale: number) =>
      this.callFunction<number>("toDecimal64", [
        this.toExpr(value),
        this.toIntegerLiteral(scale, "Decimal64 scale", 18),
      ]),
    toDecimal128: (value: ColumnRef<Scope> | Expression<unknown>, scale: number) =>
      this.callFunction<number>("toDecimal128", [
        this.toExpr(value),
        this.toIntegerLiteral(scale, "Decimal128 scale", 38),
      ]),
    has: (array: ArrayInput<Scope>, element: ParamLike<unknown> | Expression<unknown>) =>
      this.callFunction<number>("has", [this.toExpr(array), this.toValueExpr(element)]),
    hasAny: (
      array: ArrayInput<Scope>,
      elements: ParamLike<readonly unknown[]> | Expression<readonly unknown[]>,
    ) => this.callFunction<number>("hasAny", [this.toExpr(array), this.toValueExpr(elements)]),
    hasAll: (
      array: ArrayInput<Scope>,
      elements: ParamLike<readonly unknown[]> | Expression<readonly unknown[]>,
    ) => this.callFunction<number>("hasAll", [this.toExpr(array), this.toValueExpr(elements)]),
    length: (array: ArrayInput<Scope>) => this.callFunction<string>("length", [this.toExpr(array)]),
    empty: <Value extends EmptyableInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, number>>("empty", [
        this.toExpr(value),
      ]),
    notEmpty: <Value extends EmptyableInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, number>>(
        "notEmpty",
        [this.toExpr(value)],
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
      >("like", [this.toExpr(value), this.toValueExpr(pattern)]),
    ilike: <Value extends StringInput<Scope>, Pattern extends StringValueInput>(
      value: Value,
      pattern: Pattern,
    ) =>
      this.callFunction<
        MaybeNullable<
          ResolveRefOrExpressionInput<Scope, Value> | ResolveStringValueInput<Pattern>,
          number
        >
      >("ilike", [this.toExpr(value), this.toValueExpr(pattern)]),
    concat: <Parts extends readonly [StringValueInput, StringValueInput, ...StringValueInput[]]>(
      ...parts: Parts
    ) =>
      this.callFunction<MaybeNullableFromStringValues<Parts, string>>(
        "concat",
        parts.map((part) => this.toValueExpr(part)),
      ),
    lower: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>("lower", [
        this.toExpr(value),
      ]),
    upper: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>("upper", [
        this.toExpr(value),
      ]),
    substring: <Value extends StringInput<Scope>>(
      value: Value,
      offset: NumericValueInput,
      length: NumericValueInput,
    ) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "substring",
        [this.toExpr(value), this.toValueExpr(offset), this.toValueExpr(length)],
      ),
    trimBoth: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "trimBoth",
        [this.toExpr(value)],
      ),
    trimLeft: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "trimLeft",
        [this.toExpr(value)],
      ),
    trimRight: <Value extends StringInput<Scope>>(value: Value) =>
      this.callFunction<MaybeNullable<ResolveRefOrExpressionInput<Scope, Value>, string>>(
        "trimRight",
        [this.toExpr(value)],
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

  private callFunction<T>(name: string, args: readonly ExprNode[] = []): Expression<T> {
    return new Expression({ kind: "function", name, args: [...args] });
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

  private toExpr(value: ColumnRef<Scope> | Expression<unknown>): ExprNode {
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
}
