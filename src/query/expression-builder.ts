import type { ExprNode } from "../ast/query";
import type { ScopeMap } from "../type-utils";
import { escapeSingleQuotedString } from "../utils/string";
import { createValueNode } from "./helpers";
import type {
  ArrayColumnRef,
  ColumnRef,
  ParamLike,
  RefPredicateOperator,
  ResolveArrayElementType,
  ResolveColumnType,
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

interface ExpressionBuilderFunctions<Scope extends ScopeMap> {
  count(): Expression<string>;
  jsonExtractString(
    column: ColumnRef<Scope> | Expression<unknown>,
    key: string,
  ): Expression<string>;
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
  empty<Ref extends ArrayColumnRef<Scope>>(array: Ref): Expression<number>;
  empty<Element>(array: Expression<readonly Element[]>): Expression<number>;
  notEmpty<Ref extends ArrayColumnRef<Scope>>(array: Ref): Expression<number>;
  notEmpty<Element>(array: Expression<readonly Element[]>): Expression<number>;
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

  readonly fn: ExpressionBuilderFunctions<Scope> = {
    count: () => this.callFunction<string>("count"),
    jsonExtractString: (column: ColumnRef<Scope> | Expression<unknown>, key: string) =>
      this.callFunction<string>("JSONExtractString", [
        this.toExpr(column),
        { kind: "raw", sql: `'${escapeSingleQuotedString(key)}'` },
      ]),
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
    empty: (array: ArrayInput<Scope>) => this.callFunction<number>("empty", [this.toExpr(array)]),
    notEmpty: (array: ArrayInput<Scope>) =>
      this.callFunction<number>("notEmpty", [this.toExpr(array)]),
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
}
