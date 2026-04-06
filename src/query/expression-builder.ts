import type { ExprNode } from "../ast/query";
import type { ScopeMap } from "../type-utils";
import { escapeSingleQuotedString } from "../utils/string";
import { createValueNode } from "./helpers";
import type { ColumnRef, ParamLike, RefPredicateOperator, ResolveColumnType } from "./types";

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

  readonly fn = {
    count: () => new Expression<string>({ kind: "function", name: "count", args: [] }),
    jsonExtractString: (column: ColumnRef<Scope> | Expression<unknown>, key: string) =>
      new Expression<string>({
        kind: "function",
        name: "JSONExtractString",
        args: [this.toExpr(column), { kind: "raw", sql: `'${escapeSingleQuotedString(key)}'` }],
      }),
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

  private toExpr(value: ColumnRef<Scope> | Expression<unknown>): ExprNode {
    if (value instanceof Expression) {
      return value.node;
    }

    return {
      kind: "ref",
      name: value,
    };
  }
}
