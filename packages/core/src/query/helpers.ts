import type {
  ExprNode,
  SelectQueryNode,
  SourceNode,
  SubqueryExprNode,
  TableNode,
  ValueNode,
} from "../ast/query";
import type { QueryColumnMap } from "../column-metadata";
import { isClickHouseParam } from "../param";
import type { DatabaseSchema } from "../type-utils";
import type { SourceExpression } from "./types";

type TableSourceLike = {
  toSourceNode(): TableNode;
};

type AliasedQueryLike = {
  alias: string;
  toAST(): SelectQueryNode;
  getOutputColumns?(): QueryColumnMap | undefined;
};

export interface ResolvedSourceColumns {
  readonly alias: string;
  readonly columns: QueryColumnMap;
}

export function parseTableExpression(expression: string): TableNode {
  const match = expression.match(/^(.*?)\s+as\s+(.*?)$/i);

  if (!match) {
    return { kind: "table", name: expression.trim() };
  }

  return {
    kind: "table",
    name: match[1].trim(),
    alias: match[2].trim(),
  };
}

function isTableSourceLike(value: unknown): value is TableSourceLike {
  return (
    typeof value === "object" &&
    value !== null &&
    "toSourceNode" in value &&
    typeof value.toSourceNode === "function"
  );
}

function isAliasedQueryLike(value: unknown): value is AliasedQueryLike {
  return (
    typeof value === "object" &&
    value !== null &&
    "alias" in value &&
    typeof value.alias === "string" &&
    "toAST" in value &&
    typeof value.toAST === "function"
  );
}

export function isQueryLike(value: unknown): value is { toAST(): SelectQueryNode } {
  return (
    typeof value === "object" &&
    value !== null &&
    "toAST" in value &&
    typeof value.toAST === "function"
  );
}

export function parseSourceExpression<DB extends DatabaseSchema>(
  source: SourceExpression<DB>,
): SourceNode {
  if (typeof source === "string") {
    return parseTableExpression(source);
  }

  if (isTableSourceLike(source)) {
    return source.toSourceNode();
  }

  if (isAliasedQueryLike(source)) {
    return {
      kind: "subquery",
      query: source.toAST(),
      alias: source.alias,
    };
  }

  throw new Error("Unsupported source expression");
}

export function resolveSourceColumns<DB extends DatabaseSchema>(
  source: SourceExpression<DB>,
): ResolvedSourceColumns | undefined {
  if (isAliasedQueryLike(source)) {
    const columns = source.getOutputColumns?.();
    return columns ? { alias: source.alias, columns } : undefined;
  }

  return undefined;
}

export function toSubqueryExpr(query: { toAST(): SelectQueryNode }): SubqueryExprNode {
  return {
    kind: "subqueryExpr",
    query: query.toAST(),
  };
}

export function createValueNode(value: unknown, clickhouseType?: string): ValueNode {
  if (value === null) {
    throw new Error(
      'Bare null predicate values are not supported. Use whereNull()/whereNotNull() or param(null, "Nullable(...)").',
    );
  }

  if (isClickHouseParam(value)) {
    return {
      kind: "value",
      value: value.value,
      clickhouseType: value.clickhouseType,
    };
  }

  return {
    kind: "value",
    value,
    clickhouseType,
  };
}

export function appendCondition(existing: ExprNode | undefined, next: ExprNode): ExprNode {
  if (!existing) {
    return next;
  }

  if (existing.kind === "logical" && existing.op === "AND") {
    return {
      kind: "logical",
      op: "AND",
      conditions: [...existing.conditions, next],
    };
  }

  return {
    kind: "logical",
    op: "AND",
    conditions: [existing, next],
  };
}
