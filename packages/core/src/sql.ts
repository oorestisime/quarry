import type { ExprNode } from "./ast/query";
import { isClickHouseParam } from "./param";
import { Expression } from "./query/expression-builder";

/** SQL text is static; interpolations are bound values or composable expressions. */
export function sql<T = unknown>(
  strings: TemplateStringsArray,
  ...values: readonly unknown[]
): Expression<T> {
  if (!Array.isArray(strings.raw) || strings.length !== values.length + 1) {
    throw new Error("Use sql as a tagged template literal.");
  }
  return new Expression({
    kind: "fragment",
    strings: [...strings],
    values: values.map(toNode),
  });
}

function toNode(value: unknown): ExprNode {
  if (value instanceof Expression) return value.node;
  if (isClickHouseParam(value)) {
    return { kind: "value", value: value.value, clickhouseType: value.clickhouseType };
  }
  if (value === null || value === undefined) {
    throw new Error(
      'Bind null explicitly with param(null, "Nullable(...)"); undefined cannot be bound.',
    );
  }
  return { kind: "value", value };
}

/** Each argument is one literal identifier segment, including any embedded dots. */
export function identifier(...parts: readonly [string, ...string[]]): Expression<unknown> {
  return new Expression({ kind: "identifier", parts: [...parts] });
}
