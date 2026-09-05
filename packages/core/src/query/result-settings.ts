import type { ExprNode, SelectQueryNode } from "../ast/query";
import type { ClickHouseSettings } from "../client";

export const resultFormatSettings = {
  output_format_json_quote_64bit_integers: 1,
  output_format_json_quote_64bit_floats: 0,
  output_format_json_quote_decimals: 0,
  output_format_json_named_tuples_as_objects: 1,
} as const;

/** Query/HTTP settings must agree with the result types, including nested queries. */
export function resultSettings(
  query: SelectQueryNode,
  options: ClickHouseSettings = {},
): ClickHouseSettings {
  const queries: SelectQueryNode[] = [];
  const modes = new Set<boolean>();
  function expression(node: ExprNode): void {
    switch (node.kind) {
      case "subqueryExpr":
        visit(node.query);
        break;
      case "function":
        node.args.forEach(expression);
        node.parameters?.forEach(expression);
        break;
      case "fragment":
        node.values.forEach(expression);
        break;
      case "window":
        expression(node.expression);
        node.partitionBy.forEach(expression);
        node.orderBy.forEach((order) => expression(order.expr));
        break;
      case "binary":
        expression(node.left);
        expression(node.right);
        break;
      case "logical":
        node.conditions.forEach(expression);
        break;
    }
  }
  function visit(node: SelectQueryNode): void {
    queries.push(node);
    node.unionAll?.forEach(visit);
    node.with.forEach((cte) => visit(cte.query));
    if (node.from?.kind === "subquery") visit(node.from.query);
    node.joins.forEach((join) => {
      if (join.joinType !== "INNER") modes.add(join.nullable ?? false);
      if (join.source.kind === "subquery") visit(join.source.query);
      expression(join.on);
    });
    node.selections.forEach((selection) => expression(selection.expr));
    node.arrayJoins.forEach((join) => expression(join.expr));
    [node.where, node.prewhere, node.having].forEach((expr) => {
      if (expr) expression(expr);
    });
    node.groupBy.forEach(expression);
    node.distinctOn.forEach(expression);
    node.orderBy.forEach((order) => expression(order.expr));
    node.limitBy?.expressions.forEach(expression);
  }
  visit(query);
  if (modes.size > 1)
    throw new Error(
      "Use one outer-join null policy throughout a query, including subqueries and UNION ALL branches.",
    );
  const required = { ...resultFormatSettings, join_use_nulls: modes.has(true) ? 1 : 0 };
  for (const settings of [...queries.map((node) => node.settings), options]) {
    for (const [name, expected] of Object.entries(required)) {
      if (settings[name] !== undefined && Number(settings[name]) !== expected) {
        throw new Error(
          `${name} must be ${expected} to match Quarry's result types.${name === "join_use_nulls" ? " Use leftJoinNullable() for nullable results." : ""}`,
        );
      }
    }
  }
  return { ...options, ...required };
}
