import type { SelectQueryNode } from "../ast/query";

/** Table stars have unknown arity without runtime schema metadata. */
export function selectionCount(query: SelectQueryNode): number | undefined {
  if (query.unionAll) return selectionCount(query.unionAll[0]);
  let count = 0;
  for (const selection of query.selections) {
    if (selection.expr.kind === "raw" && /(^|\.)\*$/.test(selection.expr.sql)) {
      if (query.from?.kind !== "subquery" || query.joins.length) return undefined;
      const nested = selectionCount(query.from.query);
      if (nested === undefined) return undefined;
      count += nested;
    } else {
      count++;
    }
  }
  return count;
}
