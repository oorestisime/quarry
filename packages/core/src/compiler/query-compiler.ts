import type {
  ExprNode,
  InsertQueryNode,
  SelectQueryNode,
  SelectionNode,
  SourceNode,
} from "../ast/query";
import { normalizeClickHouseInputValue } from "../input-normalization";
import { escapeSingleQuotedString } from "../utils/string";

export interface CompiledQuery {
  query: string;
  params: Record<string, unknown>;
}

export interface CompiledInsertQuery<Row> {
  query: string;
  params: Record<string, unknown>;
  values?: Row[];
}

function inferClickHouseType(value: unknown): string {
  if (Array.isArray(value)) {
    const firstValue = value.find((item) => item !== null && item !== undefined);
    const memberType = firstValue === undefined ? "String" : inferClickHouseType(firstValue);
    return `Array(${memberType})`;
  }

  if (typeof value === "boolean") {
    return "Bool";
  }

  if (typeof value === "number") {
    return Number.isInteger(value) ? "Int64" : "Float64";
  }

  if (typeof value === "bigint") {
    return "Int64";
  }

  if (value instanceof Date) {
    return "DateTime";
  }

  return "String";
}

function compileSettingValue(value: string | number | boolean): string {
  if (typeof value === "string") {
    return `'${escapeSingleQuotedString(value)}'`;
  }

  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }

  return String(value);
}

class CompileContext {
  private paramIndex = 0;
  readonly params: Record<string, unknown> = {};

  bind(value: unknown, clickhouseType?: string): string {
    const name = `p${this.paramIndex++}`;
    const type = clickhouseType ?? inferClickHouseType(value);
    this.params[name] = normalizeClickHouseInputValue(value, type);
    return `{${name}:${type}}`;
  }
}

function compileExpr(expr: ExprNode, context: CompileContext): string {
  switch (expr.kind) {
    case "ref":
      return expr.name;
    case "value":
      return context.bind(expr.value, expr.clickhouseType);
    case "raw":
      return expr.sql;
    case "function":
      return `${expr.name}(${expr.args.map((arg) => compileExpr(arg, context)).join(", ")})`;
    case "subqueryExpr":
      return `(${compileQuerySql(expr.query, context)})`;
    case "binary":
      return `${compileExpr(expr.left, context)} ${expr.op} ${compileExpr(expr.right, context)}`;
    case "logical":
      return expr.conditions
        .map((condition) => {
          const compiled = compileExpr(condition, context);
          return condition.kind === "logical" ? `(${compiled})` : compiled;
        })
        .join(` ${expr.op} `);
  }
}

function compileSource(source: SourceNode, context: CompileContext): string {
  if (source.kind === "table") {
    const alias = source.alias ? ` AS ${source.alias}` : "";
    const final = source.final ? " FINAL" : "";
    return `${source.name}${alias}${final}`;
  }

  return `(${compileQuerySql(source.query, context)}) AS ${source.alias}`;
}

function compileSelection(selection: SelectionNode, context: CompileContext): string {
  const compiled = compileExpr(selection.expr, context);
  return selection.alias ? `${compiled} AS ${selection.alias}` : compiled;
}

function compileQuerySql(node: SelectQueryNode, context: CompileContext): string {
  if (!node.from) {
    throw new Error("Cannot compile a query without a FROM clause");
  }

  if (node.selections.length === 0) {
    throw new Error("Cannot compile a query without any selections");
  }

  const parts = [
    ...(node.with.length > 0
      ? [
          `WITH ${node.with
            .map((cte) => `${cte.name} AS (${compileQuerySql(cte.query, context)})`)
            .join(", ")}`,
        ]
      : []),
    `${
      node.distinctOn.length > 0
        ? `SELECT DISTINCT ON (${node.distinctOn.map((expression) => compileExpr(expression, context)).join(", ")})`
        : node.distinct
          ? "SELECT DISTINCT"
          : "SELECT"
    } ${node.selections.map((selection) => compileSelection(selection, context)).join(", ")}`,
    `FROM ${compileSource(node.from, context)}`,
  ];

  for (const join of node.joins) {
    parts.push(
      `${join.joinType} JOIN ${compileSource(join.source, context)} ON ${compileExpr(join.on, context)}`,
    );
  }

  if (node.prewhere) {
    parts.push(`PREWHERE ${compileExpr(node.prewhere, context)}`);
  }

  if (node.where) {
    parts.push(`WHERE ${compileExpr(node.where, context)}`);
  }

  if (node.groupBy.length > 0) {
    parts.push(`GROUP BY ${node.groupBy.map((expr) => compileExpr(expr, context)).join(", ")}`);
  }

  if (node.having) {
    parts.push(`HAVING ${compileExpr(node.having, context)}`);
  }

  if (node.orderBy.length > 0) {
    parts.push(
      `ORDER BY ${node.orderBy.map((item) => `${compileExpr(item.expr, context)} ${item.direction}`).join(", ")}`,
    );
  }

  if (node.limit !== undefined) {
    parts.push(`LIMIT ${node.limit}`);
  }

  if (node.offset !== undefined) {
    parts.push(`OFFSET ${node.offset}`);
  }

  const settingsEntries = Object.entries(node.settings);
  if (settingsEntries.length > 0) {
    parts.push(
      `SETTINGS ${settingsEntries.map(([key, value]) => `${key} = ${compileSettingValue(value)}`).join(", ")}`,
    );
  }

  return parts.join(" ");
}

export function compileSelectQuery(query: SelectQueryNode): CompiledQuery {
  const context = new CompileContext();

  return {
    query: compileQuerySql(query, context),
    params: context.params,
  };
}

export function compileInsertQuery<Row extends object>(
  query: InsertQueryNode,
): CompiledInsertQuery<Row> {
  if (!query.source) {
    throw new Error("Cannot compile an insert without a source");
  }

  const columns = query.columns?.length ? ` (${query.columns.join(", ")})` : "";

  if (query.source.kind === "values") {
    return {
      query: `INSERT INTO ${query.table}${columns} FORMAT JSONEachRow`,
      params: {},
      values: structuredClone(query.source.rows as Row[]),
    };
  }

  const compiledSelect = compileSelectQuery(query.source.query);

  return {
    query: `INSERT INTO ${query.table}${columns} ${compiledSelect.query}`,
    params: compiledSelect.params,
  };
}
