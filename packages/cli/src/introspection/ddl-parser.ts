import { ExpressionBuilder } from "@oorestisime/quarry";
import type {
  IntrospectionObjectDescriptor,
  ParsedColumn,
  ParsedExpressionArg,
  ParsedSchema,
  ParsedSelection,
  ParsedTable,
  ParsedView,
  QuarryColumnClause,
  QuarryColumnClauseKind,
  ParsedWhereCondition,
} from "./types";
import {
  findMatchingParen,
  findTopLevelKeyword,
  isIdentifierLike,
  normalizeWhitespace,
  splitStatements,
  splitTopLevel,
  splitTopLevelByKeyword,
  unquoteIdentifier,
} from "./syntax";

const SUPPORTED_TABLE_CLAUSES = ["PRIMARY KEY", "ORDER BY", "PARTITION BY", "TTL", "SETTINGS"];
const UNSUPPORTED_TABLE_CLAUSES = ["SAMPLE BY", "COMMENT"];
const ALL_TABLE_CLAUSES = [...SUPPORTED_TABLE_CLAUSES, ...UNSUPPORTED_TABLE_CLAUSES];
const TABLE_CLAUSE_PATTERN = new RegExp(
  `^(${ALL_TABLE_CLAUSES.map((clause) => clause.replace(/ /g, "\\s+")).join("|")})\\s+([\\s\\S]+?)(?=(?:${ALL_TABLE_CLAUSES.map((clause) => clause.replace(/ /g, "\\s+")).join("|")})\\s+|$)`,
  "i",
);
const SUPPORTED_VIEW_FUNCTION_LOOKUP = new Map(
  Object.keys(new ExpressionBuilder<any>().fn).map((functionName) => [
    functionName.toLowerCase(),
    functionName,
  ]),
);

function parseExpressionList(value: string | undefined): string[] | undefined {
  if (!value) {
    return undefined;
  }

  const trimmed = value.trim();
  const inner = trimmed.startsWith("(") && trimmed.endsWith(")") ? trimmed.slice(1, -1) : trimmed;
  const parts = splitTopLevel(inner, ",").map(normalizeWhitespace).filter(Boolean);
  return parts.length > 0 ? parts : undefined;
}

function parseSettingValue(value: string): string | number | boolean {
  const trimmed = value.trim();

  if (/^(?:true|false)$/i.test(trimmed)) {
    return trimmed.toLowerCase() === "true";
  }

  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
    return Number(trimmed);
  }

  if (
    (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    return trimmed.slice(1, -1);
  }

  return trimmed;
}

function parseSettings(
  value: string | undefined,
): Record<string, string | number | boolean> | undefined {
  if (!value) {
    return undefined;
  }

  const settings = Object.fromEntries(
    splitTopLevel(value, ",").map((entry) => {
      const [key, rawValue] = entry.split(/=(.+)/).map((part) => part.trim());
      if (!key || !rawValue) {
        throw new Error(`Unsupported SETTINGS entry '${entry}'.`);
      }

      return [key, parseSettingValue(rawValue)];
    }),
  );

  return Object.keys(settings).length > 0 ? settings : undefined;
}

function parseCodecs(value: string): string[] | undefined {
  const match = /\s+CODEC\((.+)\)\s*$/i.exec(value);
  if (!match) {
    return undefined;
  }

  return splitTopLevel(match[1]!, ",").map(normalizeWhitespace);
}

function stripCodecs(value: string): string {
  return value.replace(/\s+CODEC\((.+)\)\s*$/i, "").trim();
}

function parseColumnClause(definition: string): QuarryColumnClause | undefined {
  const clauseKinds: Array<{ keyword: string; kind: QuarryColumnClauseKind }> = [
    { keyword: "DEFAULT", kind: "default" },
    { keyword: "MATERIALIZED", kind: "materialized" },
    { keyword: "ALIAS", kind: "alias" },
  ];

  let bestMatch:
    | {
        index: number;
        kind: QuarryColumnClauseKind;
        keyword: string;
      }
    | undefined;

  for (const clause of clauseKinds) {
    const index = findTopLevelKeyword(definition, clause.keyword);
    if (index === -1) {
      continue;
    }

    if (!bestMatch || index < bestMatch.index) {
      bestMatch = {
        index,
        kind: clause.kind,
        keyword: clause.keyword,
      };
    }
  }

  if (!bestMatch) {
    return undefined;
  }

  const sql = definition.slice(bestMatch.index + bestMatch.keyword.length).trim();
  if (sql.length === 0) {
    throw new Error(`Unsupported column clause in '${definition}'.`);
  }

  return {
    kind: bestMatch.kind,
    sql,
  };
}

function stripColumnClause(definition: string): string {
  const indices = [
    findTopLevelKeyword(definition, "DEFAULT"),
    findTopLevelKeyword(definition, "MATERIALIZED"),
    findTopLevelKeyword(definition, "ALIAS"),
  ].filter((index) => index !== -1);

  if (indices.length === 0) {
    return definition.trim();
  }

  return definition.slice(0, Math.min(...indices)).trim();
}

function parseColumn(line: string): ParsedColumn {
  const match = /^(`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+([\s\S]+)$/.exec(line.trim());
  if (!match) {
    throw new Error(`Unsupported column definition '${line}'.`);
  }

  const codecs = parseCodecs(match[2]!);
  const definitionWithoutCodecs = stripCodecs(match[2]!);
  const clause = parseColumnClause(definitionWithoutCodecs);
  const clickhouseType = normalizeWhitespace(stripColumnClause(definitionWithoutCodecs));

  return {
    name: unquoteIdentifier(match[1]!),
    clickhouseType,
    ...(codecs ? { codecs } : {}),
    ...(clause ? { clause } : {}),
  };
}

function parseEngineOptions(
  tableName: string,
  engineName: string,
  engineArgs: string | undefined,
  trailingClauses: string,
) {
  const args = engineArgs ? splitTopLevel(engineArgs, ",").map((part) => part.trim()) : [];
  let primaryKey: string[] | undefined;
  let orderBy: string[] | undefined;
  let partitionBy: string[] | undefined;
  let ttl: string[] | undefined;
  let settings: Record<string, string | number | boolean> | undefined;

  let remaining = trailingClauses.trim();
  while (remaining.length > 0) {
    const clauseMatch = TABLE_CLAUSE_PATTERN.exec(remaining);
    if (!clauseMatch) {
      throw new Error(`Unsupported table clause in table '${tableName}': ${remaining}`);
    }

    const clauseName = clauseMatch[1]!.replace(/\s+/g, " ").toUpperCase();
    const clauseValue = clauseMatch[2]!.trim();

    if (UNSUPPORTED_TABLE_CLAUSES.includes(clauseName)) {
      throw new Error(`Unsupported table clause '${clauseName}' in table '${tableName}'.`);
    }

    if (clauseName === "PRIMARY KEY") {
      primaryKey = parseExpressionList(clauseValue);
    } else if (clauseName === "ORDER BY") {
      orderBy = parseExpressionList(clauseValue);
    } else if (clauseName === "PARTITION BY") {
      partitionBy = parseExpressionList(clauseValue);
    } else if (clauseName === "TTL") {
      ttl = parseExpressionList(clauseValue);
    } else if (clauseName === "SETTINGS") {
      settings = parseSettings(clauseValue);
    }

    remaining = remaining.slice(clauseMatch[0].length).trim();
  }

  const options: Record<string, unknown> = {
    ...(primaryKey ? { primaryKey } : {}),
    ...(orderBy ? { orderBy } : {}),
    ...(partitionBy ? { partitionBy } : {}),
    ...(ttl ? { ttl } : {}),
    ...(settings ? { settings } : {}),
  };

  if (engineName === "ReplacingMergeTree" && args[0] && isIdentifierLike(args[0])) {
    options.versionBy = unquoteIdentifier(args[0]);
  }

  if (engineName === "SharedReplacingMergeTree") {
    const identifierArgs = args.filter(isIdentifierLike).map(unquoteIdentifier);
    if (identifierArgs[0]) {
      options.versionBy = identifierArgs[0];
    }
    if (identifierArgs[1]) {
      options.isDeletedBy = identifierArgs[1];
    }
  }

  if (engineName === "SummingMergeTree" && args[0]) {
    const sumColumns = parseExpressionList(args.join(", "));
    if (sumColumns) {
      options.sumColumns = sumColumns.map(unquoteIdentifier);
    }
  }

  if (engineName === "SharedSummingMergeTree") {
    const summingArgs = args.filter(
      (arg) =>
        !((arg.startsWith("'") && arg.endsWith("'")) || (arg.startsWith('"') && arg.endsWith('"'))),
    );
    if (summingArgs.length > 0) {
      const sumColumns = parseExpressionList(summingArgs.join(", "));
      if (sumColumns) {
        options.sumColumns = sumColumns.map(unquoteIdentifier);
      }
    }
  }

  if (engineName === "CollapsingMergeTree" && args[0] && isIdentifierLike(args[0])) {
    options.signBy = unquoteIdentifier(args[0]);
  }

  if (engineName === "VersionedCollapsingMergeTree") {
    if (args[0] && isIdentifierLike(args[0])) {
      options.signBy = unquoteIdentifier(args[0]);
    }
    if (args[1] && isIdentifierLike(args[1])) {
      options.versionBy = unquoteIdentifier(args[1]);
    }
  }

  return options;
}

function parseCreateTable(statement: string): ParsedTable {
  const match = /^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)\s*/i.exec(statement);
  if (!match) {
    throw new Error(`Unsupported CREATE TABLE statement '${statement}'.`);
  }

  const name = unquoteIdentifier(match[1]!);
  const columnsStart = statement.indexOf("(", match.index + match[0]!.length - 1);
  const columnsEnd = findMatchingParen(statement, columnsStart);
  const columnsBlock = statement.slice(columnsStart + 1, columnsEnd);
  const trailing = statement.slice(columnsEnd + 1).trim();
  const engineMatch = /^ENGINE\s*=\s*([A-Za-z0-9]+)(?:\(([^)]*[\s\S]*?)\))?([\s\S]*)$/i.exec(
    trailing,
  );
  if (!engineMatch) {
    throw new Error(`Unsupported engine clause for table '${name}'.`);
  }

  const columns = splitTopLevel(columnsBlock, ",").map(parseColumn);
  return {
    name,
    columns,
    engineName: engineMatch[1]!,
    engineOptions: parseEngineOptions(name, engineMatch[1]!, engineMatch[2], engineMatch[3] ?? ""),
  };
}

function parseExpressionArg(value: string): ParsedExpressionArg {
  const trimmed = value.trim();

  if (/^(?:true|false)$/i.test(trimmed)) {
    return {
      kind: "boolean",
      value: trimmed.toLowerCase() === "true",
    };
  }

  if (
    (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    return {
      kind: "string",
      value: trimmed.slice(1, -1),
    };
  }

  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
    return {
      kind: "number",
      value: Number(trimmed),
    };
  }

  if (isIdentifierLike(trimmed)) {
    return {
      kind: "column",
      column: unquoteIdentifier(trimmed),
    };
  }

  throw new Error(`Unsupported expression argument '${value}'.`);
}

function resolveSupportedViewFunctionName(functionName: string, viewName: string): string {
  const resolved = SUPPORTED_VIEW_FUNCTION_LOOKUP.get(functionName.toLowerCase());
  if (!resolved) {
    throw new Error(`Unsupported view function '${functionName}' in view '${viewName}'.`);
  }

  return resolved;
}

function parseSelection(value: string, viewName: string): ParsedSelection {
  const columnMatch = /^(?:`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)$/.exec(value.trim());
  if (columnMatch) {
    return {
      kind: "column",
      column: unquoteIdentifier(columnMatch[0]),
    };
  }

  const functionMatch =
    /^([A-Za-z_][A-Za-z0-9_]*)\((.*)\)\s+AS\s+(`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)$/i.exec(
      value.trim(),
    );
  if (!functionMatch) {
    throw new Error(`Unsupported SELECT expression '${value}'.`);
  }

  const functionName = resolveSupportedViewFunctionName(functionMatch[1]!, viewName);

  return {
    kind: "function",
    functionName,
    alias: unquoteIdentifier(functionMatch[3]!),
    args:
      functionMatch[2]!.trim().length > 0
        ? splitTopLevel(functionMatch[2]!, ",").map(parseExpressionArg)
        : [],
  };
}

function parseWhereCondition(value: string): ParsedWhereCondition {
  const trimmed = value.trim();

  const nullMatch = /^(`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+IS\s+(NOT\s+)?NULL$/i.exec(
    trimmed,
  );
  if (nullMatch) {
    return {
      kind: "null",
      column: unquoteIdentifier(nullMatch[1]!),
      negated: Boolean(nullMatch[2]),
    };
  }

  const binaryMatch =
    /^(`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|<>|>=|<=|>|<)\s*([\s\S]+)$/i.exec(trimmed);
  if (!binaryMatch) {
    throw new Error(`Unsupported WHERE condition '${value}'.`);
  }

  return {
    kind: "binary",
    column: unquoteIdentifier(binaryMatch[1]!),
    operator: (binaryMatch[2] === "<>" ? "!=" : binaryMatch[2]) as
      | "="
      | "!="
      | ">"
      | ">="
      | "<"
      | "<=",
    value: parseExpressionArg(binaryMatch[3]!),
  };
}

function parseWhereClause(value: string | undefined): ParsedWhereCondition[] | undefined {
  if (!value) {
    return undefined;
  }

  const conditions = splitTopLevelByKeyword(value, "AND").map(parseWhereCondition);
  return conditions.length > 0 ? conditions : undefined;
}

function parseCreateView(statement: string): ParsedView {
  const headerMatch = /^CREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)\s*([\s\S]*)$/i.exec(
    statement,
  );
  if (!headerMatch) {
    throw new Error(`Unsupported CREATE VIEW statement '${statement}'.`);
  }

  const name = unquoteIdentifier(headerMatch[1]!);
  let remainder = headerMatch[2]!.trim();

  if (remainder.startsWith("(")) {
    const closing = findMatchingParen(remainder, 0);
    remainder = remainder.slice(closing + 1).trim();
  }

  if (!/^AS\s+SELECT\s+/i.test(remainder)) {
    throw new Error(`Unsupported CREATE VIEW statement '${statement}'.`);
  }

  remainder = remainder.replace(/^AS\s+SELECT\s+/i, "");
  const fromIndex = findTopLevelKeyword(remainder, "FROM");
  if (fromIndex === -1) {
    throw new Error(`Unsupported CREATE VIEW statement '${statement}'.`);
  }

  const selectClauseRaw = remainder.slice(0, fromIndex).trim();
  remainder = remainder.slice(fromIndex + 4).trim();

  const sourceMatch = /^([^\s]+)([\s\S]*)$/i.exec(remainder);
  if (!sourceMatch) {
    throw new Error(`Unsupported CREATE VIEW statement '${statement}'.`);
  }

  const sourceTable = unquoteIdentifier(sourceMatch[1]!);
  let tail = sourceMatch[2]!.trim();
  let isFinal = false;
  if (/^FINAL\b/i.test(tail)) {
    isFinal = true;
    tail = tail.replace(/^FINAL\b/i, "").trim();
  }

  let whereClause: string | undefined;
  if (/^WHERE\b/i.test(tail)) {
    tail = tail.replace(/^WHERE\b/i, "").trim();
    const groupByIndex = findTopLevelKeyword(tail, "GROUP BY");
    if (groupByIndex === -1) {
      whereClause = tail;
      tail = "";
    } else {
      whereClause = tail.slice(0, groupByIndex).trim();
      tail = tail.slice(groupByIndex).trim();
    }
  }

  let groupByClause: string | undefined;
  if (/^GROUP\s+BY\b/i.test(tail)) {
    groupByClause = tail.replace(/^GROUP\s+BY\b/i, "").trim();
    tail = "";
  }

  if (tail.length > 0) {
    throw new Error(`Unsupported CREATE VIEW statement '${statement}'.`);
  }

  const selectClause = normalizeWhitespace(selectClauseRaw);
  const groupBy = parseExpressionList(groupByClause);
  const where = parseWhereClause(whereClause);

  if (selectClause === "*") {
    return {
      kind: "selectAll",
      name,
      sourceTable,
      ...(isFinal ? { final: true } : {}),
      ...(where ? { where } : {}),
    };
  }

  return {
    kind: "selectExpr",
    name,
    sourceTable,
    selections: splitTopLevel(selectClauseRaw, ",").map((selection) =>
      parseSelection(selection, name),
    ),
    ...(groupBy ? { groupBy: groupBy.map(unquoteIdentifier) } : {}),
    ...(isFinal ? { final: true } : {}),
    ...(where ? { where } : {}),
  };
}

export function parseSchemaDDL(ddl: string): ParsedSchema {
  const tables: ParsedTable[] = [];
  const views: ParsedView[] = [];

  for (const statement of splitStatements(ddl)) {
    if (/^CREATE\s+TABLE\b/i.test(statement)) {
      tables.push(parseCreateTable(statement));
      continue;
    }

    if (/^CREATE\s+VIEW\b/i.test(statement)) {
      views.push(parseCreateView(statement));
      continue;
    }

    throw new Error(`Unsupported DDL statement '${statement}'.`);
  }

  return { tables, views };
}

export function describeDDLObjects(ddl: string): IntrospectionObjectDescriptor[] {
  const parsed = parseSchemaDDL(ddl);

  return [
    ...parsed.tables.map((tableDef) => ({
      name: tableDef.name,
      kind: "table" as const,
      dependencies: [],
    })),
    ...parsed.views.map((viewDef) => ({
      name: viewDef.name,
      kind: "view" as const,
      dependencies: [viewDef.sourceTable],
    })),
  ];
}
