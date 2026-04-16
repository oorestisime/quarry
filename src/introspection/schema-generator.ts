import { ExpressionBuilder } from "../query/expression-builder";

type ParsedColumn = {
  readonly name: string;
  readonly clickhouseType: string;
  readonly codecs?: readonly string[];
};

type ParsedTable = {
  readonly name: string;
  readonly columns: readonly ParsedColumn[];
  readonly engineName: string;
  readonly engineOptions: Record<string, unknown>;
};

type ParsedSelection =
  | {
      readonly kind: "column";
      readonly column: string;
    }
  | {
      readonly kind: "function";
      readonly functionName: string;
      readonly alias: string;
      readonly args: readonly ParsedExpressionArg[];
    };

type ParsedExpressionArg =
  | {
      readonly kind: "column";
      readonly column: string;
    }
  | {
      readonly kind: "string";
      readonly value: string;
    }
  | {
      readonly kind: "number";
      readonly value: number;
    };

type ParsedView =
  | {
      readonly kind: "selectAll";
      readonly name: string;
      readonly sourceTable: string;
    }
  | {
      readonly kind: "selectExpr";
      readonly name: string;
      readonly sourceTable: string;
      readonly selections: readonly ParsedSelection[];
      readonly groupBy?: readonly string[];
    };

type ParsedSchema = {
  readonly tables: readonly ParsedTable[];
  readonly views: readonly ParsedView[];
};

type ImportSpec =
  | "Array as CHArray"
  | "Bool"
  | "Date as CHDate"
  | "Date32"
  | "DateTime"
  | "DateTime64"
  | "FixedString"
  | "Float32"
  | "Float64"
  | "IPv4"
  | "IPv6"
  | "Int8"
  | "Int16"
  | "Int32"
  | "Int64"
  | "LowCardinality"
  | "Nullable"
  | "String as CHString"
  | "UInt8"
  | "UInt16"
  | "UInt32"
  | "UInt64"
  | "UUID"
  | "defineSchema"
  | "table"
  | "view";

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

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function unquoteIdentifier(value: string): string {
  const trimmed = value.trim();
  const unquoted =
    (trimmed.startsWith("`") && trimmed.endsWith("`")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
      ? trimmed.slice(1, -1)
      : trimmed;

  const segments = unquoted.split(".");
  return segments[segments.length - 1]!;
}

function isIdentifierLike(value: string): boolean {
  return /^(?:`?[A-Za-z_][A-Za-z0-9_]*`?\.)*`?[A-Za-z_][A-Za-z0-9_]*`?$/.test(value.trim());
}

function splitTopLevel(value: string, delimiter: string): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  let quote: "'" | '"' | "`" | null = null;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index]!;

    if (quote) {
      current += character;
      if (character === quote && value[index - 1] !== "\\") {
        quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === "`") {
      quote = character;
      current += character;
      continue;
    }

    if (character === "(") {
      depth += 1;
      current += character;
      continue;
    }

    if (character === ")") {
      depth -= 1;
      current += character;
      continue;
    }

    if (character === delimiter && depth === 0) {
      parts.push(current.trim());
      current = "";
      continue;
    }

    current += character;
  }

  if (current.trim().length > 0) {
    parts.push(current.trim());
  }

  return parts;
}

function splitStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = "";
  let depth = 0;
  let quote: "'" | '"' | "`" | null = null;

  for (let index = 0; index < sql.length; index += 1) {
    const character = sql[index]!;

    if (quote) {
      current += character;
      if (character === quote && sql[index - 1] !== "\\") {
        quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === "`") {
      quote = character;
      current += character;
      continue;
    }

    if (character === "(") {
      depth += 1;
      current += character;
      continue;
    }

    if (character === ")") {
      depth -= 1;
      current += character;
      continue;
    }

    if (character === ";" && depth === 0) {
      if (current.trim().length > 0) {
        statements.push(current.trim());
      }
      current = "";
      continue;
    }

    current += character;
  }

  if (current.trim().length > 0) {
    statements.push(current.trim());
  }

  return statements;
}

function findMatchingParen(value: string, openIndex: number): number {
  let depth = 0;
  let quote: "'" | '"' | "`" | null = null;

  for (let index = openIndex; index < value.length; index += 1) {
    const character = value[index]!;

    if (quote) {
      if (character === quote && value[index - 1] !== "\\") {
        quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === "`") {
      quote = character;
      continue;
    }

    if (character === "(") {
      depth += 1;
      continue;
    }

    if (character === ")") {
      depth -= 1;
      if (depth === 0) {
        return index;
      }
    }
  }

  throw new Error("Unbalanced parentheses in DDL.");
}

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

function parseColumn(line: string): ParsedColumn {
  if (/\b(?:DEFAULT|MATERIALIZED|ALIAS)\b/i.test(line)) {
    throw new Error(`Unsupported column clause in '${line}'.`);
  }

  const match = /^(`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+([\s\S]+)$/.exec(line.trim());
  if (!match) {
    throw new Error(`Unsupported column definition '${line}'.`);
  }

  const codecs = parseCodecs(match[2]!);
  const clickhouseType = normalizeWhitespace(stripCodecs(match[2]!));

  return {
    name: unquoteIdentifier(match[1]!),
    clickhouseType,
    ...(codecs ? { codecs } : {}),
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

function parseCreateView(statement: string): ParsedView {
  const match =
    /^CREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s]+)\s+AS\s+SELECT\s+([\s\S]+?)\s+FROM\s+([^\s]+)(?:\s+GROUP\s+BY\s+([\s\S]+))?$/i.exec(
      statement,
    );
  if (!match) {
    throw new Error(`Unsupported CREATE VIEW statement '${statement}'.`);
  }

  const name = unquoteIdentifier(match[1]!);
  const sourceTable = unquoteIdentifier(match[3]!);
  const selectClause = normalizeWhitespace(match[2]!);
  const groupBy = parseExpressionList(match[4]);

  if (selectClause === "*") {
    return {
      kind: "selectAll",
      name,
      sourceTable,
    };
  }

  return {
    kind: "selectExpr",
    name,
    sourceTable,
    selections: splitTopLevel(match[2]!, ",").map((selection) => parseSelection(selection, name)),
    ...(groupBy ? { groupBy: groupBy.map(unquoteIdentifier) } : {}),
  };
}

function parseSchemaDDL(ddl: string): ParsedSchema {
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

function renderColumnType(clickhouseType: string, imports: Set<ImportSpec>): string {
  if (clickhouseType === "String") {
    imports.add("String as CHString");
    return "CHString()";
  }

  if (clickhouseType === "Bool") {
    imports.add("Bool");
    return "Bool()";
  }

  if (clickhouseType === "UInt8") {
    imports.add("UInt8");
    return "UInt8()";
  }

  if (clickhouseType === "UInt16") {
    imports.add("UInt16");
    return "UInt16()";
  }

  if (clickhouseType === "UInt32") {
    imports.add("UInt32");
    return "UInt32()";
  }

  if (clickhouseType === "UInt64") {
    imports.add("UInt64");
    return "UInt64()";
  }

  if (clickhouseType === "Int8") {
    imports.add("Int8");
    return "Int8()";
  }

  if (clickhouseType === "Int16") {
    imports.add("Int16");
    return "Int16()";
  }

  if (clickhouseType === "Int32") {
    imports.add("Int32");
    return "Int32()";
  }

  if (clickhouseType === "Int64") {
    imports.add("Int64");
    return "Int64()";
  }

  if (clickhouseType === "Float32") {
    imports.add("Float32");
    return "Float32()";
  }

  if (clickhouseType === "Float64") {
    imports.add("Float64");
    return "Float64()";
  }

  if (clickhouseType === "Date") {
    imports.add("Date as CHDate");
    return "CHDate()";
  }

  if (clickhouseType === "Date32") {
    imports.add("Date32");
    return "Date32()";
  }

  if (clickhouseType === "DateTime") {
    imports.add("DateTime");
    return "DateTime()";
  }

  const dateTime64Match = /^DateTime64\((\d+)\)$/.exec(clickhouseType);
  if (dateTime64Match) {
    imports.add("DateTime64");
    return `DateTime64(${dateTime64Match[1]})`;
  }

  const fixedStringMatch = /^FixedString\((\d+)\)$/.exec(clickhouseType);
  if (fixedStringMatch) {
    imports.add("FixedString");
    return `FixedString(${fixedStringMatch[1]})`;
  }

  if (clickhouseType === "UUID") {
    imports.add("UUID");
    return "UUID()";
  }

  if (clickhouseType === "IPv4") {
    imports.add("IPv4");
    return "IPv4()";
  }

  if (clickhouseType === "IPv6") {
    imports.add("IPv6");
    return "IPv6()";
  }

  const nullableMatch = /^Nullable\((.+)\)$/.exec(clickhouseType);
  if (nullableMatch) {
    imports.add("Nullable");
    return `Nullable(${renderColumnType(nullableMatch[1]!, imports)})`;
  }

  const lowCardinalityMatch = /^LowCardinality\((.+)\)$/.exec(clickhouseType);
  if (lowCardinalityMatch) {
    imports.add("LowCardinality");
    return `LowCardinality(${renderColumnType(lowCardinalityMatch[1]!, imports)})`;
  }

  const arrayMatch = /^Array\((.+)\)$/.exec(clickhouseType);
  if (arrayMatch) {
    imports.add("Array as CHArray");
    return `CHArray(${renderColumnType(arrayMatch[1]!, imports)})`;
  }

  throw new Error(`Unsupported ClickHouse type '${clickhouseType}'.`);
}

function renderStringArray(values: readonly string[]): string {
  return `[${values.map((value) => JSON.stringify(value)).join(", ")}]`;
}

function renderColumn(column: ParsedColumn, imports: Set<ImportSpec>): string {
  const base = renderColumnType(column.clickhouseType, imports);
  const expression = column.codecs?.length
    ? `${base}.codec(${renderStringArray(column.codecs)})`
    : base;
  return `    ${column.name}: ${expression},`;
}

function renderSettings(settings: Record<string, unknown>): string[] {
  return Object.entries(settings).map(([key, value]) =>
    typeof value === "string"
      ? `      ${key}: ${JSON.stringify(value)},`
      : `      ${key}: ${String(value)},`,
  );
}

function renderTable(tableDef: ParsedTable, imports: Set<ImportSpec>): string {
  imports.add("table");

  const columnsBlock = tableDef.columns.map((column) => renderColumn(column, imports)).join("\n");
  const optionsEntries: string[] = [];

  for (const [key, value] of Object.entries(tableDef.engineOptions)) {
    if (value === undefined) {
      continue;
    }

    if (key === "settings" && value && typeof value === "object") {
      optionsEntries.push("    settings: {");
      optionsEntries.push(...renderSettings(value as Record<string, unknown>));
      optionsEntries.push("    },");
      continue;
    }

    if (Array.isArray(value)) {
      optionsEntries.push(`    ${key}: ${renderStringArray(value as string[])},`);
      continue;
    }

    optionsEntries.push(`    ${key}: ${JSON.stringify(value)},`);
  }

  const methodMap: Record<string, string> = {
    Memory: "memory",
    MergeTree: "mergeTree",
    SharedMergeTree: "sharedMergeTree",
    ReplacingMergeTree: "replacingMergeTree",
    SharedReplacingMergeTree: "sharedReplacingMergeTree",
    SummingMergeTree: "summingMergeTree",
    AggregatingMergeTree: "aggregatingMergeTree",
    CollapsingMergeTree: "collapsingMergeTree",
    VersionedCollapsingMergeTree: "versionedCollapsingMergeTree",
  };

  const method = methodMap[tableDef.engineName];
  if (!method) {
    throw new Error(`Unsupported engine '${tableDef.engineName}'.`);
  }

  if (optionsEntries.length === 0) {
    return `  ${tableDef.name}: table.${method}({\n${columnsBlock}\n  }),`;
  }

  return `  ${tableDef.name}: table.${method}(\n  {\n${columnsBlock}\n  },\n  {\n${optionsEntries.join("\n")}\n  },\n  ),`;
}

function renderExpressionArg(arg: ParsedExpressionArg, alias: string): string {
  if (arg.kind === "column") {
    return JSON.stringify(`${alias}.${arg.column}`);
  }

  if (arg.kind === "string") {
    return JSON.stringify(arg.value);
  }

  return String(arg.value);
}

function renderSelection(selection: ParsedSelection, alias: string): string {
  if (selection.kind === "column") {
    return JSON.stringify(`${alias}.${selection.column}`);
  }

  const args = selection.args.map((arg) => renderExpressionArg(arg, alias)).join(", ");
  return `eb.fn.${selection.functionName}(${args}).as(${JSON.stringify(selection.alias)})`;
}

function renderView(viewDef: ParsedView, imports: Set<ImportSpec>, index: number): string {
  imports.add("view");
  const alias = `t${index}`;

  if (viewDef.kind === "selectAll") {
    return `  ${viewDef.name}: view.as(db.selectFrom(${JSON.stringify(`${viewDef.sourceTable} as ${alias}`)}).selectAll(${JSON.stringify(alias)})),`;
  }

  const lines = [
    `  ${viewDef.name}: view.as(`,
    `    db`,
    `      .selectFrom(${JSON.stringify(`${viewDef.sourceTable} as ${alias}`)})`,
    `      .selectExpr((eb) => [${viewDef.selections.map((selection) => renderSelection(selection, alias)).join(", ")}])`,
  ];

  if (viewDef.groupBy && viewDef.groupBy.length > 0) {
    lines.push(
      `      .groupBy(${viewDef.groupBy
        .map((column) => JSON.stringify(`${alias}.${column}`))
        .join(", ")})`,
    );
  }

  lines.push("  ),");
  return lines.join("\n");
}

export function generateSchemaModuleFromDDL(ddl: string): string {
  const parsed = parseSchemaDDL(ddl);
  const imports = new Set<ImportSpec>(["defineSchema", "table"]);
  const tableLines = parsed.tables.map((tableDef) => renderTable(tableDef, imports));
  const importList = [...imports].sort((left, right) => left.localeCompare(right));

  let schemaExpression = `defineSchema({\n${tableLines.join("\n")}\n})`;

  if (parsed.views.length > 0) {
    imports.add("view");
    const updatedImportList = [...imports].sort((left, right) => left.localeCompare(right));
    const viewLines = parsed.views.map((viewDef, index) => renderView(viewDef, imports, index));
    schemaExpression = `${schemaExpression}.views((db) => ({\n${viewLines.join("\n")}\n}))`;
    return `import { ${updatedImportList.join(", ")} } from "@oorestisime/quarry";\n\nexport const schema = ${schemaExpression};\n`;
  }

  return `import { ${importList.join(", ")} } from "@oorestisime/quarry";\n\nexport const schema = ${schemaExpression};\n`;
}
