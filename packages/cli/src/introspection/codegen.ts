import type {
  ImportSpec,
  ParsedColumn,
  ParsedExpressionArg,
  ParsedSchema,
  ParsedSelection,
  ParsedView,
  QuarryColumnClause,
  ParsedWhereCondition,
} from "./types";

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

  const decimalMatch = /^Decimal\((\d+),\s*(\d+)\)$/.exec(clickhouseType);
  if (decimalMatch) {
    imports.add("Decimal");
    return `Decimal(${decimalMatch[1]}, ${decimalMatch[2]})`;
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

function renderPropertyKey(value: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(value) ? value : JSON.stringify(value);
}

function renderColumnClause(clause: QuarryColumnClause | undefined): string {
  if (!clause) {
    return "";
  }

  if (clause.kind === "default") {
    return `.defaultSql(${JSON.stringify(clause.sql)})`;
  }

  if (clause.kind === "materialized") {
    return `.materializedSql(${JSON.stringify(clause.sql)})`;
  }

  return `.aliasSql(${JSON.stringify(clause.sql)})`;
}

function renderColumn(column: ParsedColumn, imports: Set<ImportSpec>): string {
  const base = renderColumnType(column.clickhouseType, imports);
  const withClause = `${base}${renderColumnClause(column.clause)}`;
  const expression = column.codecs?.length
    ? `${withClause}.codec(${renderStringArray(column.codecs)})`
    : withClause;

  return `    ${renderPropertyKey(column.name)}: ${expression},`;
}

function renderSettings(settings: Record<string, unknown>): string[] {
  return Object.entries(settings).map(([key, value]) =>
    typeof value === "string"
      ? `      ${key}: ${JSON.stringify(value)},`
      : `      ${key}: ${String(value)},`,
  );
}

function renderTable(tableDef: ParsedSchema["tables"][number], imports: Set<ImportSpec>): string {
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
    SharedSummingMergeTree: "sharedSummingMergeTree",
    AggregatingMergeTree: "aggregatingMergeTree",
    CollapsingMergeTree: "collapsingMergeTree",
    VersionedCollapsingMergeTree: "versionedCollapsingMergeTree",
  };

  const method = methodMap[tableDef.engineName];
  if (!method) {
    throw new Error(`Unsupported engine '${tableDef.engineName}'.`);
  }

  if (optionsEntries.length === 0) {
    return `  ${renderPropertyKey(tableDef.name)}: table.${method}({\n${columnsBlock}\n  }),`;
  }

  return `  ${renderPropertyKey(tableDef.name)}: table.${method}(\n  {\n${columnsBlock}\n  },\n  {\n${optionsEntries.join("\n")}\n  },\n  ),`;
}

function renderExpressionArg(arg: ParsedExpressionArg): string {
  if (arg.kind === "column") {
    return JSON.stringify(arg.column);
  }

  if (arg.kind === "boolean") {
    return String(arg.value);
  }

  if (arg.kind === "string") {
    return JSON.stringify(arg.value);
  }

  return String(arg.value);
}

function renderSelection(selection: ParsedSelection): string {
  if (selection.kind === "column") {
    return JSON.stringify(selection.column);
  }

  const args = selection.args.map((arg) => renderExpressionArg(arg)).join(", ");
  return `eb.fn.${selection.functionName}(${args}).as(${JSON.stringify(selection.alias)})`;
}

function renderViewSource(viewDef: ParsedView): string {
  if (!viewDef.final) {
    return JSON.stringify(viewDef.sourceTable);
  }

  return `db.table(${JSON.stringify(viewDef.sourceTable)}).final()`;
}

function renderWhereValue(arg: ParsedExpressionArg): string {
  if (arg.kind === "column") {
    return JSON.stringify(arg.column);
  }

  if (arg.kind === "boolean") {
    return String(arg.value);
  }

  if (arg.kind === "string") {
    return JSON.stringify(arg.value);
  }

  return String(arg.value);
}

function renderWhereCondition(condition: ParsedWhereCondition): string {
  if (condition.kind === "null") {
    return condition.negated
      ? `.whereNotNull(${JSON.stringify(condition.column)})`
      : `.whereNull(${JSON.stringify(condition.column)})`;
  }

  return `.where(${JSON.stringify(condition.column)}, ${JSON.stringify(condition.operator)}, ${renderWhereValue(condition.value)})`;
}

function renderView(viewDef: ParsedView, imports: Set<ImportSpec>): string {
  imports.add("view");

  if (viewDef.kind === "selectAll") {
    const chain = [
      `db.selectFrom(${renderViewSource(viewDef)})`,
      `.selectAll()`,
      ...(viewDef.where?.map((condition) => renderWhereCondition(condition)) ?? []),
    ];

    return `  ${renderPropertyKey(viewDef.name)}: view.as(${chain.join("")}),`;
  }

  const lines = [
    `  ${renderPropertyKey(viewDef.name)}: view.as(`,
    `    db`,
    `      .selectFrom(${renderViewSource(viewDef)})`,
    `      .selectExpr((eb) => [${viewDef.selections.map((selection) => renderSelection(selection)).join(", ")}])`,
  ];

  for (const condition of viewDef.where ?? []) {
    lines.push(`      ${renderWhereCondition(condition)}`);
  }

  if (viewDef.groupBy && viewDef.groupBy.length > 0) {
    lines.push(
      `      .groupBy(${viewDef.groupBy.map((column) => JSON.stringify(column)).join(", ")})`,
    );
  }

  lines.push("  ),");
  return lines.join("\n");
}

export function generateSchemaModuleFromParsed(parsed: ParsedSchema): string {
  const imports = new Set<ImportSpec>(["defineSchema", "table"]);
  const tableLines = parsed.tables.map((tableDef) => renderTable(tableDef, imports));
  const importList = [...imports].sort((left, right) => left.localeCompare(right));

  let schemaExpression = `defineSchema({\n${tableLines.join("\n")}\n})`;

  if (parsed.views.length > 0) {
    imports.add("view");
    const updatedImportList = [...imports].sort((left, right) => left.localeCompare(right));
    const viewLines = parsed.views.map((viewDef) => renderView(viewDef, imports));
    schemaExpression = `${schemaExpression}.views((db) => ({\n${viewLines.join("\n")}\n}))`;
    return `import { ${updatedImportList.join(", ")} } from "@oorestisime/quarry";\n\nexport const schema = ${schemaExpression};\n`;
  }

  return `import { ${importList.join(", ")} } from "@oorestisime/quarry";\n\nexport const schema = ${schemaExpression};\n`;
}
