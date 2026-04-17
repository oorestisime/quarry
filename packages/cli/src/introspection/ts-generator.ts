import { normalizeWhitespace, splitTopLevel } from "./syntax";

export interface TypeScriptIntrospectionColumn {
  readonly objectName: string;
  readonly name: string;
  readonly clickhouseType: string;
  readonly position: number;
}

type TypeScriptImportSpec =
  | "ClickHouseDate"
  | "ClickHouseDate32"
  | "ClickHouseDateTime"
  | "ClickHouseDateTime64"
  | "ClickHouseDecimal"
  | "ClickHouseInt64"
  | "ClickHouseUInt64"
  | "TypedTable"
  | "TypedView";

interface TypeScriptIntrospectionSource {
  readonly name: string;
  readonly columns: readonly TypeScriptIntrospectionColumn[];
}

function renderPropertyKey(value: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(value) ? value : JSON.stringify(value);
}

function toInterfaceBaseName(value: string): string {
  const parts = value.match(/[A-Za-z0-9]+/g) ?? [];
  const joined = parts.map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`).join("");
  const normalized = joined.length > 0 ? joined : "Source";
  return /^[0-9]/.test(normalized) ? `_${normalized}` : normalized;
}

function buildInterfaceNames(
  sources: readonly TypeScriptIntrospectionSource[],
): Map<string, string> {
  const interfaceNames = new Map<string, string>();
  const usedNames = new Map<string, number>();

  for (const source of sources) {
    const baseName = toInterfaceBaseName(source.name);
    const nextIndex = (usedNames.get(baseName) ?? 0) + 1;
    usedNames.set(baseName, nextIndex);
    interfaceNames.set(source.name, nextIndex === 1 ? baseName : `${baseName}${nextIndex}`);
  }

  return interfaceNames;
}

function wrapUnion(type: string): string {
  return type.includes("|") ? `(${type})` : type;
}

function parseWrappedType(clickhouseType: string, prefix: string): string | undefined {
  if (!clickhouseType.startsWith(`${prefix}(`) || !clickhouseType.endsWith(")")) {
    return undefined;
  }

  return clickhouseType.slice(prefix.length + 1, -1);
}

function renderTupleType(inner: string, imports: Set<TypeScriptImportSpec>): string {
  const parts = splitTopLevel(inner, ",");
  if (parts.length === 0) {
    return "[]";
  }

  const namedEntries = parts.map((part) => {
    const match = /^(`[^`]+`|"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+([\s\S]+)$/.exec(part);
    if (!match) {
      return undefined;
    }

    return {
      name: match[1]!.replace(/^([`"])(.*)\1$/, "$2"),
      type: renderTypeScriptType(match[2]!, imports, false),
    };
  });

  if (namedEntries.every((entry) => entry !== undefined)) {
    return `{ ${namedEntries.map((entry) => `${renderPropertyKey(entry!.name)}: ${entry!.type}`).join("; ")} }`;
  }

  return `[${parts.map((part) => renderTypeScriptType(part, imports, false)).join(", ")}]`;
}

function renderScalarType(
  normalized: string,
  imports: Set<TypeScriptImportSpec>,
  allowAliases: boolean,
): string | undefined {
  if (
    normalized === "String" ||
    normalized === "UUID" ||
    normalized === "IPv4" ||
    normalized === "IPv6" ||
    normalized === "JSON" ||
    /^FixedString\(\d+\)$/.test(normalized) ||
    /^Enum(?:8|16)\(/.test(normalized) ||
    /^Int(?:128|256)$/.test(normalized) ||
    /^UInt(?:128|256)$/.test(normalized)
  ) {
    return "string";
  }

  if (normalized === "Date") {
    if (allowAliases) {
      imports.add("ClickHouseDate");
      return "ClickHouseDate";
    }

    return "string";
  }

  if (normalized === "Date32") {
    if (allowAliases) {
      imports.add("ClickHouseDate32");
      return "ClickHouseDate32";
    }

    return "string";
  }

  if (normalized === "DateTime" || /^DateTime\('[^']+'\)$/.test(normalized)) {
    if (allowAliases) {
      imports.add("ClickHouseDateTime");
      return "ClickHouseDateTime";
    }

    return "string";
  }

  if (/^DateTime64\(\d+(?:,\s*'[^']+')?\)$/.test(normalized)) {
    if (allowAliases) {
      imports.add("ClickHouseDateTime64");
      return "ClickHouseDateTime64";
    }

    return "string";
  }

  if (normalized === "Bool") {
    return "boolean";
  }

  if (/^Int64$/.test(normalized)) {
    if (allowAliases) {
      imports.add("ClickHouseInt64");
      return "ClickHouseInt64";
    }

    return "string";
  }

  if (/^UInt64$/.test(normalized)) {
    if (allowAliases) {
      imports.add("ClickHouseUInt64");
      return "ClickHouseUInt64";
    }

    return "string";
  }

  if (
    /^Int(?:8|16|32)$/.test(normalized) ||
    /^UInt(?:8|16|32)$/.test(normalized) ||
    /^Float(?:32|64)$/.test(normalized)
  ) {
    return "number";
  }

  if (/^Decimal(?:32|64|128|256)?\(/.test(normalized)) {
    if (allowAliases) {
      imports.add("ClickHouseDecimal");
      return "ClickHouseDecimal";
    }

    return "number";
  }

  return undefined;
}

export function renderTypeScriptType(
  clickhouseType: string,
  imports: Set<TypeScriptImportSpec>,
  allowAliases = true,
): string {
  const normalized = normalizeWhitespace(clickhouseType);

  const scalarType = renderScalarType(normalized, imports, allowAliases);
  if (scalarType) {
    return scalarType;
  }

  const nullableInner = parseWrappedType(normalized, "Nullable");
  if (nullableInner) {
    return `${wrapUnion(renderTypeScriptType(nullableInner, imports, allowAliases))} | null`;
  }

  const lowCardinalityInner = parseWrappedType(normalized, "LowCardinality");
  if (lowCardinalityInner) {
    return renderTypeScriptType(lowCardinalityInner, imports, allowAliases);
  }

  const arrayInner = parseWrappedType(normalized, "Array");
  if (arrayInner) {
    return `Array<${renderTypeScriptType(arrayInner, imports, false)}>`;
  }

  const mapInner = parseWrappedType(normalized, "Map");
  if (mapInner) {
    const parts = splitTopLevel(mapInner, ",");
    const valueType = parts[1] ? renderTypeScriptType(parts[1], imports, false) : "unknown";
    return `Record<string, ${valueType}>`;
  }

  const tupleInner = parseWrappedType(normalized, "Tuple");
  if (tupleInner) {
    return renderTupleType(tupleInner, imports);
  }

  const simpleAggregateInner = parseWrappedType(normalized, "SimpleAggregateFunction");
  if (simpleAggregateInner) {
    const parts = splitTopLevel(simpleAggregateInner, ",");
    return parts[1] ? renderTypeScriptType(parts[1], imports, allowAliases) : "unknown";
  }

  if (normalized.startsWith("AggregateFunction(") || normalized.startsWith("Object(")) {
    return "unknown";
  }

  return "unknown";
}

function renderColumns(
  columns: readonly TypeScriptIntrospectionColumn[],
  imports: Set<TypeScriptImportSpec>,
): string {
  return columns
    .slice()
    .sort((left, right) => left.position - right.position)
    .map(
      (column) =>
        `    ${renderPropertyKey(column.name)}: ${renderTypeScriptType(column.clickhouseType, imports)};`,
    )
    .join("\n");
}

function renderSourceBlock(
  name: string,
  interfaceName: string,
  kind: "table" | "view",
  imports: Set<TypeScriptImportSpec>,
): string {
  const wrapper = kind === "table" ? "TypedTable" : "TypedView";
  imports.add(wrapper);
  return `  ${renderPropertyKey(name)}: ${wrapper}<${interfaceName}>;`;
}

function renderInterfaceBlock(name: string, entries: readonly string[]): string {
  if (entries.length === 0) {
    return `export interface ${name} {}`;
  }

  return `export interface ${name} {\n${entries.join("\n")}\n}`;
}

function renderRowInterface(
  name: string,
  columns: readonly TypeScriptIntrospectionColumn[],
  imports: Set<TypeScriptImportSpec>,
): string {
  const columnsBlock = renderColumns(columns, imports);
  return columnsBlock.length > 0
    ? `export interface ${name} {\n${columnsBlock}\n}`
    : `export interface ${name} {}`;
}

export function generateTypeScriptSchemaModule(
  tables: readonly { name: string; columns: readonly TypeScriptIntrospectionColumn[] }[],
  views: readonly { name: string; columns: readonly TypeScriptIntrospectionColumn[] }[],
): string {
  const imports = new Set<TypeScriptImportSpec>();
  const sources = [...tables, ...views];
  const interfaceNames = buildInterfaceNames(sources);
  const rowInterfaces = sources.map((source) =>
    renderRowInterface(interfaceNames.get(source.name)!, source.columns, imports),
  );
  const tablesBlock = renderInterfaceBlock(
    "Tables",
    tables.map((table) =>
      renderSourceBlock(table.name, interfaceNames.get(table.name)!, "table", imports),
    ),
  );
  const viewsBlock = renderInterfaceBlock(
    "Views",
    views.map((view) =>
      renderSourceBlock(view.name, interfaceNames.get(view.name)!, "view", imports),
    ),
  );
  const importList = [...imports].sort((left, right) => left.localeCompare(right));
  const importBlock = `import type { ${importList.join(", ")} } from "@oorestisime/quarry";\n\n`;

  return `${importBlock}${rowInterfaces.join("\n\n")}\n\n${tablesBlock}\n\n${viewsBlock}\n\nexport interface DB extends Tables, Views {}\n`;
}
