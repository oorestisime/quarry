import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import process from "node:process";
import { createClient } from "@clickhouse/client";
import {
  generateTypeScriptSchemaModule,
  type TypeScriptColumnTypeOverrides,
  type TypeScriptIntrospectionColumn,
  type TypeScriptTypeImport,
} from "./introspection/ts-generator";

export interface IntrospectArgs {
  readonly _configResolved?: boolean;
  readonly config?: string;
  readonly url?: string;
  readonly user?: string;
  readonly password?: string;
  readonly database?: string;
  readonly out?: string;
  readonly tablesOnly?: boolean;
  readonly includePattern?: string;
  readonly excludePattern?: string;
  readonly imports?: readonly TypeScriptTypeImport[];
  readonly typeOverrides?: TypeScriptColumnTypeOverrides;
}

export interface IntrospectionConfig {
  readonly url?: string;
  readonly user?: string;
  readonly password?: string;
  readonly database?: string;
  readonly out?: string;
  readonly tablesOnly?: boolean;
  readonly includePattern?: string;
  readonly excludePattern?: string;
  readonly imports?: Record<string, readonly ImportConfigSpec[]>;
  readonly typeOverrides?: Record<string, Record<string, string>>;
}

type ImportConfigSpec = string | { readonly name: string; readonly as?: string };

export interface IntrospectionConnectionOptions {
  readonly url: string;
  readonly user?: string;
  readonly password?: string;
  readonly database: string;
  readonly includePattern?: RegExp;
  readonly excludePattern?: RegExp;
}

export interface IntrospectionObject {
  readonly name: string;
  readonly engine: string;
}

export interface IntrospectionFailure {
  readonly name: string;
  readonly engine: string;
  readonly message: string;
}

export interface IntrospectionSummary {
  readonly generatedObjects: number;
  readonly generatedTables: number;
  readonly generatedViews: number;
  readonly generatedDictionaries: number;
  readonly skippedObjects: number;
}

export interface IntrospectionResult {
  readonly source: string;
  readonly failures: readonly IntrospectionFailure[];
  readonly summary: IntrospectionSummary;
}

export interface IntrospectionSchemaHeaderOptions {
  readonly config?: string;
  readonly database?: string;
  readonly tablesOnly?: boolean;
  readonly includePattern?: string;
  readonly excludePattern?: string;
  readonly overriddenColumns?: number;
}

interface SystemTableRow {
  readonly name: string;
  readonly engine: string;
}

interface SystemColumnRow {
  readonly table: string;
  readonly name: string;
  readonly type: string;
  readonly position: number;
}

interface SystemDictionaryRow {
  readonly name: string;
}

interface SystemDictionaryAttributeRow {
  readonly objectName: string;
  readonly name: string;
  readonly clickhouseType: string;
}

const MAX_FAILURE_DETAILS = 40;
const CONFIG_KEYS = new Set([
  "url",
  "user",
  "password",
  "database",
  "out",
  "tablesOnly",
  "includePattern",
  "excludePattern",
  "imports",
  "typeOverrides",
]);
const IDENTIFIER_PATTERN = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function sortObjects(left: IntrospectionObject, right: IntrospectionObject): number {
  const leftRank = left.engine.endsWith("View") ? 1 : 0;
  const rightRank = right.engine.endsWith("View") ? 1 : 0;

  if (leftRank !== rightRank) {
    return leftRank - rightRank;
  }

  return left.name.localeCompare(right.name);
}

function tryFormatTypeScript(source: string): string {
  try {
    return execFileSync("oxfmt", ["--stdin-filepath", "db.ts"], {
      input: source,
      encoding: "utf8",
    });
  } catch {
    return source;
  }
}

function isTableLikeObject(object: IntrospectionObject): boolean {
  return object.engine !== "View";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stripUndefinedArgs(args: IntrospectArgs): IntrospectArgs {
  return Object.fromEntries(
    Object.entries(args).filter(([, value]) => value !== undefined),
  ) as IntrospectArgs;
}

function validateStringField(
  config: Record<string, unknown>,
  key: keyof IntrospectionConfig,
): string | undefined {
  const value = config[key];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(`Invalid introspection config: '${key}' must be a string.`);
  }
  return value;
}

function validateBooleanField(
  config: Record<string, unknown>,
  key: keyof IntrospectionConfig,
): boolean | undefined {
  const value = config[key];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "boolean") {
    throw new Error(`Invalid introspection config: '${key}' must be a boolean.`);
  }
  return value;
}

function validateImportName(value: string, label: string): void {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`Invalid introspection config: ${label} must be a TypeScript identifier.`);
  }
}

function validateOverrideType(value: string, sourceName: string, columnName: string): void {
  if (value.trim().length === 0 || value !== value.trim() || /[;\r\n]/.test(value)) {
    throw new Error(
      `Invalid introspection config: type override for '${sourceName}.${columnName}' must be a single-line TypeScript type expression.`,
    );
  }
}

function normalizeConfiguredImports(value: unknown): readonly TypeScriptTypeImport[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!isRecord(value)) {
    throw new Error("Invalid introspection config: 'imports' must be an object.");
  }

  const localNames = new Set<string>();
  const imports: TypeScriptTypeImport[] = [];
  for (const [from, specs] of Object.entries(value)) {
    if (from.length === 0) {
      throw new Error("Invalid introspection config: import module path must not be empty.");
    }
    if (!Array.isArray(specs)) {
      throw new Error(`Invalid introspection config: imports for '${from}' must be an array.`);
    }

    for (const spec of specs) {
      let normalized: TypeScriptTypeImport;
      if (typeof spec === "string") {
        validateImportName(spec, `import '${spec}'`);
        normalized = { from, name: spec };
      } else if (isRecord(spec)) {
        const name = spec.name;
        const alias = spec.as;
        if (typeof name !== "string") {
          throw new Error(`Invalid introspection config: import from '${from}' is missing a name.`);
        }
        validateImportName(name, `import '${name}'`);
        if (alias !== undefined && typeof alias !== "string") {
          throw new Error(
            `Invalid introspection config: import alias for '${name}' must be a string.`,
          );
        }
        if (alias !== undefined) {
          validateImportName(alias, `import alias '${alias}'`);
        }
        normalized = alias ? { from, name, as: alias } : { from, name };
      } else {
        throw new Error(
          `Invalid introspection config: imports for '${from}' must be strings or objects.`,
        );
      }

      const localName = normalized.as ?? normalized.name;
      if (localNames.has(localName)) {
        throw new Error(`Invalid introspection config: duplicate imported type '${localName}'.`);
      }
      localNames.add(localName);
      imports.push(normalized);
    }
  }

  return imports;
}

function normalizeTypeOverrides(value: unknown): TypeScriptColumnTypeOverrides | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!isRecord(value)) {
    throw new Error("Invalid introspection config: 'typeOverrides' must be an object.");
  }

  const overrides = new Map<string, Map<string, string>>();
  for (const [sourceName, sourceOverrides] of Object.entries(value)) {
    if (!isRecord(sourceOverrides)) {
      throw new Error(
        `Invalid introspection config: typeOverrides for '${sourceName}' must be an object.`,
      );
    }

    const columns = new Map<string, string>();
    for (const [columnName, type] of Object.entries(sourceOverrides)) {
      if (typeof type !== "string") {
        throw new Error(
          `Invalid introspection config: type override for '${sourceName}.${columnName}' must be a string.`,
        );
      }
      validateOverrideType(type, sourceName, columnName);
      columns.set(columnName, type);
    }
    overrides.set(sourceName, columns);
  }

  return overrides;
}

function validateImportedTypesAreUsed(
  imports?: readonly TypeScriptTypeImport[],
  typeOverrides?: TypeScriptColumnTypeOverrides,
): void {
  if (!imports || imports.length === 0) {
    return;
  }

  const overrideTypes = [...(typeOverrides?.values() ?? [])].flatMap((sourceOverrides) => [
    ...sourceOverrides.values(),
  ]);
  for (const spec of imports) {
    const localName = spec.as ?? spec.name;
    const referencePattern = new RegExp(`\\b${localName}\\b`);
    if (!overrideTypes.some((type) => referencePattern.test(type))) {
      throw new Error(
        `Invalid introspection config: imported type '${localName}' is not referenced by any type override.`,
      );
    }
  }
}

function pluralize(count: number, singular: string, plural = `${singular}s`): string {
  return `${count} ${count === 1 ? singular : plural}`;
}

function formatSchemaModuleHeader(): string {
  return "// This file is autogenerated by `quarry introspect`. Do not edit manually.";
}

function buildSchemaHeaderOptions(
  args: IntrospectArgs,
  options: Pick<IntrospectionConnectionOptions, "database">,
): IntrospectionSchemaHeaderOptions {
  return {
    database: options.database,
    tablesOnly: args.tablesOnly ?? false,
    ...(args.includePattern ? { includePattern: args.includePattern } : {}),
    ...(args.excludePattern ? { excludePattern: args.excludePattern } : {}),
  };
}

export async function loadIntrospectionConfig(configPath: string): Promise<IntrospectArgs> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(await readFile(configPath, "utf8"));
  } catch (error) {
    throw new Error(
      `Could not read introspection config '${configPath}': ${getErrorMessage(error)}`,
    );
  }

  if (!isRecord(parsed)) {
    throw new Error("Invalid introspection config: root value must be an object.");
  }

  for (const key of Object.keys(parsed)) {
    if (!CONFIG_KEYS.has(key)) {
      throw new Error(`Invalid introspection config: unknown key '${key}'.`);
    }
  }

  const imports = normalizeConfiguredImports(parsed.imports);
  const typeOverrides = normalizeTypeOverrides(parsed.typeOverrides);
  validateImportedTypesAreUsed(imports, typeOverrides);

  return {
    url: validateStringField(parsed, "url"),
    user: validateStringField(parsed, "user"),
    password: validateStringField(parsed, "password"),
    database: validateStringField(parsed, "database"),
    out: validateStringField(parsed, "out"),
    tablesOnly: validateBooleanField(parsed, "tablesOnly"),
    includePattern: validateStringField(parsed, "includePattern"),
    excludePattern: validateStringField(parsed, "excludePattern"),
    imports,
    typeOverrides,
  };
}

export async function resolveIntrospectionArgs(args: IntrospectArgs): Promise<IntrospectArgs> {
  if (args._configResolved) {
    return args;
  }

  const configArgs = args.config ? await loadIntrospectionConfig(args.config) : {};
  return {
    ...configArgs,
    ...stripUndefinedArgs(args),
    _configResolved: true,
  };
}

export function formatIntrospectionFailureReport(
  failures: readonly IntrospectionFailure[],
  prefix = "Skipped",
): string {
  const visibleFailures = failures.slice(0, MAX_FAILURE_DETAILS);
  const details = visibleFailures
    .map((failure) => `- ${failure.name} (${failure.engine}): ${failure.message}`)
    .join("\n");
  const remainder =
    failures.length > MAX_FAILURE_DETAILS
      ? `\n...and ${failures.length - MAX_FAILURE_DETAILS} more unsupported objects.`
      : "";

  return `${prefix} ${failures.length} unsupported objects:\n${details}${remainder}`;
}

export function formatIntrospectionSummaryReport(summary: IntrospectionSummary): string {
  const parts = [
    pluralize(summary.generatedTables, "table"),
    pluralize(summary.generatedViews, "view"),
    pluralize(summary.generatedDictionaries, "dictionary", "dictionaries"),
  ];
  const report = `Generated ${pluralize(summary.generatedObjects, "object")}: ${parts.join(", ")}.`;
  return summary.skippedObjects > 0
    ? `${report} Skipped ${pluralize(summary.skippedObjects, "object")}.`
    : report;
}

export function resolveConnectionOptions(
  args: IntrospectArgs,
  env: NodeJS.ProcessEnv = process.env,
): IntrospectionConnectionOptions {
  const url = args.url ?? env.CLICKHOUSE_URL;
  if (!url) {
    throw new Error("Missing ClickHouse URL. Pass --url or set CLICKHOUSE_URL in the environment.");
  }

  const compilePattern = (
    pattern: string | undefined,
    name: "includePattern" | "excludePattern",
  ) => {
    if (!pattern) {
      return undefined;
    }

    try {
      return new RegExp(pattern);
    } catch (error) {
      throw new Error(`Invalid ${name} '${pattern}': ${getErrorMessage(error)}`);
    }
  };

  return {
    url,
    user: args.user ?? env.CLICKHOUSE_USER,
    password: args.password ?? env.CLICKHOUSE_PASSWORD,
    database: args.database ?? env.CLICKHOUSE_DATABASE ?? "default",
    ...(args.includePattern
      ? { includePattern: compilePattern(args.includePattern, "includePattern") }
      : {}),
    ...(args.excludePattern
      ? { excludePattern: compilePattern(args.excludePattern, "excludePattern") }
      : {}),
  };
}

export function filterExcludedObjects(
  objects: readonly IntrospectionObject[],
  includePattern?: RegExp,
  excludePattern?: RegExp,
): IntrospectionObject[] {
  return objects.filter((object) => {
    if (includePattern && !includePattern.test(object.name)) {
      return false;
    }

    if (excludePattern && excludePattern.test(object.name)) {
      return false;
    }

    return true;
  });
}

function validateTypeOverrideTargets(
  sources: readonly { name: string; columns: readonly TypeScriptIntrospectionColumn[] }[],
  typeOverrides?: TypeScriptColumnTypeOverrides,
): void {
  if (!typeOverrides) {
    return;
  }

  const columnsBySource = new Map(
    sources.map((source) => [source.name, new Set(source.columns.map((column) => column.name))]),
  );

  for (const [sourceName, sourceOverrides] of typeOverrides) {
    const sourceColumns = columnsBySource.get(sourceName);
    if (!sourceColumns) {
      throw new Error(
        `Invalid introspection config: type override references unknown source '${sourceName}'.`,
      );
    }

    for (const columnName of sourceOverrides.keys()) {
      if (!sourceColumns.has(columnName)) {
        throw new Error(
          `Invalid introspection config: type override references unknown column '${sourceName}.${columnName}'.`,
        );
      }
    }
  }
}

export function buildTypeScriptModuleResult(
  objects: readonly IntrospectionObject[],
  columns: readonly TypeScriptIntrospectionColumn[],
  _headerOptions?: IntrospectionSchemaHeaderOptions,
  dictionaries?: readonly { name: string; columns: readonly TypeScriptIntrospectionColumn[] }[],
  generatorOptions: {
    readonly imports?: readonly TypeScriptTypeImport[];
    readonly typeOverrides?: TypeScriptColumnTypeOverrides;
  } = {},
): IntrospectionResult {
  if (objects.length === 0 && (!dictionaries || dictionaries.length === 0)) {
    throw new Error("No tables, views, or dictionaries were available to introspect.");
  }

  const orderedObjects = [...objects].sort(sortObjects);
  const columnsByObject = new Map<string, TypeScriptIntrospectionColumn[]>();
  for (const column of columns) {
    const existing = columnsByObject.get(column.objectName);
    if (existing) {
      existing.push(column);
    } else {
      columnsByObject.set(column.objectName, [column]);
    }
  }

  const failures: IntrospectionFailure[] = [];
  const tables: Array<{ name: string; columns: readonly TypeScriptIntrospectionColumn[] }> = [];
  const views: Array<{ name: string; columns: readonly TypeScriptIntrospectionColumn[] }> = [];

  for (const object of orderedObjects) {
    const objectColumns = columnsByObject.get(object.name) ?? [];
    if (objectColumns.length === 0) {
      failures.push({
        name: object.name,
        engine: object.engine,
        message: "Could not load column metadata for the introspected object.",
      });
      continue;
    }

    if (isTableLikeObject(object)) {
      tables.push({ name: object.name, columns: objectColumns });
    } else {
      views.push({ name: object.name, columns: objectColumns });
    }
  }

  if (tables.length + views.length === 0 && (!dictionaries || dictionaries.length === 0)) {
    throw new Error(
      failures.length > 0
        ? formatIntrospectionFailureReport(
            failures,
            "Could not generate trusted TypeScript DB types for",
          )
        : "No supported tables, views, or dictionaries were available to introspect.",
    );
  }

  validateTypeOverrideTargets(
    [...tables, ...views, ...(dictionaries ?? [])],
    generatorOptions.typeOverrides,
  );

  return {
    source: tryFormatTypeScript(
      `${formatSchemaModuleHeader()}\n\n${generateTypeScriptSchemaModule(tables, views, dictionaries, generatorOptions)}`,
    ),
    failures,
    summary: {
      generatedObjects: tables.length + views.length + (dictionaries?.length ?? 0),
      generatedTables: tables.length,
      generatedViews: views.length,
      generatedDictionaries: dictionaries?.length ?? 0,
      skippedObjects: failures.length,
    },
  };
}

export async function fetchDatabaseObjects(
  options: IntrospectionConnectionOptions,
  tablesOnly = false,
): Promise<IntrospectionObject[]> {
  const client = createClient({
    url: options.url,
    username: options.user,
    password: options.password,
    request_timeout: 30_000,
  });

  try {
    const result = await client.query({
      query: `
        SELECT
          name,
          engine
        FROM system.tables
        WHERE database = {database:String}
          AND engine NOT IN ('Dictionary', 'MaterializedView')
          ${tablesOnly ? "AND engine != 'View'" : ""}
      `,
      query_params: {
        database: options.database,
      },
      format: "JSONEachRow",
    });

    const rows = await result.json<SystemTableRow>();
    return rows.map((row) => ({
      name: row.name,
      engine: row.engine,
    }));
  } finally {
    await client.close();
  }
}

export async function fetchDatabaseColumns(
  options: IntrospectionConnectionOptions,
): Promise<TypeScriptIntrospectionColumn[]> {
  const client = createClient({
    url: options.url,
    username: options.user,
    password: options.password,
    request_timeout: 30_000,
  });

  try {
    const result = await client.query({
      query: `
        SELECT
          table,
          name,
          type,
          position
        FROM system.columns
        WHERE database = {database:String}
        ORDER BY table ASC, position ASC
      `,
      query_params: {
        database: options.database,
      },
      format: "JSONEachRow",
    });

    const rows = await result.json<SystemColumnRow>();
    return rows.map((row) => ({
      objectName: row.table,
      name: row.name,
      clickhouseType: row.type,
      position: row.position,
    }));
  } finally {
    await client.close();
  }
}

export async function fetchDictionaryObjects(
  options: IntrospectionConnectionOptions,
): Promise<IntrospectionObject[]> {
  const client = createClient({
    url: options.url,
    username: options.user,
    password: options.password,
    request_timeout: 30_000,
  });

  try {
    const result = await client.query({
      query: `
        SELECT
          name,
          'Dictionary' AS engine
        FROM system.dictionaries
        WHERE database = {database:String}
      `,
      query_params: {
        database: options.database,
      },
      format: "JSONEachRow",
    });

    const rows = await result.json<SystemDictionaryRow>();
    return rows.map((row) => ({
      name: row.name,
      engine: "Dictionary",
    }));
  } finally {
    await client.close();
  }
}

export async function fetchDictionaryAttributes(
  options: IntrospectionConnectionOptions,
): Promise<TypeScriptIntrospectionColumn[]> {
  const client = createClient({
    url: options.url,
    username: options.user,
    password: options.password,
    request_timeout: 30_000,
  });

  async function tableExists(tableName: string): Promise<boolean> {
    try {
      const result = await client.query({
        query: `SELECT count() AS count FROM system.tables WHERE database = 'system' AND name = {tableName:String}`,
        query_params: { tableName },
        format: "JSONEachRow",
      });
      const rows = await result.json<{ count: string }>();
      return Number(rows[0]?.count ?? 0) > 0;
    } catch {
      return false;
    }
  }

  async function fetchFromDictionaryAttributes(): Promise<TypeScriptIntrospectionColumn[]> {
    const result = await client.query({
      query: `
        SELECT
          dictionary AS objectName,
          name,
          type AS clickhouseType,
          1 AS position
        FROM system.dictionary_attributes
        WHERE database = {database:String}
        ORDER BY dictionary ASC, name ASC
      `,
      query_params: { database: options.database },
      format: "JSONEachRow",
    });
    const rows = await result.json<SystemDictionaryAttributeRow>();
    return rows.map((row, index) => ({
      objectName: row.objectName,
      name: row.name,
      clickhouseType: row.clickhouseType,
      position: index + 1,
    }));
  }

  async function fetchFromDictionariesViaSubquery(): Promise<TypeScriptIntrospectionColumn[]> {
    const result = await client.query({
      query: `
        SELECT
          dict_name AS objectName,
          attr_name AS name,
          attr_type AS clickhouseType,
          1 AS position
        FROM (
          SELECT name AS dict_name, attribute.names, attribute.types
          FROM system.dictionaries
          WHERE database = {database:String}
        )
        ARRAY JOIN attribute.names AS attr_name, attribute.types AS attr_type
        ORDER BY objectName ASC, name ASC
      `,
      query_params: { database: options.database },
      format: "JSONEachRow",
    });
    interface SubqueryRow {
      readonly objectName: string;
      readonly name: string;
      readonly clickhouseType: string;
      readonly position: number;
    }
    const rows = await result.json<SubqueryRow>();
    return rows.map((row) => ({
      objectName: row.objectName,
      name: row.name,
      clickhouseType: row.clickhouseType,
      position: row.position,
    }));
  }

  try {
    if (await tableExists("dictionary_attributes")) {
      return await fetchFromDictionaryAttributes();
    }
    return await fetchFromDictionariesViaSubquery();
  } finally {
    await client.close();
  }
}

export async function writeSchemaModule(source: string, outPath: string): Promise<void> {
  await mkdir(dirname(outPath), { recursive: true });
  await writeFile(outPath, source, "utf8");
}

export async function introspectDatabase(args: IntrospectArgs): Promise<IntrospectionResult> {
  const resolvedArgs = await resolveIntrospectionArgs(args);
  const options = resolveConnectionOptions(resolvedArgs);
  const allObjects = await fetchDatabaseObjects(options, resolvedArgs.tablesOnly);
  const filteredObjects = filterExcludedObjects(
    allObjects,
    options.includePattern,
    options.excludePattern,
  );
  const objects = resolvedArgs.tablesOnly
    ? filteredObjects.filter(isTableLikeObject)
    : filteredObjects;

  let dictionaries:
    | Array<{ name: string; columns: readonly TypeScriptIntrospectionColumn[] }>
    | undefined;
  if (!resolvedArgs.tablesOnly) {
    const dictObjects = await fetchDictionaryObjects(options);
    const filteredDicts = filterExcludedObjects(
      dictObjects,
      options.includePattern,
      options.excludePattern,
    );
    const dictColumns = await fetchDictionaryAttributes(options);
    const columnsByDict = new Map<string, TypeScriptIntrospectionColumn[]>();
    for (const column of dictColumns) {
      // Handle both bare names and qualified names (e.g. "default.dict_name")
      let dictName = column.objectName;
      const parts = dictName.split(".");
      if (parts.length > 1 && !filteredDicts.some((d) => d.name === dictName)) {
        dictName = parts[parts.length - 1]!;
      }
      const existing = columnsByDict.get(dictName);
      if (existing) {
        existing.push(column);
      } else {
        columnsByDict.set(dictName, [column]);
      }
    }

    dictionaries = filteredDicts
      .map((dict) => ({
        name: dict.name,
        columns: columnsByDict.get(dict.name) ?? [],
      }))
      .filter((dict) => dict.columns.length > 0);
  }

  if (objects.length === 0 && (!dictionaries || dictionaries.length === 0)) {
    throw new Error(
      resolvedArgs.tablesOnly
        ? `No table-like objects found in database '${options.database}'.`
        : `No tables, views, or dictionaries found in database '${options.database}'.`,
    );
  }

  const columns = await fetchDatabaseColumns(options);
  return buildTypeScriptModuleResult(
    objects,
    columns,
    buildSchemaHeaderOptions(resolvedArgs, options),
    dictionaries,
    {
      imports: resolvedArgs.imports,
      typeOverrides: resolvedArgs.typeOverrides,
    },
  );
}
