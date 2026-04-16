import type { CteNode, SelectQueryNode } from "../ast/query";
import { createEmptySelectQueryNode } from "../ast/query";
import { parseSourceExpression, resolveSourceColumns } from "../query/helpers";
import { SelectQueryBuilder } from "../query/select-query-builder";
import { TableSourceBuilder } from "../query/source-builder";
import type { ScopeFromSourceExpression, SourceExpression } from "../query/types";
import type { DatabaseSchema, InferResult, Simplify, TableName } from "../type-utils";

export interface QuarryColumn<Select, Insert = Select, Where = Select> {
  readonly __quarryColumn: true;
  readonly __selectType?: Select;
  readonly __insertType?: Insert;
  readonly __whereType?: Where;
  readonly clickhouseType: string;
  readonly codecs?: NonEmptyList<string>;
  codec(codecs: NonEmptyList<string>): QuarryColumn<Select, Insert, Where>;
}

export type SchemaColumns = Record<string, QuarryColumn<any, any, any>>;

type ColumnName<Columns extends SchemaColumns> = Extract<keyof Columns, string>;
type NonEmptyList<Value> = readonly [Value, ...Value[]];

export type QuarrySettingValue = string | number | boolean;

export interface MergeTreeTableOptions<Columns extends SchemaColumns> {
  readonly primaryKey?: NonEmptyList<ColumnName<Columns>>;
  readonly orderBy?: NonEmptyList<ColumnName<Columns>>;
  readonly partitionBy?: NonEmptyList<string>;
  readonly ttl?: NonEmptyList<string>;
  readonly settings?: Readonly<Record<string, QuarrySettingValue>>;
}

export interface SharedMergeTreeOptions<
  Columns extends SchemaColumns,
> extends MergeTreeTableOptions<Columns> {}

type ReplacingMergeTreeQuirks<Columns extends SchemaColumns> =
  | {
      readonly versionBy?: undefined;
      readonly isDeletedBy?: undefined;
    }
  | {
      readonly versionBy: ColumnName<Columns>;
      readonly isDeletedBy?: ColumnName<Columns>;
    };

export type ReplacingMergeTreeOptions<Columns extends SchemaColumns> =
  MergeTreeTableOptions<Columns> & ReplacingMergeTreeQuirks<Columns>;

export type SharedReplacingMergeTreeOptions<Columns extends SchemaColumns> =
  MergeTreeTableOptions<Columns> & ReplacingMergeTreeQuirks<Columns>;

export type SummingMergeTreeOptions<Columns extends SchemaColumns> =
  MergeTreeTableOptions<Columns> & {
    readonly sumColumns?: NonEmptyList<ColumnName<Columns>>;
  };

export interface AggregatingMergeTreeOptions<
  Columns extends SchemaColumns,
> extends MergeTreeTableOptions<Columns> {}

export type CollapsingMergeTreeOptions<Columns extends SchemaColumns> =
  MergeTreeTableOptions<Columns> & {
    readonly signBy: ColumnName<Columns>;
  };

export type VersionedCollapsingMergeTreeOptions<Columns extends SchemaColumns> =
  MergeTreeTableOptions<Columns> & {
    readonly signBy: ColumnName<Columns>;
    readonly versionBy: ColumnName<Columns>;
  };

interface StoredMergeTreeTableOptions {
  readonly primaryKey?: NonEmptyList<string>;
  readonly orderBy?: NonEmptyList<string>;
  readonly partitionBy?: NonEmptyList<string>;
  readonly ttl?: NonEmptyList<string>;
  readonly settings?: Readonly<Record<string, QuarrySettingValue>>;
}

interface StoredSharedMergeTreeOptions extends StoredMergeTreeTableOptions {}

type StoredReplacingMergeTreeQuirks =
  | {
      readonly versionBy?: undefined;
      readonly isDeletedBy?: undefined;
    }
  | {
      readonly versionBy: string;
      readonly isDeletedBy?: string;
    };

type StoredReplacingMergeTreeOptions = StoredMergeTreeTableOptions & StoredReplacingMergeTreeQuirks;

type StoredSharedReplacingMergeTreeOptions = StoredMergeTreeTableOptions &
  StoredReplacingMergeTreeQuirks;

type StoredSummingMergeTreeOptions = StoredMergeTreeTableOptions & {
  readonly sumColumns?: NonEmptyList<string>;
};

interface StoredAggregatingMergeTreeOptions extends StoredMergeTreeTableOptions {}

type StoredCollapsingMergeTreeOptions = StoredMergeTreeTableOptions & {
  readonly signBy: string;
};

type StoredVersionedCollapsingMergeTreeOptions = StoredMergeTreeTableOptions & {
  readonly signBy: string;
  readonly versionBy: string;
};

type QuarryNonFinalEngine<Name extends string> = {
  readonly name: Name;
  readonly finalCapable: false;
};

type QuarryFinalEngine<Name extends string> = {
  readonly name: Name;
  readonly finalCapable: true;
};

type QuarryConfiguredNonFinalEngine<Name extends string, Options> = QuarryNonFinalEngine<Name> & {
  readonly options?: Options;
};

type QuarryConfiguredFinalEngine<Name extends string, Options> = QuarryFinalEngine<Name> & {
  readonly options?: Options;
};

export type QuarryEngine =
  | QuarryNonFinalEngine<"Table">
  | QuarryNonFinalEngine<"Memory">
  | QuarryConfiguredNonFinalEngine<"MergeTree", StoredMergeTreeTableOptions>
  | QuarryConfiguredNonFinalEngine<"SharedMergeTree", StoredSharedMergeTreeOptions>
  | QuarryConfiguredFinalEngine<"ReplacingMergeTree", StoredReplacingMergeTreeOptions>
  | QuarryConfiguredFinalEngine<"SharedReplacingMergeTree", StoredSharedReplacingMergeTreeOptions>
  | QuarryConfiguredFinalEngine<"SummingMergeTree", StoredSummingMergeTreeOptions>
  | QuarryConfiguredFinalEngine<"AggregatingMergeTree", StoredAggregatingMergeTreeOptions>
  | QuarryConfiguredFinalEngine<"CollapsingMergeTree", StoredCollapsingMergeTreeOptions>
  | QuarryConfiguredFinalEngine<
      "VersionedCollapsingMergeTree",
      StoredVersionedCollapsingMergeTreeOptions
    >;

export interface NormalizedSchemaColumn {
  readonly clickhouseType: string;
  readonly codecs?: NonEmptyList<string>;
}

export interface QuarryTableSource<Columns extends SchemaColumns> {
  readonly __quarrySource: true;
  readonly kind: "table";
  readonly columns: Columns;
  readonly engine: QuarryEngine;
}

export interface QuarryQueryViewSource<
  Query extends SelectQueryBuilder<any, any, any, any>,
  Columns extends SchemaColumns,
> {
  readonly __quarrySource: true;
  readonly kind: "view";
  readonly query: Query;
  readonly __columns?: Columns;
}

export type QuarrySource =
  | QuarryTableSource<SchemaColumns>
  | QuarryQueryViewSource<SelectQueryBuilder<any, any, any, any>, SchemaColumns>;

export type SchemaDefinition = Record<string, QuarrySource>;
export type BaseSchemaDefinition = Record<string, QuarryTableSource<SchemaColumns>>;

type InferQueryColumns<Query> =
  Query extends SelectQueryBuilder<any, any, any, infer Columns>
    ? Columns extends SchemaColumns
      ? Columns
      : never
    : never;

type QueryViewDefinitions = Record<
  string,
  QuarryQueryViewSource<SelectQueryBuilder<any, any, any, any>, SchemaColumns>
>;

interface QuarryColumnMetadata {
  readonly codecs?: NonEmptyList<string>;
}

function normalizeColumnCodecs(codecs: readonly string[]): NonEmptyList<string> {
  if (codecs.length === 0) {
    throw new Error("codec() must include at least one codec.");
  }

  const normalized = codecs.map((codec) => codec.trim());

  for (const codec of normalized) {
    if (codec.length === 0) {
      throw new Error("codec() entries must be non-empty strings.");
    }
  }

  return normalized as unknown as NonEmptyList<string>;
}

function createColumn<Select, Insert = Select, Where = Select>(
  clickhouseType: string,
  metadata: QuarryColumnMetadata = {},
): QuarryColumn<Select, Insert, Where> {
  return {
    __quarryColumn: true,
    clickhouseType,
    ...(metadata.codecs ? { codecs: metadata.codecs } : {}),
    codec(codecs: NonEmptyList<string>) {
      return createColumn<Select, Insert, Where>(clickhouseType, {
        ...metadata,
        codecs: normalizeColumnCodecs(codecs),
      });
    },
  };
}

export function String(): QuarryColumn<string> {
  return createColumn("String");
}

export function Bool(): QuarryColumn<boolean> {
  return createColumn("Bool");
}

export function UInt8(): QuarryColumn<number> {
  return createColumn("UInt8");
}

export function UInt16(): QuarryColumn<number> {
  return createColumn("UInt16");
}

export function UInt32(): QuarryColumn<number> {
  return createColumn("UInt32");
}

export function UInt64(): QuarryColumn<string, string | number | bigint, string | number | bigint> {
  return createColumn("UInt64");
}

export function Int32(): QuarryColumn<number> {
  return createColumn("Int32");
}

export function Int8(): QuarryColumn<number> {
  return createColumn("Int8");
}

export function Int16(): QuarryColumn<number> {
  return createColumn("Int16");
}

export function Int64(): QuarryColumn<string, string | number | bigint, string | number | bigint> {
  return createColumn("Int64");
}

export function Float32(): QuarryColumn<number> {
  return createColumn("Float32");
}

export function Float64(): QuarryColumn<number> {
  return createColumn("Float64");
}

export function Date(): QuarryColumn<string, string | globalThis.Date, string | globalThis.Date> {
  return createColumn("Date");
}

export function Date32(): QuarryColumn<string, string | globalThis.Date, string | globalThis.Date> {
  return createColumn("Date32");
}

export function DateTime(): QuarryColumn<
  string,
  string | globalThis.Date,
  string | globalThis.Date
> {
  return createColumn("DateTime");
}

export function DateTime64(
  precision = 3,
): QuarryColumn<string, string | globalThis.Date, string | globalThis.Date> {
  return createColumn(`DateTime64(${precision})`);
}

export function Nullable<Select, Insert, Where>(
  inner: QuarryColumn<Select, Insert, Where>,
): QuarryColumn<Select | null, Insert | null, Where | null> {
  return createColumn(`Nullable(${inner.clickhouseType})`, {
    codecs: inner.codecs,
  });
}

export function LowCardinality<Select, Insert, Where>(
  inner: QuarryColumn<Select, Insert, Where>,
): QuarryColumn<Select, Insert, Where> {
  return createColumn(`LowCardinality(${inner.clickhouseType})`, {
    codecs: inner.codecs,
  });
}

export function Array<Select, Insert, Where>(
  inner: QuarryColumn<Select, Insert, Where>,
): QuarryColumn<Select[], Insert[], Where[]> {
  return createColumn(`Array(${inner.clickhouseType})`, {
    codecs: inner.codecs,
  });
}

export function FixedString(length: number): QuarryColumn<string> {
  return createColumn(`FixedString(${length})`);
}

export function UUID(): QuarryColumn<string> {
  return createColumn("UUID");
}

export function IPv4(): QuarryColumn<string> {
  return createColumn("IPv4");
}

export function IPv6(): QuarryColumn<string> {
  return createColumn("IPv6");
}

function createTableSource<Columns extends SchemaColumns>(
  columns: Columns,
  engine: QuarryEngine,
): QuarryTableSource<Columns> {
  return {
    __quarrySource: true,
    kind: "table",
    columns,
    engine,
  };
}

function assertNonEmptyList(values: readonly string[] | undefined, context: string): void {
  if (values && values.length === 0) {
    throw new Error(`${context} must include at least one value.`);
  }
}

function assertKnownColumn<Columns extends SchemaColumns>(
  columns: Columns,
  column: string,
  context: string,
): void {
  if (!(column in columns)) {
    throw new Error(`Unknown column '${column}' in ${context}.`);
  }
}

function assertKnownColumns<Columns extends SchemaColumns>(
  columns: Columns,
  columnNames: readonly string[] | undefined,
  context: string,
): void {
  assertNonEmptyList(columnNames, context);

  for (const columnName of columnNames ?? []) {
    assertKnownColumn(columns, columnName, context);
  }
}

function validateMergeTreeOptions<Columns extends SchemaColumns>(
  columns: Columns,
  options: MergeTreeTableOptions<Columns> | undefined,
  engineName: string,
): void {
  if (!options) {
    return;
  }

  assertKnownColumns(columns, options.primaryKey, `${engineName}.primaryKey`);
  assertKnownColumns(columns, options.orderBy, `${engineName}.orderBy`);
  assertNonEmptyList(options.partitionBy, `${engineName}.partitionBy`);
  assertNonEmptyList(options.ttl, `${engineName}.ttl`);
}

function validateReplacingMergeTreeOptions<Columns extends SchemaColumns>(
  columns: Columns,
  options:
    | ReplacingMergeTreeOptions<Columns>
    | SharedReplacingMergeTreeOptions<Columns>
    | undefined,
  engineName: string,
): void {
  validateMergeTreeOptions(columns, options, engineName);

  if (!options) {
    return;
  }

  if (options.isDeletedBy && !options.versionBy) {
    throw new Error(`${engineName}.isDeletedBy requires ${engineName}.versionBy.`);
  }

  if (options.versionBy) {
    assertKnownColumn(columns, options.versionBy, `${engineName}.versionBy`);
  }

  if (options.isDeletedBy) {
    assertKnownColumn(columns, options.isDeletedBy, `${engineName}.isDeletedBy`);
  }
}

function validateSummingMergeTreeOptions<Columns extends SchemaColumns>(
  columns: Columns,
  options: SummingMergeTreeOptions<Columns> | undefined,
  engineName: string,
): void {
  validateMergeTreeOptions(columns, options, engineName);

  if (!options) {
    return;
  }

  assertKnownColumns(columns, options.sumColumns, `${engineName}.sumColumns`);
}

function validateCollapsingMergeTreeOptions<Columns extends SchemaColumns>(
  columns: Columns,
  options: CollapsingMergeTreeOptions<Columns> | undefined,
  engineName: string,
): void {
  validateMergeTreeOptions(columns, options, engineName);

  if (!options) {
    return;
  }

  assertKnownColumn(columns, options.signBy, `${engineName}.signBy`);
}

function validateVersionedCollapsingMergeTreeOptions<Columns extends SchemaColumns>(
  columns: Columns,
  options: VersionedCollapsingMergeTreeOptions<Columns> | undefined,
  engineName: string,
): void {
  validateMergeTreeOptions(columns, options, engineName);

  if (!options) {
    return;
  }

  assertKnownColumn(columns, options.signBy, `${engineName}.signBy`);
  assertKnownColumn(columns, options.versionBy, `${engineName}.versionBy`);
}

function createQueryViewSource<
  Query extends SelectQueryBuilder<any, any, any, any>,
  Columns extends SchemaColumns,
>(query: Query): QuarryQueryViewSource<Query, Columns> {
  return {
    __quarrySource: true,
    kind: "view",
    query,
  } as QuarryQueryViewSource<Query, Columns>;
}

type TableFactory = (<Columns extends SchemaColumns>(
  columns: Columns,
) => QuarryTableSource<Columns>) & {
  memory<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
  mergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options?: MergeTreeTableOptions<Columns>,
  ): QuarryTableSource<Columns>;
  sharedMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options?: SharedMergeTreeOptions<Columns>,
  ): QuarryTableSource<Columns>;
  replacingMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options?: ReplacingMergeTreeOptions<Columns>,
  ): QuarryTableSource<Columns>;
  sharedReplacingMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options?: SharedReplacingMergeTreeOptions<Columns>,
  ): QuarryTableSource<Columns>;
  summingMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options?: SummingMergeTreeOptions<Columns>,
  ): QuarryTableSource<Columns>;
  aggregatingMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options?: AggregatingMergeTreeOptions<Columns>,
  ): QuarryTableSource<Columns>;
  collapsingMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options: CollapsingMergeTreeOptions<Columns>,
  ): QuarryTableSource<Columns>;
  versionedCollapsingMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
    options: VersionedCollapsingMergeTreeOptions<Columns>,
  ): QuarryTableSource<Columns>;
};

export const table: TableFactory = Object.assign(
  <Columns extends SchemaColumns>(columns: Columns) =>
    createTableSource(columns, { name: "Table", finalCapable: false }),
  {
    memory<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "Memory", finalCapable: false });
    },
    mergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options?: MergeTreeTableOptions<Columns>,
    ) {
      validateMergeTreeOptions(columns, options, "mergeTree");
      return createTableSource(
        columns,
        options
          ? { name: "MergeTree", finalCapable: false, options }
          : { name: "MergeTree", finalCapable: false },
      );
    },
    sharedMergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options?: SharedMergeTreeOptions<Columns>,
    ) {
      validateMergeTreeOptions(columns, options, "sharedMergeTree");
      return createTableSource(
        columns,
        options
          ? { name: "SharedMergeTree", finalCapable: false, options }
          : { name: "SharedMergeTree", finalCapable: false },
      );
    },
    replacingMergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options?: ReplacingMergeTreeOptions<Columns>,
    ) {
      validateReplacingMergeTreeOptions(columns, options, "replacingMergeTree");
      return createTableSource(
        columns,
        options
          ? { name: "ReplacingMergeTree", finalCapable: true, options }
          : { name: "ReplacingMergeTree", finalCapable: true },
      );
    },
    sharedReplacingMergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options?: SharedReplacingMergeTreeOptions<Columns>,
    ) {
      validateReplacingMergeTreeOptions(columns, options, "sharedReplacingMergeTree");
      return createTableSource(
        columns,
        options
          ? { name: "SharedReplacingMergeTree", finalCapable: true, options }
          : { name: "SharedReplacingMergeTree", finalCapable: true },
      );
    },
    summingMergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options?: SummingMergeTreeOptions<Columns>,
    ) {
      validateSummingMergeTreeOptions(columns, options, "summingMergeTree");
      return createTableSource(
        columns,
        options
          ? { name: "SummingMergeTree", finalCapable: true, options }
          : { name: "SummingMergeTree", finalCapable: true },
      );
    },
    aggregatingMergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options?: AggregatingMergeTreeOptions<Columns>,
    ) {
      validateMergeTreeOptions(columns, options, "aggregatingMergeTree");
      return createTableSource(
        columns,
        options
          ? { name: "AggregatingMergeTree", finalCapable: true, options }
          : { name: "AggregatingMergeTree", finalCapable: true },
      );
    },
    collapsingMergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options: CollapsingMergeTreeOptions<Columns>,
    ) {
      validateCollapsingMergeTreeOptions(columns, options, "collapsingMergeTree");
      return createTableSource(columns, {
        name: "CollapsingMergeTree",
        finalCapable: true,
        options,
      });
    },
    versionedCollapsingMergeTree<Columns extends SchemaColumns>(
      columns: Columns,
      options: VersionedCollapsingMergeTreeOptions<Columns>,
    ) {
      validateVersionedCollapsingMergeTreeOptions(columns, options, "versionedCollapsingMergeTree");
      return createTableSource(columns, {
        name: "VersionedCollapsingMergeTree",
        finalCapable: true,
        options,
      });
    },
  },
);

type ViewFactory = {
  as<Query extends SelectQueryBuilder<any, any, any, any>>(
    query: Query,
  ): QuarryQueryViewSource<Query, InferQueryColumns<Query>>;
};

export const view: ViewFactory = {
  as<Query extends SelectQueryBuilder<any, any, any, any>>(query: Query) {
    return createQueryViewSource<Query, InferQueryColumns<Query>>(query);
  },
};

export interface NormalizedSchemaSource {
  readonly kind: "table" | "view";
  readonly insertable: boolean;
  readonly finalCapable: boolean;
  readonly columns: Record<string, NormalizedSchemaColumn>;
  readonly engine?: QuarryEngine;
  readonly query?: SelectQueryNode;
}

export type NormalizedSchema = Record<string, NormalizedSchemaSource>;

export class SchemaViewDB<DB extends DatabaseSchema, Sources extends DatabaseSchema = DB> {
  constructor(
    private readonly schema: NormalizedSchema,
    private readonly withs: CteNode[] = [],
  ) {}

  table<Table extends TableName<DB>>(table: Table): TableSourceBuilder<DB, Table> {
    return new TableSourceBuilder<DB, Table>(table, undefined, false, this.schema[table]);
  }

  with<Name extends string, Query extends SelectQueryBuilder<any, any, any, any>>(
    name: Name,
    callback: (db: SchemaViewDB<DB, Sources>) => Query,
  ): SchemaViewDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>> {
    const query = callback(new SchemaViewDB<DB, Sources>(this.schema));

    return new SchemaViewDB<DB, Simplify<Sources & { [K in Name]: InferResult<Query> }>>(
      this.schema,
      [...this.withs, { name, query: query.toAST() }],
    );
  }

  selectFrom<Source extends SourceExpression<Sources>>(
    source: Source,
  ): SelectQueryBuilder<Sources, ScopeFromSourceExpression<Sources, Source>, {}, {}> {
    const node = createEmptySelectQueryNode();
    node.with = structuredClone(this.withs);
    node.from = parseSourceExpression(source);

    const resolvedSource = resolveSourceColumns(source, this.schema);
    const scopeColumns = resolvedSource
      ? { [resolvedSource.alias]: resolvedSource.columns }
      : undefined;

    return new SelectQueryBuilder(node, undefined, this.schema, scopeColumns);
  }
}

export class SchemaBuilder<Sources extends SchemaDefinition> {
  constructor(readonly definition: Sources) {}

  views<const Views extends QueryViewDefinitions>(
    callback: (db: SchemaViewDB<Sources>) => Views,
  ): SchemaBuilder<Simplify<Sources & Views>> {
    const db = new SchemaViewDB<Sources>(normalizeSchema(this.definition));
    const views = callback(db);

    return new SchemaBuilder({
      ...this.definition,
      ...views,
    } as Simplify<Sources & Views>);
  }

  toDefinition(): Sources {
    return this.definition;
  }
}

type ResolvableSchemaBuilder = Pick<SchemaBuilder<SchemaDefinition>, "toDefinition">;

export type SchemaLike = SchemaDefinition | ResolvableSchemaBuilder;

export function defineSchema<const S extends BaseSchemaDefinition>(schema: S): SchemaBuilder<S> {
  return new SchemaBuilder(schema);
}

function isResolvableSchemaBuilder(schema: SchemaLike): schema is ResolvableSchemaBuilder {
  return typeof (schema as ResolvableSchemaBuilder).toDefinition === "function";
}

export function resolveSchemaDefinition(schema: SchemaLike): SchemaDefinition {
  return isResolvableSchemaBuilder(schema) ? schema.toDefinition() : schema;
}

function normalizeColumns(columns: SchemaColumns): Record<string, NormalizedSchemaColumn> {
  return Object.fromEntries(
    Object.entries(columns).map(([name, column]) => [
      name,
      {
        clickhouseType: column.clickhouseType,
        ...(column.codecs ? { codecs: column.codecs } : {}),
      },
    ]),
  );
}

export function normalizeSchema(schemaLike: SchemaLike): NormalizedSchema {
  const schema = resolveSchemaDefinition(schemaLike);
  const normalized: NormalizedSchema = {};

  for (const [name, source] of Object.entries(schema)) {
    if (source.kind === "table") {
      normalized[name] = {
        kind: "table",
        insertable: true,
        finalCapable: source.engine.finalCapable,
        columns: normalizeColumns(source.columns),
        engine: source.engine,
      };
      continue;
    }

    const columns = source.query.getOutputColumns();
    if (!columns || Object.keys(columns).length === 0) {
      throw new Error(
        `Unable to infer selectable columns for view '${name}'. Ensure the view query only selects expressions with known schema metadata.`,
      );
    }

    normalized[name] = {
      kind: "view",
      insertable: false,
      finalCapable: false,
      columns,
      query: source.query.toAST(),
    };
  }

  return normalized;
}

function padNumber(value: number, width = 2): string {
  return globalThis.String(value).padStart(width, "0");
}

function formatDateValue(value: Date): string {
  return `${value.getUTCFullYear()}-${padNumber(value.getUTCMonth() + 1)}-${padNumber(value.getUTCDate())}`;
}

function formatDateTimeValue(value: Date, precision = 0): string {
  const base = `${formatDateValue(value)} ${padNumber(value.getUTCHours())}:${padNumber(value.getUTCMinutes())}:${padNumber(value.getUTCSeconds())}`;

  if (precision <= 0) {
    return base;
  }

  const milliseconds = padNumber(value.getUTCMilliseconds(), 3);
  const fractional =
    precision <= 3 ? milliseconds.slice(0, precision) : milliseconds.padEnd(precision, "0");

  return `${base}.${fractional}`;
}

export function normalizeClickHouseInputValue(value: unknown, clickhouseType: string): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  if (clickhouseType.startsWith("LowCardinality(") && clickhouseType.endsWith(")")) {
    return normalizeClickHouseInputValue(value, clickhouseType.slice("LowCardinality(".length, -1));
  }

  if (clickhouseType.startsWith("Nullable(") && clickhouseType.endsWith(")")) {
    return normalizeClickHouseInputValue(value, clickhouseType.slice("Nullable(".length, -1));
  }

  if (clickhouseType.startsWith("Array(") && clickhouseType.endsWith(")")) {
    const memberType = clickhouseType.slice("Array(".length, -1);
    return globalThis.Array.isArray(value)
      ? value.map((member) => normalizeClickHouseInputValue(member, memberType))
      : value;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof globalThis.Date) {
    if (clickhouseType === "Date") {
      return formatDateValue(value);
    }

    if (clickhouseType === "Date32") {
      return formatDateValue(value);
    }

    if (clickhouseType === "DateTime") {
      return formatDateTimeValue(value);
    }

    const dateTime64Match = /^DateTime64\((\d+)\)$/.exec(clickhouseType);
    if (dateTime64Match) {
      return formatDateTimeValue(value, Number(dateTime64Match[1]));
    }
  }

  return value;
}

export function normalizeInsertRow<Row extends object>(
  row: Row,
  source: NormalizedSchemaSource | undefined,
): Row {
  if (!source) {
    return row;
  }

  const normalizedEntries = Object.entries(row).map(([key, value]) => {
    const column = source.columns[key];
    return [key, column ? normalizeClickHouseInputValue(value, column.clickhouseType) : value];
  });

  return Object.fromEntries(normalizedEntries) as Row;
}
