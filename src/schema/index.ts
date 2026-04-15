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
}

export interface QuarryEngine {
  readonly name: string;
  readonly finalCapable: boolean;
}

export interface NormalizedSchemaColumn {
  readonly clickhouseType: string;
}

export type SchemaColumns = Record<string, QuarryColumn<any, any, any>>;

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

function createColumn<Select, Insert = Select, Where = Select>(
  clickhouseType: string,
): QuarryColumn<Select, Insert, Where> {
  return {
    __quarryColumn: true,
    clickhouseType,
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
  return createColumn(`Nullable(${inner.clickhouseType})`);
}

export function LowCardinality<Select, Insert, Where>(
  inner: QuarryColumn<Select, Insert, Where>,
): QuarryColumn<Select, Insert, Where> {
  return createColumn(`LowCardinality(${inner.clickhouseType})`);
}

export function Array<Select, Insert, Where>(
  inner: QuarryColumn<Select, Insert, Where>,
): QuarryColumn<Select[], Insert[], Where[]> {
  return createColumn(`Array(${inner.clickhouseType})`);
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
  mergeTree<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
  replacingMergeTree<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
  summingMergeTree<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
  aggregatingMergeTree<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
  collapsingMergeTree<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
  versionedCollapsingMergeTree<Columns extends SchemaColumns>(
    columns: Columns,
  ): QuarryTableSource<Columns>;
};

export const table: TableFactory = Object.assign(
  <Columns extends SchemaColumns>(columns: Columns) =>
    createTableSource(columns, { name: "Table", finalCapable: false }),
  {
    memory<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "Memory", finalCapable: false });
    },
    mergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "MergeTree", finalCapable: false });
    },
    replacingMergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "ReplacingMergeTree", finalCapable: true });
    },
    summingMergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "SummingMergeTree", finalCapable: true });
    },
    aggregatingMergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "AggregatingMergeTree", finalCapable: true });
    },
    collapsingMergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "CollapsingMergeTree", finalCapable: true });
    },
    versionedCollapsingMergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, {
        name: "VersionedCollapsingMergeTree",
        finalCapable: true,
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

export type SchemaLike = SchemaDefinition | SchemaBuilder<SchemaDefinition>;

export function defineSchema<const S extends BaseSchemaDefinition>(schema: S): SchemaBuilder<S> {
  return new SchemaBuilder(schema);
}

export function resolveSchemaDefinition(schema: SchemaLike): SchemaDefinition {
  return schema instanceof SchemaBuilder ? schema.toDefinition() : schema;
}

function normalizeColumns(columns: SchemaColumns): Record<string, NormalizedSchemaColumn> {
  return Object.fromEntries(
    Object.entries(columns).map(([name, column]) => [
      name,
      { clickhouseType: column.clickhouseType },
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
