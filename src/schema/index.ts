export interface QuarryColumn<Select, Insert = Select, Where = Select> {
  readonly __quarryColumn: true;
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

export interface QuarryViewSource<Columns extends SchemaColumns> {
  readonly __quarrySource: true;
  readonly kind: "view";
  readonly columns: Columns;
}

export interface QuarryDerivedViewSource<From extends string> {
  readonly __quarrySource: true;
  readonly kind: "view";
  readonly deriveFrom: From;
  readonly useFinal: boolean;
  final(): QuarryDerivedViewSource<From>;
}

export type QuarrySource =
  | QuarryTableSource<SchemaColumns>
  | QuarryViewSource<SchemaColumns>
  | QuarryDerivedViewSource<string>;

export type SchemaDefinition = Record<string, QuarrySource>;

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

export function UInt32(): QuarryColumn<number> {
  return createColumn("UInt32");
}

export function UInt64(): QuarryColumn<string, string | number | bigint, string | number | bigint> {
  return createColumn("UInt64");
}

export function Int64(): QuarryColumn<string, string | number | bigint, string | number | bigint> {
  return createColumn("Int64");
}

export function Float64(): QuarryColumn<number> {
  return createColumn("Float64");
}

export function Date(): QuarryColumn<string, string | globalThis.Date, string | globalThis.Date> {
  return createColumn("Date");
}

export function DateTime(): QuarryColumn<string, string | globalThis.Date, string | globalThis.Date> {
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

export function Array<Select, Insert, Where>(
  inner: QuarryColumn<Select, Insert, Where>,
): QuarryColumn<Select[], Insert[], Where[]> {
  return createColumn(`Array(${inner.clickhouseType})`);
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

function createViewSource<Columns extends SchemaColumns>(columns: Columns): QuarryViewSource<Columns> {
  return {
    __quarrySource: true,
    kind: "view",
    columns,
  };
}

function createDerivedViewSource<From extends string>(
  deriveFrom: From,
  useFinal = false,
): QuarryDerivedViewSource<From> {
  return {
    __quarrySource: true,
    kind: "view",
    deriveFrom,
    useFinal,
    final() {
      return createDerivedViewSource(deriveFrom, true);
    },
  };
}

type TableFactory = (<Columns extends SchemaColumns>(columns: Columns) => QuarryTableSource<Columns>) & {
  mergeTree<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
  replacingMergeTree<Columns extends SchemaColumns>(columns: Columns): QuarryTableSource<Columns>;
};

export const table: TableFactory = Object.assign(
  <Columns extends SchemaColumns>(columns: Columns) =>
    createTableSource(columns, { name: "Table", finalCapable: false }),
  {
    mergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "MergeTree", finalCapable: false });
    },
    replacingMergeTree<Columns extends SchemaColumns>(columns: Columns) {
      return createTableSource(columns, { name: "ReplacingMergeTree", finalCapable: true });
    },
  },
);

type ViewFactory = (<Columns extends SchemaColumns>(columns: Columns) => QuarryViewSource<Columns>) & {
  from<From extends string>(source: From): QuarryDerivedViewSource<From>;
};

export const view: ViewFactory = Object.assign(
  <Columns extends SchemaColumns>(columns: Columns) => createViewSource(columns),
  {
    from<From extends string>(source: From) {
      return createDerivedViewSource(source);
    },
  },
);

type ValidateSchema<S extends SchemaDefinition> = {
  [K in keyof S]: S[K] extends QuarryDerivedViewSource<infer From>
    ? From extends Extract<keyof S, string>
      ? S[K]
      : never
    : S[K];
};

export function defineSchema<const S extends SchemaDefinition>(schema: S & ValidateSchema<S>): S {
  return schema;
}

export interface NormalizedSchemaSource {
  readonly kind: "table" | "view";
  readonly insertable: boolean;
  readonly finalCapable: boolean;
  readonly columns: Record<string, NormalizedSchemaColumn>;
}

export type NormalizedSchema = Record<string, NormalizedSchemaSource>;

function normalizeColumns(columns: SchemaColumns): Record<string, NormalizedSchemaColumn> {
  return Object.fromEntries(
    Object.entries(columns).map(([name, column]) => [name, { clickhouseType: column.clickhouseType }]),
  );
}

function resolveColumns(
  schema: SchemaDefinition,
  source: QuarrySource,
  path: string[] = [],
): Record<string, NormalizedSchemaColumn> {
  if ("columns" in source) {
    return normalizeColumns(source.columns);
  }

  if (!(source.deriveFrom in schema)) {
    throw new Error(`Derived view references unknown source '${source.deriveFrom}'.`);
  }

  if (path.includes(source.deriveFrom)) {
    throw new Error(`Derived view cycle detected: ${[...path, source.deriveFrom].join(" -> ")}.`);
  }

  return resolveColumns(schema, schema[source.deriveFrom], [...path, source.deriveFrom]);
}

export function normalizeSchema(schema: SchemaDefinition): NormalizedSchema {
  const normalized: NormalizedSchema = {};

  for (const [name, source] of Object.entries(schema)) {
    if (source.kind === "table") {
      normalized[name] = {
        kind: "table",
        insertable: true,
        finalCapable: source.engine.finalCapable,
        columns: resolveColumns(schema, source, [name]),
      };
      continue;
    }

    if ("deriveFrom" in source && !(source.deriveFrom in schema)) {
      throw new Error(`Derived view '${name}' references unknown source '${source.deriveFrom}'.`);
    }

    normalized[name] = {
      kind: "view",
      insertable: false,
      finalCapable: false,
      columns: resolveColumns(schema, source, [name]),
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
