export interface ColumnType<Select, Insert = Select, Where = Insert> {
  readonly __columnTypeSelect?: Select;
  readonly __columnTypeInsert?: Insert;
  readonly __columnTypeWhere?: Where;
}

/** A DEFAULT column may be omitted on insert while remaining required on read. */
export type Generated<T> =
  T extends ColumnType<infer Select, infer Insert, infer Where>
    ? ColumnType<Select, Insert | undefined, Where>
    : ColumnType<T, T | undefined, T>;

/** MATERIALIZED and ALIAS columns are selectable but cannot be inserted. */
export type GeneratedAlways<T> =
  T extends ColumnType<infer Select, any, infer Where>
    ? ColumnType<Select, never, Where>
    : ColumnType<T, never, T>;

export interface TypedTable<Row extends object> {
  readonly __sourceKind?: "table";
  readonly __sourceRow?: Row;
}

export interface TypedView<Row extends object> {
  readonly __sourceKind?: "view";
  readonly __sourceRow?: Row;
}

export interface TypedDictionary<Row extends object> {
  readonly __sourceKind?: "dictionary";
  readonly __sourceRow?: Row;
}

export type ClickHouseDate = ColumnType<string, string, string>;

export type ClickHouseDate32 = ColumnType<string, string, string>;

export type ClickHouseDateTime = ColumnType<
  string,
  string | globalThis.Date,
  string | globalThis.Date
>;

export type ClickHouseDateTime64 = ColumnType<
  string,
  string | globalThis.Date,
  string | globalThis.Date
>;

export type ClickHouseUInt64 = ColumnType<
  string,
  string | number | bigint,
  string | number | bigint
>;

export type ClickHouseInt64 = ColumnType<
  string,
  string | number | bigint,
  string | number | bigint
>;

export type ClickHouseDecimal = ColumnType<number, number | string, number | string>;
