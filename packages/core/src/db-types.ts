export interface ColumnType<Select, Insert = Select, Where = Insert> {
  readonly __columnTypeSelect?: Select;
  readonly __columnTypeInsert?: Insert;
  readonly __columnTypeWhere?: Where;
}

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
