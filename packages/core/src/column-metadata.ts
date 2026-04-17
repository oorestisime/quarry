import type { ColumnType } from "./db-types";

export interface QueryColumn<Select = unknown, Where = Select> extends ColumnType<
  Select,
  Select,
  Where
> {
  readonly clickhouseType: string;
}

export type QueryColumnMap = Record<string, QueryColumn<any, any>>;
