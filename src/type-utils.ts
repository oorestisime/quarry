import type {
  QuarryColumn,
  QuarryDerivedViewSource,
  QuarryTableSource,
  QuarryViewSource,
} from "./schema";

export type DatabaseSchema = object;
export type ScopeMap = Record<string, Record<string, unknown>>;

export type Simplify<T> = { [K in keyof T]: T[K] } & {};

export type UnionToIntersection<T> = (T extends unknown ? (value: T) => void : never) extends (
  value: infer R,
) => void
  ? R
  : never;

export type InferResult<T> = T extends { readonly __resultType: infer Output } ? Output : never;

export type SourceName<DB extends DatabaseSchema> = Extract<keyof DB, string>;

export type SelectValue<T> = T extends QuarryColumn<infer Select, any, any> ? Select : T;
export type InsertValue<T> = T extends QuarryColumn<any, infer Insert, any> ? Insert : T;
export type WhereValue<T> = T extends QuarryColumn<any, any, infer Where> ? Where : T;

type SourceColumns<DB extends DatabaseSchema, Source> = Source extends QuarryTableSource<infer Columns>
  ? Columns
  : Source extends QuarryViewSource<infer Columns>
    ? Columns
    : Source extends QuarryDerivedViewSource<infer From>
      ? From extends SourceName<DB>
        ? SourceColumns<DB, DB[From]>
        : never
      : Source extends object
        ? { [K in Extract<keyof Source, string>]: Source[K] }
        : never;

export type TableName<DB extends DatabaseSchema> = Extract<
  {
    [K in SourceName<DB>]: DB[K] extends QuarryViewSource<any> | QuarryDerivedViewSource<any>
      ? never
      : K;
  }[SourceName<DB>],
  string
>;

export type SelectableSourceName<DB extends DatabaseSchema> = SourceName<DB>;

export type InsertableSourceName<DB extends DatabaseSchema> = Extract<
  {
    [K in SourceName<DB>]: DB[K] extends QuarryViewSource<any> | QuarryDerivedViewSource<any>
      ? never
      : K;
  }[SourceName<DB>],
  string
>;

export type TableRow<
  DB extends DatabaseSchema,
  Table extends SelectableSourceName<DB>,
> = SourceColumns<DB, DB[Table]> extends infer Row extends object
  ? { [K in Extract<keyof Row, string>]: SelectValue<Row[K]> }
  : never;

export type ScopeRow<
  DB extends DatabaseSchema,
  Table extends SelectableSourceName<DB>,
> = SourceColumns<DB, DB[Table]> extends infer Row extends object
  ? { [K in Extract<keyof Row, string>]: Row[K] }
  : never;

export type InsertRow<
  DB extends DatabaseSchema,
  Table extends InsertableSourceName<DB>,
> = SourceColumns<DB, DB[Table]> extends infer Row extends object
  ? { [K in Extract<keyof Row, string>]: InsertValue<Row[K]> }
  : never;

export type PredicateRow<
  DB extends DatabaseSchema,
  Table extends SelectableSourceName<DB>,
> = SourceColumns<DB, DB[Table]> extends infer Row extends object
  ? { [K in Extract<keyof Row, string>]: WhereValue<Row[K]> }
  : never;

export type QueryRow<T> = T extends object ? { [K in Extract<keyof T, string>]: T[K] } : never;
