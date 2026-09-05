import type { QueryColumn } from "./column-metadata";
import type { ColumnType, TypedDictionary, TypedTable, TypedView } from "./db-types";

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

export type SelectValue<T> =
  T extends ColumnType<infer Select, any, any>
    ? Select
    : T extends QueryColumn<infer Select, any>
      ? Select
      : T;
export type InsertValue<T> = T extends ColumnType<any, infer Insert, any> ? Insert : SelectValue<T>;
export type WhereValue<T> =
  T extends ColumnType<any, any, infer Where>
    ? Where
    : T extends QueryColumn<any, infer Where>
      ? Where
      : T;

type SourceColumns<Source> =
  Source extends TypedTable<infer Row>
    ? { [K in Extract<keyof Row, string>]: Row[K] }
    : Source extends TypedView<infer Row>
      ? { [K in Extract<keyof Row, string>]: Row[K] }
      : Source extends TypedDictionary<any>
        ? never
        : Source extends object
          ? { [K in Extract<keyof Source, string>]: Source[K] }
          : never;

export type DictionaryName<DB extends DatabaseSchema> = Extract<
  {
    [K in SourceName<DB>]: DB[K] extends TypedDictionary<any> ? K : never;
  }[SourceName<DB>],
  string
>;

type NonDictionarySourceName<DB extends DatabaseSchema> = Extract<
  {
    [K in SourceName<DB>]: DB[K] extends TypedDictionary<any> ? never : K;
  }[SourceName<DB>],
  string
>;

export type TableName<DB extends DatabaseSchema> = Extract<
  {
    [K in NonDictionarySourceName<DB>]: DB[K] extends TypedView<any> ? never : K;
  }[NonDictionarySourceName<DB>],
  string
>;

export type SelectableSourceName<DB extends DatabaseSchema> = NonDictionarySourceName<DB>;

export type InsertableSourceName<DB extends DatabaseSchema> = TableName<DB>;

/**
 * Validates one source name without classifying every key in the database schema.
 * Prefer these helpers on generic API call sites; the mapped classifier types
 * above remain exported for backwards compatibility.
 */
export type ValidSelectableSourceName<DB extends DatabaseSchema, Name extends SourceName<DB>> =
  DB[Name] extends TypedDictionary<any> ? never : Name;

export type ValidInsertableSourceName<
  DB extends DatabaseSchema,
  Name extends SourceName<DB>,
> = DB[Name] extends TypedView<any> | TypedDictionary<any> ? never : Name;

export type ValidDictionaryName<DB extends DatabaseSchema, Name extends SourceName<DB>> =
  DB[Name] extends TypedDictionary<any> ? Name : never;

export type Selectable<Source> =
  SourceColumns<Source> extends infer Row extends object
    ? { [K in Extract<keyof Row, string>]: SelectValue<Row[K]> }
    : never;

export type Insertable<Source> =
  Source extends TypedView<any>
    ? never
    : SourceColumns<Source> extends infer Row extends object
      ? Simplify<
          {
            [K in Extract<keyof Row, string> as InsertValue<Row[K]> extends never
              ? never
              : undefined extends InsertValue<Row[K]>
                ? never
                : K]: InsertValue<Row[K]>;
          } & {
            [K in Extract<keyof Row, string> as InsertValue<Row[K]> extends never
              ? never
              : undefined extends InsertValue<Row[K]>
                ? K
                : never]?: InsertValue<Row[K]>;
          }
        >
      : never;

export type TableRow<DB extends DatabaseSchema, Table extends SourceName<DB>> = Selectable<
  DB[Table]
>;

export type ScopeRow<DB extends DatabaseSchema, Table extends SourceName<DB>> =
  DB[Table] extends TypedDictionary<any> ? never : SourceColumns<DB[Table]>;

export type InsertRow<DB extends DatabaseSchema, Table extends SourceName<DB>> = Insertable<
  DB[Table]
>;

export type PredicateRow<DB extends DatabaseSchema, Table extends SourceName<DB>> =
  DB[Table] extends TypedDictionary<any>
    ? never
    : SourceColumns<DB[Table]> extends infer Row extends object
      ? { [K in Extract<keyof Row, string>]: WhereValue<Row[K]> }
      : never;

export type QueryRow<T> = T extends object ? { [K in Extract<keyof T, string>]: T[K] } : never;

export type DictionaryRow<DB extends DatabaseSchema, Name extends SourceName<DB>> =
  DB[Name] extends TypedDictionary<infer Row> ? Row : never;

export type DictionaryAttributeName<
  DB extends DatabaseSchema,
  Name extends SourceName<DB>,
> = Extract<keyof DictionaryRow<DB, Name>, string>;

export type DictionaryAttributeType<
  DB extends DatabaseSchema,
  Name extends SourceName<DB>,
  Attr extends DictionaryAttributeName<DB, Name>,
> = SelectValue<DictionaryRow<DB, Name>[Attr]>;
