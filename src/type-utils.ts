export type DatabaseSchema = object;
export type ScopeMap = Record<string, Record<string, unknown>>;

export type Simplify<T> = { [K in keyof T]: T[K] } & {};

export type UnionToIntersection<T> = (T extends unknown ? (value: T) => void : never) extends (
  value: infer R,
) => void
  ? R
  : never;

export type InferResult<T> = T extends { readonly __resultType: infer Output } ? Output : never;

export type TableName<DB extends DatabaseSchema> = Extract<keyof DB, string>;

export type TableRow<
  DB extends DatabaseSchema,
  Table extends TableName<DB>,
> = DB[Table] extends object ? { [K in Extract<keyof DB[Table], string>]: DB[Table][K] } : never;

export type QueryRow<T> = T extends object ? { [K in Extract<keyof T, string>]: T[K] } : never;
