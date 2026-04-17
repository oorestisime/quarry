import type { SelectQueryNode } from "../ast/query";
import type { QueryColumn, QueryColumnMap } from "../column-metadata";
import type { ClickHouseParam } from "../param";
import type {
  DatabaseSchema,
  QueryRow,
  ScopeRow,
  ScopeMap,
  SelectValue,
  WhereValue,
  Simplify,
  SelectableSourceName,
  TableName,
  UnionToIntersection,
} from "../type-utils";
import type { AliasedExpression, Expression, ExpressionBuilder } from "./expression-builder";
import type { AliasedQuery, TableSourceBuilder } from "./source-builder";

export type TableExpression<DB extends DatabaseSchema> =
  | SelectableSourceName<DB>
  | `${SelectableSourceName<DB>} as ${string}`;

export type SourceExpression<DB extends DatabaseSchema> =
  | TableExpression<DB>
  | TableSourceBuilder<DB, TableName<DB>, string>
  | AliasedQuery<object, string, any>;

type ParseTableExpression<T extends string> = T extends `${infer Table} as ${infer Alias}`
  ? { table: Table; alias: Alias }
  : { table: T; alias: T };

export type ScopeFromTableExpression<DB extends DatabaseSchema, TE extends TableExpression<DB>> =
  ParseTableExpression<TE> extends {
    table: infer Table extends SelectableSourceName<DB>;
    alias: infer Alias extends string;
  }
    ? { [K in Alias]: ScopeRow<DB, Table> }
    : never;

type ScopeFromAliasedQuery<Source> =
  Source extends AliasedQuery<infer Row, infer Alias, infer OutputColumns>
    ? { [K in Alias]: keyof OutputColumns extends never ? QueryRow<Row> : QueryRow<OutputColumns> }
    : never;

type ScopeFromTableSourceBuilder<DB extends DatabaseSchema, Source> =
  Source extends TableSourceBuilder<
    DB,
    infer Table extends TableName<DB>,
    infer Alias extends string
  >
    ? { [K in Alias]: ScopeRow<DB, Table> }
    : never;

export type ScopeFromSourceExpression<DB extends DatabaseSchema, Source> =
  Source extends TableExpression<DB>
    ? ScopeFromTableExpression<DB, Source>
    : Source extends TableSourceBuilder<DB, TableName<DB>, string>
      ? ScopeFromTableSourceBuilder<DB, Source>
      : ScopeFromAliasedQuery<Source>;

export type ScopeAlias<Scope extends ScopeMap> = Extract<keyof Scope, string>;

export type OnlyScopeAlias<Scope extends ScopeMap> =
  ScopeAlias<Scope> extends infer Alias extends string
    ? Exclude<ScopeAlias<Scope>, Alias> extends never
      ? Alias
      : never
    : never;

type QualifiedColumnRef<Scope extends ScopeMap> = {
  [K in ScopeAlias<Scope>]: `${K}.${Extract<keyof Scope[K], string>}`;
}[ScopeAlias<Scope>];

type UnqualifiedColumnRef<Scope extends ScopeMap> =
  OnlyScopeAlias<Scope> extends infer Alias extends ScopeAlias<Scope>
    ? Extract<keyof Scope[Alias], string>
    : never;

export type ColumnRef<Scope extends ScopeMap> =
  | QualifiedColumnRef<Scope>
  | UnqualifiedColumnRef<Scope>;

type ParseSelectionExpression<T extends string> = T extends `${infer Expr} as ${infer Alias}`
  ? { expr: Expr; alias: Alias }
  : { expr: T; alias: never };

type SelectionString<Scope extends ScopeMap> =
  | ColumnRef<Scope>
  | `${ColumnRef<Scope>} as ${string}`;

type ColumnNameFromRef<T extends string> = T extends `${string}.${infer Column}` ? Column : T;

type WrapOutputColumn<T> =
  T extends QueryColumn<any, any> ? T : QueryColumn<SelectValue<T>, WhereValue<T>>;

type ScopeRawValue<Row extends object, Key extends string> = Key extends keyof Row
  ? Row[Key]
  : never;

type ScopeSelectedValue<Row extends object, Key extends string> = Key extends keyof Row
  ? SelectValue<Row[Key]>
  : never;

type ScopePredicateValue<Row extends object, Key extends string> = Key extends keyof Row
  ? WhereValue<Row[Key]>
  : never;

export type ResolveColumnType<
  Scope extends ScopeMap,
  Ref extends string,
> = Ref extends `${infer Alias}.${infer Column}`
  ? Alias extends ScopeAlias<Scope>
    ? Column extends keyof Scope[Alias]
      ? ScopeSelectedValue<Scope[Alias], Extract<Column, string>>
      : never
    : never
  : OnlyScopeAlias<Scope> extends infer Alias extends ScopeAlias<Scope>
    ? Ref extends keyof Scope[Alias]
      ? ScopeSelectedValue<Scope[Alias], Extract<Ref, string>>
      : never
    : never;

export type ResolvePredicateColumnType<
  Scope extends ScopeMap,
  Ref extends string,
> = Ref extends `${infer Alias}.${infer Column}`
  ? Alias extends ScopeAlias<Scope>
    ? Column extends keyof Scope[Alias]
      ? ScopePredicateValue<Scope[Alias], Extract<Column, string>>
      : never
    : never
  : OnlyScopeAlias<Scope> extends infer Alias extends ScopeAlias<Scope>
    ? Ref extends keyof Scope[Alias]
      ? ScopePredicateValue<Scope[Alias], Extract<Ref, string>>
      : never
    : never;

export type ResolveScopeColumnType<
  Scope extends ScopeMap,
  Ref extends string,
> = Ref extends `${infer Alias}.${infer Column}`
  ? Alias extends ScopeAlias<Scope>
    ? Column extends keyof Scope[Alias]
      ? ScopeRawValue<Scope[Alias], Extract<Column, string>>
      : never
    : never
  : OnlyScopeAlias<Scope> extends infer Alias extends ScopeAlias<Scope>
    ? Ref extends keyof Scope[Alias]
      ? ScopeRawValue<Scope[Alias], Extract<Ref, string>>
      : never
    : never;

type NonTupleArray<T> = T extends readonly unknown[]
  ? number extends T["length"]
    ? T
    : never
  : never;

type StringLike<T> = Exclude<T, null> extends string ? T : never;

export type ArrayColumnRef<Scope extends ScopeMap> = {
  [Ref in ColumnRef<Scope>]: NonTupleArray<ResolveColumnType<Scope, Ref>> extends never
    ? never
    : Ref;
}[ColumnRef<Scope>];

export type StringColumnRef<Scope extends ScopeMap> = {
  [Ref in ColumnRef<Scope>]: StringLike<ResolveColumnType<Scope, Ref>> extends never ? never : Ref;
}[ColumnRef<Scope>];

export type EmptyableColumnRef<Scope extends ScopeMap> =
  | ArrayColumnRef<Scope>
  | StringColumnRef<Scope>;

export type ResolveArrayElementType<Scope extends ScopeMap, Ref extends ColumnRef<Scope>> =
  NonTupleArray<ResolveColumnType<Scope, Ref>> extends readonly (infer Item)[] ? Item : never;

type SelectionAlias<T extends string> = ParseSelectionExpression<T>["alias"];

type SelectionOutputKey<T extends string> = [SelectionAlias<T>] extends [never]
  ? ColumnNameFromRef<ParseSelectionExpression<T>["expr"] & string>
  : SelectionAlias<T>;

type SelectionOutputValue<Scope extends ScopeMap, T extends string> = ResolveColumnType<
  Scope,
  Extract<ParseSelectionExpression<T>["expr"], string>
>;

type OutputColumnRef<Output extends object> = Extract<keyof Output, string>;

export type OrderByRef<Scope extends ScopeMap, Output extends object> =
  | ColumnRef<Scope>
  | OutputColumnRef<Output>;

export type HavingRef<Scope extends ScopeMap, Output extends object> =
  | ColumnRef<Scope>
  | OutputColumnRef<Output>;

export type ResolveHavingType<
  Scope extends ScopeMap,
  Output extends object,
  Ref extends HavingRef<Scope, Output>,
> =
  Ref extends OutputColumnRef<Output>
    ? Output[Ref]
    : Ref extends string
      ? ResolveColumnType<Scope, Ref>
      : never;

export type SelectionExpression<Scope extends ScopeMap> =
  | SelectionString<Scope>
  | AliasedExpression<unknown, string, unknown>;

export type GroupByExpression<Scope extends ScopeMap> =
  | ColumnRef<Scope>
  | ((expressionBuilder: ExpressionBuilder<Scope>) => Expression<unknown>);

type SelectionResult<Scope extends ScopeMap, Selection> = Selection extends string
  ? { [K in SelectionOutputKey<Selection>]: SelectionOutputValue<Scope, Selection> }
  : Selection extends AliasedExpression<infer Value, infer Alias, any>
    ? { [K in Alias]: Value }
    : never;

type SelectionColumnResult<Scope extends ScopeMap, Selection> = Selection extends string
  ? {
      [K in SelectionOutputKey<Selection>]: WrapOutputColumn<
        ResolveScopeColumnType<Scope, Extract<ParseSelectionExpression<Selection>["expr"], string>>
      >;
    }
  : Selection extends AliasedExpression<infer Value, infer Alias, infer Where>
    ? { [K in Alias]: QueryColumn<Value, Where> }
    : never;

export type QueryLike = { toAST(): SelectQueryNode } | AliasedQuery<object, string, any>;

export type SelectionOutput<
  Scope extends ScopeMap,
  Selections extends readonly SelectionExpression<Scope>[],
> = Simplify<UnionToIntersection<SelectionResult<Scope, Selections[number]>>>;

export type SelectionOutputColumns<
  Scope extends ScopeMap,
  Selections extends readonly SelectionExpression<Scope>[],
> =
  Simplify<
    UnionToIntersection<SelectionColumnResult<Scope, Selections[number]>>
  > extends infer Columns
    ? Columns extends QueryColumnMap
      ? Columns
      : {}
    : {};

export type ScopeSelectionOutput<
  Scope extends ScopeMap,
  Alias extends ScopeAlias<Scope>,
> = Simplify<{
  [K in Extract<keyof Scope[Alias], string>]: ScopeSelectedValue<Scope[Alias], K>;
}>;

export type ScopeSelectionColumns<Scope extends ScopeMap, Alias extends ScopeAlias<Scope>> =
  Simplify<{
    [K in Extract<keyof Scope[Alias], string>]: WrapOutputColumn<Scope[Alias][K]>;
  }> extends infer Columns
    ? Columns extends QueryColumnMap
      ? Columns
      : {}
    : {};

export type AllScopeSelectionOutput<Scope extends ScopeMap> = Simplify<{
  [K in {
    [Alias in ScopeAlias<Scope>]: Extract<keyof Scope[Alias], string>;
  }[ScopeAlias<Scope>]]: {
    [Alias in ScopeAlias<Scope>]: ScopeSelectedValue<Scope[Alias], Extract<K, string>>;
  }[ScopeAlias<Scope>];
}>;

export type AllScopeSelectionColumns<Scope extends ScopeMap> =
  Simplify<{
    [K in {
      [Alias in ScopeAlias<Scope>]: Extract<keyof Scope[Alias], string>;
    }[ScopeAlias<Scope>]]: WrapOutputColumn<
      {
        [Alias in ScopeAlias<Scope>]: ScopeRawValue<Scope[Alias], Extract<K, string>>;
      }[ScopeAlias<Scope>]
    >;
  }> extends infer Columns
    ? Columns extends QueryColumnMap
      ? Columns
      : {}
    : {};

export type ParamLike<T> = T | ClickHouseParam<T>;

type NonNullish<T> = Exclude<T, null>;

export type PredicateOperator = "=" | "!=" | ">" | ">=" | "<" | "<=" | "in" | "not in";
export type RefPredicateOperator = "=" | "!=" | ">" | ">=" | "<" | "<=";

export type PredicateValue<Value, Operator extends PredicateOperator> = Operator extends
  | "in"
  | "not in"
  ? readonly NonNullish<Value>[] | ClickHouseParam<readonly Value[]>
  : NonNullish<Value> | ClickHouseParam<Value>;

export type ExpressionPredicateValue<Value, Operator extends PredicateOperator> =
  | PredicateValue<Value, Operator>
  | ClickHouseParam<unknown>;

export type HavingValue<Value, Operator extends PredicateOperator> = ExpressionPredicateValue<
  Value,
  Operator
>;
