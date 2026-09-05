import type { SelectQueryNode } from "../ast/query";
import type { QueryColumn, QueryColumnMap } from "../column-metadata";
import type { ColumnType, TypedDictionary, TypedView } from "../db-types";
import type { ClickHouseParam } from "../param";
import type {
  DatabaseSchema,
  QueryRow,
  ScopeRow,
  ScopeMap,
  SelectValue,
  WhereValue,
  Simplify,
  SourceName,
  UnionToIntersection,
} from "../type-utils";
import type { AliasedExpression, Expression, ExpressionBuilder } from "./expression-builder";
import type { AliasedQuery, TableSourceBuilder } from "./source-builder";

export type TableExpression<DB extends DatabaseSchema> = SourceName<DB> | `${string} as ${string}`;

export type SourceExpression<DB extends DatabaseSchema> =
  | TableExpression<DB>
  | TableSourceBuilder<DB, SourceName<DB>, string>
  | AliasedQuery<object, string, any>;

type ParseTableExpression<T extends string> = T extends `${infer Table} as ${infer Alias}`
  ? { table: Table; alias: Alias }
  : { table: T; alias: T };

export type ValidTableExpression<
  DB extends DatabaseSchema,
  TE extends TableExpression<DB>,
> = ParseTableExpression<TE>["table"] extends infer Table extends SourceName<DB>
  ? DB[Table] extends TypedDictionary<any>
    ? never
    : TE
  : never;

export type ValidSourceExpression<DB extends DatabaseSchema, Source extends SourceExpression<DB>> =
  Source extends TableExpression<DB>
    ? ValidTableExpression<DB, Source>
    : Source extends TableSourceBuilder<DB, infer Table extends SourceName<DB>, string>
      ? DB[Table] extends TypedView<any> | TypedDictionary<any>
        ? never
        : Source
      : Source extends AliasedQuery<object, string, any>
        ? Source
        : never;

export type ScopeFromTableExpression<DB extends DatabaseSchema, TE extends TableExpression<DB>> =
  ParseTableExpression<TE> extends {
    table: infer Table extends SourceName<DB>;
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
    infer Table extends SourceName<DB>,
    infer Alias extends string
  >
    ? { [K in Alias]: ScopeRow<DB, Table> }
    : never;

export type ScopeFromSourceExpression<DB extends DatabaseSchema, Source> =
  Source extends TableExpression<DB>
    ? ScopeFromTableExpression<DB, Source>
    : Source extends TableSourceBuilder<DB, SourceName<DB>, string>
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

type ColumnNameFromRef<
  Scope extends ScopeMap,
  T extends string,
> = T extends `${infer Alias}.${infer Column}` ? (Alias extends ScopeAlias<Scope> ? Column : T) : T;

type WrapOutputColumn<T> =
  T extends QueryColumn<any, any> ? T : QueryColumn<SelectValue<T>, WhereValue<T>>;

type ScopeRawValue<Row extends object, Key extends string> = Key extends keyof Row
  ? Row[Key]
  : never;

type ScopeSelectedValue<Row extends object, Key extends string> = SelectValue<
  ScopeRawValue<Row, Key>
>;

type UnqualifiedColumn<Scope extends ScopeMap, Ref extends string> =
  OnlyScopeAlias<Scope> extends infer Alias extends ScopeAlias<Scope>
    ? ScopeRawValue<Scope[Alias], Ref>
    : never;

export type ResolveScopeColumnType<
  Scope extends ScopeMap,
  Ref extends string,
> = Ref extends `${infer Alias}.${infer Column}`
  ? Alias extends ScopeAlias<Scope>
    ? ScopeRawValue<Scope[Alias], Column>
    : UnqualifiedColumn<Scope, Ref>
  : UnqualifiedColumn<Scope, Ref>;

export type ResolveColumnType<Scope extends ScopeMap, Ref extends string> = SelectValue<
  ResolveScopeColumnType<Scope, Ref>
>;

export type ResolvePredicateColumnType<Scope extends ScopeMap, Ref extends string> = WhereValue<
  ResolveScopeColumnType<Scope, Ref>
>;

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

type ArrayElement<Value> = NonTupleArray<Value> extends readonly (infer Item)[] ? Item : never;

type ArrayJoinedColumn<Column> =
  Column extends QueryColumn<infer Select, infer Where>
    ? QueryColumn<ArrayElement<Select>, ArrayElement<Where>>
    : Column extends ColumnType<infer Select, infer Insert, infer Where>
      ? ColumnType<ArrayElement<Select>, ArrayElement<Insert>, ArrayElement<Where>>
      : ArrayElement<Column>;

type IsArrayJoinTarget<
  Ref extends string,
  Alias extends string,
  Column extends string,
> = Ref extends `${infer RefAlias}.${infer RefColumn}`
  ? RefAlias extends Alias
    ? RefColumn extends Column
      ? true
      : false
    : false
  : Ref extends Column
    ? true
    : false;

export type ArrayJoinedScope<Scope extends ScopeMap, Ref extends string> = Simplify<{
  [Alias in keyof Scope]: {
    [Column in keyof Scope[Alias]]: IsArrayJoinTarget<
      Ref,
      Extract<Alias, string>,
      Extract<Column, string>
    > extends true
      ? ArrayJoinedColumn<Scope[Alias][Column]>
      : Scope[Alias][Column];
  };
}>;

type SelectionAlias<T extends string> = ParseSelectionExpression<T>["alias"];

type SelectionOutputKey<Scope extends ScopeMap, T extends string> = [SelectionAlias<T>] extends [
  never,
]
  ? ColumnNameFromRef<Scope, ParseSelectionExpression<T>["expr"] & string>
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

/** Preserve SQL selection order for positional operations such as INSERT SELECT. */
export type SelectionTypes<Scope extends ScopeMap, Selections extends readonly unknown[]> = {
  [Index in keyof Selections]: Selections[Index] extends string
    ? SelectionOutputValue<Scope, Selections[Index]>
    : Selections[Index] extends AliasedExpression<infer Value, string, any>
      ? Value
      : never;
};

export type NullableScope<Scope extends ScopeMap> = {
  [Alias in keyof Scope]: {
    [Column in keyof Scope[Alias]]: QueryColumn<
      SelectValue<Scope[Alias][Column]> | null,
      WhereValue<Scope[Alias][Column]> | null
    >;
  };
};

export type GroupByExpression<Scope extends ScopeMap, Dicts extends DatabaseSchema = never> =
  | ColumnRef<Scope>
  | ((expressionBuilder: ExpressionBuilder<Scope, Dicts>) => Expression<unknown>);

type SelectionResult<Scope extends ScopeMap, Selection> = Selection extends string
  ? { [K in SelectionOutputKey<Scope, Selection>]: SelectionOutputValue<Scope, Selection> }
  : Selection extends AliasedExpression<infer Value, infer Alias, any>
    ? { [K in Alias]: Value }
    : never;

type SelectionColumnResult<Scope extends ScopeMap, Selection> = Selection extends string
  ? {
      [K in SelectionOutputKey<Scope, Selection>]: WrapOutputColumn<
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
